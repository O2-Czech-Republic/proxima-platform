/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.flink.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

@Slf4j
public class CommitLogSourceFunction<T> extends RichParallelSourceFunction<T>
    implements CheckpointListener, CheckpointedFunction {

  private static final String OFFSETS_STATE_NAME = "offsets";

  private static int assignPartition(Partition partition, int numParallelSubtasks) {
    return partition.getId() % numParallelSubtasks;
  }

  public static CommitLogSourceFunction<StreamElement> of(
      RepositoryFactory repositoryFactory, List<AttributeDescriptor<?>> attributeDescriptors) {
    return new CommitLogSourceFunction<>(
        repositoryFactory, attributeDescriptors, new IdentityResultExtractor());
  }

  @FunctionalInterface
  public interface ResultExtractor<T> extends Serializable {

    T toResult(StreamElement element);
  }

  public static class IdentityResultExtractor implements ResultExtractor<StreamElement> {

    @Override
    public StreamElement toResult(StreamElement element) {
      return element;
    }
  }

  @SuppressWarnings("java:S1948")
  private static class SourceLogObserver<T> implements LogObserver {

    private final transient CountDownLatch completed = new CountDownLatch(1);
    private final Set<Partition> seenPartitions = new HashSet<>();

    private final SourceContext<T> sourceContext;
    private final ResultExtractor<T> resultExtractor;

    /**
     * When restoring from checkpoint, we need to skip the first element in each partition, due to
     * {@link ObserveHandle#getCurrentOffsets()} contract.
     */
    private final boolean skipFirstElementFromEachPartition;

    private long watermark = Watermarks.MIN_WATERMARK;

    @Nullable private volatile Throwable error = null;

    private SourceLogObserver(
        SourceContext<T> sourceContext,
        ResultExtractor<T> resultExtractor,
        boolean skipFirstElementFromEachPartition) {
      this.sourceContext = sourceContext;
      this.resultExtractor = resultExtractor;
      this.skipFirstElementFromEachPartition = skipFirstElementFromEachPartition;
    }

    @Override
    public boolean onError(Throwable error) {
      this.error = error;
      completed.countDown();
      return false;
    }

    @Override
    public void onCompleted() {
      completed.countDown();
    }

    @Override
    public void onCancelled() {
      completed.countDown();
    }

    @Override
    public void onIdle(OnIdleContext context) {
      if (context.getWatermark() > watermark) {
        watermark = context.getWatermark();
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.emitWatermark(new Watermark(watermark));
        }
      }
    }

    @Override
    public void onRepartition(OnRepartitionContext context) {
      // No-op.
    }

    @Override
    public boolean onNext(StreamElement ingest, OnNextContext context) {
      final boolean skipElement =
          skipFirstElementFromEachPartition && seenPartitions.add(context.getPartition());
      if (!skipElement) {
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.collect(resultExtractor.toResult(ingest));
        }
        if (context.getWatermark() > watermark) {
          watermark = context.getWatermark();
          synchronized (sourceContext.getCheckpointLock()) {
            sourceContext.emitWatermark(new Watermark(watermark));
          }
        }
      }
      return true;
    }

    public void awaitCompleted() throws InterruptedException {
      completed.await();
    }

    public Optional<Throwable> getError() {
      return Optional.ofNullable(error);
    }
  }

  private final RepositoryFactory repositoryFactory;
  private final List<AttributeDescriptor<?>> attributeDescriptors;
  private final ResultExtractor<T> resultExtractor;

  @Nullable private transient List<Offset> restoredOffsets;
  private transient ListState<Offset> persistedOffsets;

  @Nullable private transient volatile ObserveHandle observeHandle;

  /**
   * Latch that is removed once the source switches to a running state. This is only for marking the
   * transition to the running state and can not be used to determine, whether source is still
   * running (eg. after close).
   */
  private transient volatile CountDownLatch running = new CountDownLatch(1);

  public CommitLogSourceFunction(
      RepositoryFactory repositoryFactory,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ResultExtractor<T> resultExtractor) {
    this.repositoryFactory = repositoryFactory;
    this.attributeDescriptors = attributeDescriptors;
    this.resultExtractor = resultExtractor;
  }

  @VisibleForTesting
  CommitLogReader getCommitLogReader(List<AttributeDescriptor<?>> attributeDescriptors) {
    return repositoryFactory
        .apply()
        .getOrCreateOperator(DirectDataOperator.class)
        .getCommitLogReader(attributeDescriptors)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Unable to find commit log reader for [%s].", attributeDescriptors)));
  }

  @Override
  public void open(Configuration parameters) {
    running = new CountDownLatch(1);
  }

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    final CommitLogReader reader = getCommitLogReader(attributeDescriptors);
    Preconditions.checkArgument(
        reader.hasExternalizableOffsets(), "Reader [%s] doesn't support external offsets.", reader);
    final Set<Partition> partitions =
        reader
            .getPartitions()
            .stream()
            .filter(
                partition ->
                    assignPartition(partition, getRuntimeContext().getNumberOfParallelSubtasks())
                        == getRuntimeContext().getIndexOfThisSubtask())
            .collect(Collectors.toSet());
    if (!partitions.isEmpty()) {
      final SourceLogObserver<T> observer;
      if (restoredOffsets != null) {
        observer = new SourceLogObserver<>(sourceContext, resultExtractor, true);
        observeHandle =
            reader.observeBulkOffsets(
                restoredOffsets
                    .stream()
                    .filter(offset -> partitions.contains(offset.getPartition()))
                    .collect(Collectors.toList()),
                observer);
      } else {
        observer = new SourceLogObserver<>(sourceContext, resultExtractor, false);
        observeHandle = reader.observeBulkPartitions(partitions, Position.OLDEST, observer);
      }
      Objects.requireNonNull(observeHandle).waitUntilReady();
      running.countDown();
      log.info("Source [{}]: RUNNING", this);
      observer.awaitCompleted();
      final Optional<Throwable> maybeError = observer.getError();
      if (maybeError.isPresent()) {
        log.error("Source [{}]: FAILED", this, maybeError.get());
        ExceptionUtils.rethrowAsIllegalStateException(maybeError.get());
      } else {
        log.info("Source [{}]: COMPLETED", this);
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.emitWatermark(new Watermark(Watermarks.MAX_WATERMARK));
        }
      }
    } else {
      running.countDown();
      log.info("Source [{}]: RUNNING", this);
      log.info("Source [{}]: COMPLETED", this);
      synchronized (sourceContext.getCheckpointLock()) {
        sourceContext.emitWatermark(new Watermark(Watermarks.MAX_WATERMARK));
      }
    }
  }

  @Override
  public void cancel() {
    if (observeHandle != null) {
      Objects.requireNonNull(observeHandle).close();
    }
  }

  @Override
  public void close() {
    if (observeHandle != null) {
      Objects.requireNonNull(observeHandle).close();
    }
  }

  public void notifyCheckpointComplete(long l) {
    // No-op.
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    persistedOffsets.clear();
    if (observeHandle != null) {
      for (Offset offset : Objects.requireNonNull(observeHandle).getCurrentOffsets()) {
        persistedOffsets.add(offset);
      }
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    final OperatorStateStore stateStore = context.getOperatorStateStore();
    persistedOffsets =
        stateStore.getUnionListState(
            new ListStateDescriptor<>(OFFSETS_STATE_NAME, new JavaSerializer<>()));
    if (context.isRestored()) {
      restoredOffsets = new ArrayList<>();
      persistedOffsets.get().forEach(restoredOffsets::add);
      log.info(
          "CommitLog subtask {} restored state: {}.",
          getRuntimeContext().getIndexOfThisSubtask(),
          restoredOffsets);
    } else {
      log.info(
          "CommitLog subtask {} has no state to restore.",
          getRuntimeContext().getIndexOfThisSubtask());
    }
  }

  @VisibleForTesting
  void awaitRunning() throws InterruptedException {
    running.await();
  }
}
