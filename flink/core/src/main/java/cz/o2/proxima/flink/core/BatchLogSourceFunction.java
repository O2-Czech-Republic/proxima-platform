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

import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.direct.batch.Offset;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.flink.core.batch.OffsetTrackingBatchLogReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

@Slf4j
public class BatchLogSourceFunction<T> extends RichParallelSourceFunction<T>
    implements CheckpointListener, CheckpointedFunction {

  private static final String OFFSETS_STATE_NAME = "offsets";

  private static int assignPartition(Partition partition, int numParallelSubtasks) {
    return partition.getId() % numParallelSubtasks;
  }

  @FunctionalInterface
  public interface ResultExtractor<T> extends Serializable {

    T toResult(StreamElement element);
  }

  @SuppressWarnings("java:S1948")
  @VisibleForTesting
  static class SourceLogObserver<T> implements BatchLogObserver {

    private final transient CountDownLatch completed = new CountDownLatch(1);
    private final Set<Partition> seenPartitions = new HashSet<>();

    private final SourceContext<T> sourceContext;
    private final ResultExtractor<T> resultExtractor;

    /**
     * When restoring from checkpoint, we need to skip the first element in each partition, because
     * it has been already consumed.
     */
    private final Set<Partition> skipFirstElementFromEachPartition;

    private long watermark = Watermarks.MIN_WATERMARK;

    @Nullable private volatile Throwable error = null;

    private SourceLogObserver(
        SourceContext<T> sourceContext,
        ResultExtractor<T> resultExtractor,
        Set<Partition> skipFirstElementFromEachPartition) {
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
    public boolean onNext(StreamElement ingest, OnNextContext context) {
      final boolean skipElement =
          skipFirstElementFromEachPartition.contains(context.getPartition())
              && seenPartitions.add(context.getPartition());
      if (!skipElement) {
        synchronized (sourceContext.getCheckpointLock()) {
          final OffsetTrackingBatchLogReader.OffsetCommitter committer =
              (OffsetTrackingBatchLogReader.OffsetCommitter) context;
          sourceContext.collect(resultExtractor.toResult(ingest));
          committer.markOffsetAsConsumed();
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
  @Nullable private transient ListState<Offset> persistedOffsets;

  @Nullable private transient volatile ObserveHandle observeHandle;

  /**
   * Latch that is removed once the source switches to a running state. This is only for marking the
   * transition to the running state and can not be used to determine, whether source is still
   * running (eg. after close).
   */
  private transient volatile CountDownLatch running = new CountDownLatch(1);

  public BatchLogSourceFunction(
      RepositoryFactory repositoryFactory,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ResultExtractor<T> resultExtractor) {
    this.repositoryFactory = repositoryFactory;
    this.attributeDescriptors = attributeDescriptors;
    this.resultExtractor = resultExtractor;
  }

  @VisibleForTesting
  BatchLogReader getBatchLogReader(List<AttributeDescriptor<?>> attributeDescriptors) {
    final BatchLogReader batchLogReader =
        repositoryFactory
            .apply()
            .getOrCreateOperator(DirectDataOperator.class)
            .getBatchLogReader(attributeDescriptors)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to find batch log reader for [%s].", attributeDescriptors)));
    return OffsetTrackingBatchLogReader.of(batchLogReader);
  }

  /**
   * Allow tests to wrap the source observer, in order to place a barrier for deterministically
   * acquiring the checkpoint lock.
   *
   * @param sourceObserver Source observer to wrap.
   * @return Wrapped observer.
   */
  @VisibleForTesting
  BatchLogObserver wrapSourceObserver(BatchLogObserver sourceObserver) {
    return sourceObserver;
  }

  @Override
  public void open(Configuration parameters) {
    running = new CountDownLatch(1);
  }

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    final BatchLogReader reader = getBatchLogReader(attributeDescriptors);
    final List<Partition> partitions =
        reader
            .getPartitions()
            .stream()
            .filter(
                partition ->
                    assignPartition(partition, getRuntimeContext().getNumberOfParallelSubtasks())
                        == getRuntimeContext().getIndexOfThisSubtask())
            .collect(Collectors.toList());
    if (!partitions.isEmpty()) {
      final SourceLogObserver<T> observer;
      if (restoredOffsets != null) {
        final List<Offset> filteredOffsets =
            restoredOffsets
                .stream()
                .filter(
                    offset ->
                        assignPartition(
                                offset.getPartition(),
                                getRuntimeContext().getNumberOfParallelSubtasks())
                            == getRuntimeContext().getIndexOfThisSubtask())
                .collect(Collectors.toList());
        final Set<Partition> skipFirstElement =
            filteredOffsets
                .stream()
                .filter(offset -> offset.getElementIndex() >= 0)
                .map(Offset::getPartition)
                .collect(Collectors.toSet());
        observer = new SourceLogObserver<>(sourceContext, resultExtractor, skipFirstElement);
        observeHandle =
            reader.observeOffsets(
                filteredOffsets, attributeDescriptors, wrapSourceObserver(observer));
      } else {
        observer = new SourceLogObserver<>(sourceContext, resultExtractor, Collections.emptySet());
        observeHandle =
            reader.observe(partitions, attributeDescriptors, wrapSourceObserver(observer));
      }
      log.info("Source [{}]: RUNNING", this);
      running.countDown();
      observer.awaitCompleted();
      final Optional<Throwable> maybeError = observer.getError();
      if (maybeError.isPresent()) {
        log.error("Source [{}]: FAILED", this, maybeError.get());
        ExceptionUtils.rethrowAsIllegalStateException(maybeError.get());
      } else {
        finishAndMarkAsIdle(sourceContext);
      }
    } else {
      running.countDown();
      finishAndMarkAsIdle(sourceContext);
    }
  }

  /**
   * We're done with this source. Progress watermark to {@link Watermark#MAX_WATERMARK}, mark source
   * as idle and sleep.
   *
   * @param sourceContext Source context.
   * @see <a href="https://issues.apache.org/jira/browse/FLINK-2491">FLINK-2491</a>
   */
  @VisibleForTesting
  void finishAndMarkAsIdle(SourceContext<?> sourceContext) {
    log.info("Source [{}]: COMPLETED", this);
    synchronized (sourceContext.getCheckpointLock()) {
      sourceContext.emitWatermark(new Watermark(Watermarks.MAX_WATERMARK));
    }
    sourceContext.markAsTemporarilyIdle();
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

  protected List<Offset> getCurrentOffsets(ObserveHandle handle) {
    final OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle cast =
        (OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle) handle;
    // Filter out finished partitions, as we don't need them for restoring the state.
    return cast.getCurrentOffsets()
        .stream()
        .filter(offset -> !offset.isLast())
        .collect(Collectors.toList());
  }

  public void notifyCheckpointComplete(long l) {
    // No-op.
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    Objects.requireNonNull(persistedOffsets).clear();
    if (observeHandle != null) {
      for (Offset offset : getCurrentOffsets(Objects.requireNonNull(observeHandle))) {
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
      Objects.requireNonNull(persistedOffsets).get().forEach(restoredOffsets::add);
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
