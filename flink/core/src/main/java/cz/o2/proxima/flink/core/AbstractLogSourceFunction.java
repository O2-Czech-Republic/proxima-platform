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

import cz.o2.proxima.direct.LogObserver;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
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
abstract class AbstractLogSourceFunction<
        ReaderT,
        ObserverT extends AbstractSourceLogObserver<OffsetT, ContextT, OutputT>,
        OffsetT extends Serializable,
        ContextT extends LogObserver.OnNextContext<OffsetT>,
        OutputT>
    extends RichParallelSourceFunction<OutputT>
    implements CheckpointListener, CheckpointedFunction {

  private static final String OFFSETS_STATE_NAME = "offsets";

  /**
   * Get index of the subtask, that will be executing a given {@link Partition}.
   *
   * @param partition Partition to get subtask index for.
   * @param numParallelSubtasks Source parallelism.
   * @return Subtask index.
   */
  private static int getSubtaskIndex(Partition partition, int numParallelSubtasks) {
    return partition.getId() % numParallelSubtasks;
  }

  @Getter private final RepositoryFactory repositoryFactory;
  private final List<AttributeDescriptor<?>> attributeDescriptors;
  private final ResultExtractor<OutputT> resultExtractor;

  @Nullable private transient List<OffsetT> restoredOffsets;
  @Nullable private transient ListState<OffsetT> persistedOffsets;

  @Nullable private transient volatile ObserveHandle<OffsetT> observeHandle;

  /**
   * Latch that is removed once the source switches to a running state. This is only for marking the
   * transition to the running state and can not be used to determine, whether source is still
   * running (eg. after close).
   */
  private transient volatile CountDownLatch running;

  private transient volatile CountDownLatch cancelled;

  public AbstractLogSourceFunction(
      RepositoryFactory repositoryFactory,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ResultExtractor<OutputT> resultExtractor) {
    this.repositoryFactory = repositoryFactory;
    this.attributeDescriptors = attributeDescriptors;
    this.resultExtractor = resultExtractor;
  }

  @Override
  public void open(Configuration parameters) {
    running = new CountDownLatch(1);
    cancelled = new CountDownLatch(1);
  }

  abstract ReaderT createLogReader(List<AttributeDescriptor<?>> attributeDescriptors);

  abstract List<Partition> getPartitions(ReaderT reader);

  abstract Partition getOffsetPartition(OffsetT offset);

  abstract Set<Partition> getSkipFirstElement(List<OffsetT> offsets);

  abstract ObserverT createObserver(
      SourceContext<OutputT> sourceContext,
      ResultExtractor<OutputT> resultExtractor,
      Set<Partition> skipFirstElement);

  abstract ObserveHandle<OffsetT> observe(
      ReaderT reader,
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ObserverT observer);

  abstract ObserveHandle<OffsetT> observeOffsets(
      ReaderT reader,
      List<OffsetT> offsets,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ObserverT observer);

  public interface ObserveHandle<OffsetT> extends Closeable {

    List<OffsetT> getCurrentOffsets();

    @Override
    void close();
  }

  @Override
  public void run(SourceContext<OutputT> sourceContext) throws Exception {
    final ReaderT reader = createLogReader(attributeDescriptors);
    final List<Partition> partitions =
        getPartitions(reader)
            .stream()
            .filter(
                partition ->
                    getSubtaskIndex(partition, getRuntimeContext().getNumberOfParallelSubtasks())
                        == getRuntimeContext().getIndexOfThisSubtask())
            .collect(Collectors.toList());
    if (!partitions.isEmpty()) {
      final ObserverT observer;
      if (restoredOffsets != null) {
        final List<OffsetT> filteredOffsets =
            restoredOffsets
                .stream()
                .filter(
                    offset ->
                        getSubtaskIndex(
                                getOffsetPartition(offset),
                                getRuntimeContext().getNumberOfParallelSubtasks())
                            == getRuntimeContext().getIndexOfThisSubtask())
                .collect(Collectors.toList());
        final Set<Partition> skipFirstElement = getSkipFirstElement(filteredOffsets);
        observer = createObserver(sourceContext, resultExtractor, skipFirstElement);
        observeHandle = observeOffsets(reader, filteredOffsets, attributeDescriptors, observer);
      } else {
        observer = createObserver(sourceContext, resultExtractor, Collections.emptySet());
        observeHandle = observe(reader, partitions, attributeDescriptors, observer);
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
    while (cancelled.getCount() > 0) {
      try {
        cancelled.await();
      } catch (InterruptedException e) {
        if (cancelled.getCount() == 0) {
          // Re-interrupt if cancelled.
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public void cancel() {
    cancelled.countDown();
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
    Objects.requireNonNull(persistedOffsets).clear();
    if (observeHandle != null) {
      for (OffsetT offset : observeHandle.getCurrentOffsets()) {
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
          "BatchLog subtask {} restored state: {}.",
          getRuntimeContext().getIndexOfThisSubtask(),
          restoredOffsets);
    } else {
      log.info(
          "BatchLog subtask {} has no state to restore.",
          getRuntimeContext().getIndexOfThisSubtask());
    }
  }

  @VisibleForTesting
  void awaitRunning() throws InterruptedException {
    running.await();
  }
}
