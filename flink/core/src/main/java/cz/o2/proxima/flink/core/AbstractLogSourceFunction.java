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

/**
 * Abstract implementation of a source function, that is able to receive data via {@link
 * LogObserver}. This implementation has the full support for checkpointing and re-scaling.
 *
 * <p>Currently missing features:
 *
 * <ul>
 *   <li>Flexible specs for reader settings (eg. position, timestamp boundaries, ...).
 *   <li>Watermark synchronization using global aggregate.
 *   <li>Unified metrics for monitoring and alerting.
 * </ul>
 *
 * @param <ReaderT> Reader to use, for reading data.
 * @param <ObserverT> Observer implementation.
 * @param <OffsetT> Offset used by the current reader.
 * @param <ContextT> Context available to the observer.
 * @param <OutputT> Output element type.
 */
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
   * A handle, that unifies handles from different log readers. For example {@link
   * cz.o2.proxima.direct.batch.BatchLogReader} is not able to retrieve offsets by default.
   *
   * @param <OffsetT> Type of offset used by current log reader.
   */
  public interface UnifiedObserveHandle<OffsetT> extends Closeable {

    /**
     * Get list of already consumed offsets. This method must be thread-safe, because it's used
     * during asynchronous checkpoint.
     *
     * @return List of offsets.
     */
    List<OffsetT> getConsumedOffsets();

    @Override
    void close();
  }

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

  @Nullable private transient volatile UnifiedObserveHandle<OffsetT> observeHandle;

  /**
   * Latch that is removed once the source switches to a running state. This is only for marking the
   * transition to the running state and can not be used to determine, whether source is still
   * running (eg. after close).
   */
  private transient volatile CountDownLatch running;

  /**
   * Signals when the source gets cancelled, to break out of the infinite loop inside {@link
   * #finishAndMarkAsIdle(SourceContext)}.
   */
  private transient volatile CountDownLatch cancelled;

  AbstractLogSourceFunction(
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

  /**
   * Construct a new log reader for given attributes.
   *
   * @param attributeDescriptors Attributes we want to read.
   * @return New log reader instance.
   */
  abstract ReaderT createLogReader(List<AttributeDescriptor<?>> attributeDescriptors);

  /**
   * Get a "global" list of partitions that we want to read by the source. This list will be evenly
   * distributed between parallel subtask.
   *
   * @param reader Reader to get a list of partitions from.
   * @return List of partitions.
   */
  abstract List<Partition> getPartitions(ReaderT reader);

  /**
   * Get a partition the given offset belongs to. This method is mostly a workaround for not having
   * a base interface for offset implementations.
   *
   * @param offset Offset to get a partition from.
   * @return Partition.
   */
  abstract Partition getOffsetPartition(OffsetT offset);

  /**
   * When we're restoring from checkpoint, we need to skip the first element from partition in some
   * cases, so we don't re-read already processed element. This is due to offsets not implementing
   * {@link Comparable}, which may be impossible for some storages - eg. PubSub.
   *
   * @param offsets Offsets we're restoring.
   * @return Set of partitions, we want to skip the first read element from.
   */
  abstract Set<Partition> getSkipFirstElementFromPartitions(List<OffsetT> offsets);

  /**
   * Create a {@link AbstractSourceLogObserver log observer}, that "re-directs" received element
   * into a {@link SourceContext}.
   *
   * @param sourceContext Source context to use for writing output and watermarks.
   * @param resultExtractor Function for extracting results from {@link
   *     cz.o2.proxima.storage.StreamElement}.
   * @param skipFirstElement List of partitions, we want to skip the first element from. See {@link
   *     #getSkipFirstElementFromPartitions(List)} for more details.
   * @return Log observer.
   */
  abstract ObserverT createLogObserver(
      SourceContext<OutputT> sourceContext,
      ResultExtractor<OutputT> resultExtractor,
      Set<Partition> skipFirstElement);

  /**
   * Observer partitions using a given reader. This method is called when starting a source from the
   * clean state.
   *
   * @param reader Reader to use for observing partitions.
   * @param partitions Partitions belonging to a current subtask, that we want to observe.
   * @param attributeDescriptors List of attributes to observe.
   * @param observer Log observer.
   * @return Observe handle.
   */
  abstract UnifiedObserveHandle<OffsetT> observePartitions(
      ReaderT reader,
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ObserverT observer);

  /**
   * Observer offsets using a given reader. This method is called when restoring a source from the
   * checkpoint / savepoint.
   *
   * @param reader Reader to use for observing partitions.
   * @param offsets Restored offsets belonging to a current subtask, that we want to observe.
   * @param attributeDescriptors List of attributes to observe.
   * @param observer Log observer.
   * @return Observe handle.
   */
  abstract UnifiedObserveHandle<OffsetT> observeRestoredOffsets(
      ReaderT reader,
      List<OffsetT> offsets,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ObserverT observer);

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
        final Set<Partition> skipFirstElement = getSkipFirstElementFromPartitions(filteredOffsets);
        observer = createLogObserver(sourceContext, resultExtractor, skipFirstElement);
        observeHandle =
            observeRestoredOffsets(reader, filteredOffsets, attributeDescriptors, observer);
      } else {
        observer = createLogObserver(sourceContext, resultExtractor, Collections.emptySet());
        observeHandle = observePartitions(reader, partitions, attributeDescriptors, observer);
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
      for (OffsetT offset : observeHandle.getConsumedOffsets()) {
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
