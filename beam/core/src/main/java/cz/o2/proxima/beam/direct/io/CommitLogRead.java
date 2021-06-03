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
package cz.o2.proxima.beam.direct.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import cz.o2.proxima.beam.direct.io.BlockingQueueLogObserver.UnifiedContext;
import cz.o2.proxima.beam.direct.io.OffsetRestrictionTracker.OffsetRange;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.CommitLogReader.Factory;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A {@link PTransform} that reads from a {@link CommitLogReader} using splittable DoFn. */
public class CommitLogRead extends PTransform<PBegin, PCollection<StreamElement>> {

  /**
   * Create the {@link CommitLogRead} transform.
   *
   * @param observeName name of the observer
   * @param position {@link Position} to read from
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repo repository
   * @param reader the reader
   * @return {@link CommitLogRead} transform for the commit log
   */
  public static CommitLogRead of(
      String observeName, Position position, long limit, Repository repo, CommitLogReader reader) {

    return of(observeName, position, limit, repo.asFactory(), reader);
  }

  /**
   * Create the {@link CommitLogRead} transform.
   *
   * @param observeName name of the observer
   * @param position {@link Position} to read from
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repositoryFactory repository factory
   * @param reader the reader
   * @return {@link CommitLogRead} transform for the commit log
   */
  public static CommitLogRead of(
      String observeName,
      Position position,
      long limit,
      RepositoryFactory repositoryFactory,
      CommitLogReader reader) {

    return new CommitLogRead(observeName, position, limit, false, repositoryFactory, reader);
  }

  /**
   * Create the {@link CommitLogRead} transform.
   *
   * @param observeName name of the observer
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repositoryFactory repository factory
   * @param reader the reader
   * @return {@link CommitLogRead} transform for the commit log
   */
  public static CommitLogRead ofBounded(
      String observeName, long limit, RepositoryFactory repositoryFactory, CommitLogReader reader) {

    return new CommitLogRead(observeName, Position.OLDEST, limit, true, repositoryFactory, reader);
  }

  @BoundedPerElement
  private class BoundedCommitLogReadFn extends AbstractCommitLogReadFn {

    private BoundedCommitLogReadFn(
        @Nullable String name,
        Position position,
        long limit,
        RepositoryFactory repositoryFactory,
        CommitLogReader.Factory<?> readerFactory) {

      super(name, position, limit, repositoryFactory, readerFactory);
    }

    @ProcessElement
    public void processBounded(
        RestrictionTracker<OffsetRange, Offset> tracker,
        OutputReceiver<StreamElement> output,
        ManualWatermarkEstimator<?> watermarkEstimator,
        BundleFinalizer finalizer) {

      ProcessContinuation continuation;
      do {
        continuation = process(tracker, output, watermarkEstimator, finalizer);
      } while (continuation.shouldResume());
      Preconditions.checkState(
          !continuation.shouldResume(),
          "Should have terminated processing of the whole restriction, got %s",
          continuation);
    }

    @Setup
    @Override
    public void setup() {
      super.setup();
    }

    @Teardown
    @Override
    public void tearDown() {
      super.tearDown();
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction() {
      return OffsetRange.initialRestriction(limit, true);
    }

    @SplitRestriction
    @Override
    public void splitRestriction(
        @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> splits) {

      super.splitRestriction(restriction, splits);
    }

    @GetRestrictionCoder
    @Override
    public Coder<OffsetRange> getRestrictionCoder() {
      return super.getRestrictionCoder();
    }

    @NewWatermarkEstimator
    @Override
    public Manual newWatermarkEstimator(@WatermarkEstimatorState Instant initialWatemark) {
      return super.newWatermarkEstimator(initialWatemark);
    }

    @GetInitialWatermarkEstimatorState
    @Override
    public Instant getInitialWatermarkEstimatorState() {
      return super.getInitialWatermarkEstimatorState();
    }

    @GetWatermarkEstimatorStateCoder
    @Override
    public Coder<Instant> getWatermarkEstimatorStateCoder() {
      return super.getWatermarkEstimatorStateCoder();
    }

    ObserveHandle observeBulkOffsets(
        OffsetRange restriction,
        CommitLogReader reader,
        BlockingQueueLogObserver.CommitLog observer) {

      return reader.observeBulkOffsets(
          Collections.singletonList(restriction.getStartOffset()), true, observer);
    }

    ObserveHandle observeBulkPartitions(
        String name,
        OffsetRange restriction,
        CommitLogReader reader,
        BlockingQueueLogObserver.CommitLog observer) {

      return reader.observeBulkPartitions(
          name,
          Collections.singletonList(restriction.getPartition()),
          restriction.getPosition(),
          true,
          observer);
    }
  }

  @UnboundedPerElement
  private class UnboundedCommitLogReadFn extends AbstractCommitLogReadFn {

    private UnboundedCommitLogReadFn(
        @Nullable String name,
        Position position,
        long limit,
        RepositoryFactory repositoryFactory,
        CommitLogReader.Factory<?> readerFactory) {

      super(name, position, limit, repositoryFactory, readerFactory);
    }

    @ProcessElement
    public ProcessContinuation processUnbounded(
        RestrictionTracker<OffsetRange, Offset> tracker,
        OutputReceiver<StreamElement> output,
        ManualWatermarkEstimator<?> watermarkEstimator,
        BundleFinalizer finalizer) {

      return process(tracker, output, watermarkEstimator, finalizer);
    }

    @Setup
    @Override
    public void setup() {
      super.setup();
    }

    @Teardown
    @Override
    public void tearDown() {
      super.tearDown();
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction() {
      return OffsetRange.initialRestriction(limit, false);
    }

    @SplitRestriction
    @Override
    public void splitRestriction(
        @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> splits) {

      super.splitRestriction(restriction, splits);
    }

    @GetRestrictionCoder
    @Override
    public Coder<OffsetRange> getRestrictionCoder() {
      return super.getRestrictionCoder();
    }

    @NewWatermarkEstimator
    @Override
    public Manual newWatermarkEstimator(@WatermarkEstimatorState Instant initialWatemark) {
      return super.newWatermarkEstimator(initialWatemark);
    }

    @GetInitialWatermarkEstimatorState
    @Override
    public Instant getInitialWatermarkEstimatorState() {
      return super.getInitialWatermarkEstimatorState();
    }

    @GetWatermarkEstimatorStateCoder
    @Override
    public Coder<Instant> getWatermarkEstimatorStateCoder() {
      return super.getWatermarkEstimatorStateCoder();
    }

    ObserveHandle observeBulkOffsets(
        OffsetRange restriction,
        CommitLogReader reader,
        BlockingQueueLogObserver.CommitLog observer) {

      return reader.observeBulkOffsets(
          Collections.singletonList(restriction.getStartOffset()), observer);
    }

    ObserveHandle observeBulkPartitions(
        String name,
        OffsetRange restriction,
        CommitLogReader reader,
        BlockingQueueLogObserver.CommitLog observer) {

      return reader.observeBulkPartitions(
          name,
          Collections.singletonList(restriction.getPartition()),
          restriction.getPosition(),
          observer);
    }
  }

  private abstract class AbstractCommitLogReadFn extends DoFn<byte[], StreamElement> {

    @Nullable protected final String name;
    protected final Position position;
    protected final RepositoryFactory repositoryFactory;
    protected final Factory<?> readerFactory;
    protected final long limit;
    protected transient Map<Integer, ObserveHandle> runningObserves;
    protected transient Map<Integer, Offset> partitionToSeekedOffset;
    protected transient Map<Integer, BlockingQueueLogObserver.CommitLog> observers;
    private transient boolean externalizableOffsets = false;

    public AbstractCommitLogReadFn(
        @Nullable String name,
        Position position,
        long limit,
        RepositoryFactory repositoryFactory,
        CommitLogReader.Factory<?> readerFactory) {

      this.name = name;
      this.position = position;
      this.limit = limit;
      this.repositoryFactory = repositoryFactory;
      this.readerFactory = readerFactory;
    }

    public ProcessContinuation process(
        RestrictionTracker<OffsetRange, Offset> tracker,
        OutputReceiver<StreamElement> output,
        ManualWatermarkEstimator<?> watermarkEstimator,
        BundleFinalizer finalizer) {

      AtomicReference<UnifiedContext> ackContext = new AtomicReference<>();
      BundleFinalizer.Callback bundleFinalize =
          () -> Optional.ofNullable(ackContext.getAndSet(null)).ifPresent(UnifiedContext::confirm);

      finalizer.afterBundleCommit(BoundedWindow.TIMESTAMP_MAX_VALUE, bundleFinalize);

      Partition part = tracker.currentRestriction().getPartition();

      final BlockingQueueLogObserver.CommitLog currentObserver = observers.get(part.getId());

      if (currentObserver != null && externalizableOffsets) {
        closeHandleIfUnmatchingOffsets(tracker, part, currentObserver);
      }

      if (runningObserves.get(part.getId()) == null) {
        // start current restriction
        startObserve(this.name, part, tracker.currentRestriction());
        // start the consumption after the other restrictions are started
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(100));
      }

      boolean canIgnoreFirstElement =
          externalizableOffsets
              && !tracker.currentRestriction().isStartInclusive()
              && Objects.equals(
                  partitionToSeekedOffset.get(part.getId()),
                  tracker.currentRestriction().getStartOffset());

      final BlockingQueueLogObserver.CommitLog observer =
          Objects.requireNonNull(observers.get(part.getId()));

      watermarkEstimator.setWatermark(Instant.ofEpochMilli(observer.getWatermark()));

      while (!Thread.currentThread().isInterrupted()
          && observer.getWatermark() < Watermarks.MAX_WATERMARK
          && observer.peekElement()) {

        UnifiedContext currentPeekContext = Objects.requireNonNull(observer.getPeekContext());
        Offset offset = Objects.requireNonNull(currentPeekContext.getOffset());
        if (canIgnoreFirstElement) {
          canIgnoreFirstElement = false;
          // discard the peeked element
          observer.take();
          // skip the exclusive first offset
          continue;
        }
        if (!tracker.tryClaim(offset)) {
          return ProcessContinuation.stop();
        }
        StreamElement element = Objects.requireNonNull(observer.take());
        output.outputWithTimestamp(element, Instant.ofEpochMilli(element.getStamp()));
        ackContext.set(currentPeekContext);
        watermarkEstimator.setWatermark(Instant.ofEpochMilli(observer.getWatermark()));
      }

      Optional.ofNullable(observer.getError())
          .ifPresent(ExceptionUtils::rethrowAsIllegalStateException);

      boolean terminated =
          tracker.currentRestriction().isLimitConsumed()
              || observer.getWatermark() >= Watermarks.MAX_WATERMARK;
      return terminated
          ? ProcessContinuation.stop()
          : ProcessContinuation.resume().withResumeDelay(Duration.millis(100));
    }

    private void closeHandleIfUnmatchingOffsets(
        RestrictionTracker<OffsetRange, Offset> tracker,
        Partition part,
        BlockingQueueLogObserver.CommitLog observer) {

      final Offset currentOffset;
      if (observer.getLastReadContext() != null) {
        currentOffset = observer.getLastReadContext().getOffset();
      } else {
        currentOffset = partitionToSeekedOffset.get(part.getId());
      }
      if (!Objects.equals(currentOffset, tracker.currentRestriction().getStartOffset())) {
        // there was existing handle with read context, which means we have already read some data
        // and any commit (or nack) must wait till checkpoint
        closeHandle(part.getId(), false);
      }
    }

    protected void closeHandle(int part, boolean nack) {
      Optional.ofNullable(observers.remove(part)).ifPresent(observer -> observer.stop(nack));
      Optional.ofNullable(runningObserves.remove(part)).ifPresent(ObserveHandle::close);
      partitionToSeekedOffset.remove(part);
    }

    private void startObserve(@Nullable String name, Partition partition, OffsetRange restriction) {
      CommitLogReader reader = readerFactory.apply(repositoryFactory.apply());
      this.externalizableOffsets = reader.hasExternalizableOffsets();
      final BlockingQueueLogObserver.CommitLog observer = newObserver(name, restriction);
      observers.put(partition.getId(), observer);
      final ObserveHandle handle;
      if (restriction.getStartOffset() != null) {
        handle = observeBulkOffsets(restriction, reader, observer);
        partitionToSeekedOffset.put(partition.getId(), restriction.getStartOffset());
      } else {
        handle = observeBulkPartitions(name, restriction, reader, observer);
      }
      runningObserves.put(partition.getId(), handle);
    }

    abstract ObserveHandle observeBulkOffsets(
        OffsetRange restriction,
        CommitLogReader reader,
        BlockingQueueLogObserver.CommitLog observer);

    abstract ObserveHandle observeBulkPartitions(
        @Nullable String name,
        OffsetRange restriction,
        CommitLogReader reader,
        BlockingQueueLogObserver.CommitLog observer);

    public void setup() {
      runningObserves = new HashMap<>();
      partitionToSeekedOffset = new HashMap<>();
      observers = new HashMap<>();
    }

    public void tearDown() {
      Lists.newArrayList(observers.keySet()).forEach(p -> closeHandle(p, true));
    }

    void splitRestriction(OffsetRange restriction, OutputReceiver<OffsetRange> splits) {
      if (restriction.isInitial()) {
        CommitLogReader reader = readerFactory.apply(repositoryFactory.apply());
        // compute starting offsets from commit log reader
        List<Partition> partitions = reader.getPartitions();
        partitions.forEach(p -> splits.output(OffsetRange.startingFrom(p, position, restriction)));
      } else {
        splits.output(restriction);
      }
    }

    public Coder<OffsetRange> getRestrictionCoder() {
      return SerializableCoder.of(OffsetRange.class);
    }

    public Manual newWatermarkEstimator(Instant initialWatemark) {
      return SDFUtils.rangeCheckedManualEstimator(initialWatemark);
    }

    public Instant getInitialWatermarkEstimatorState() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    public Coder<Instant> getWatermarkEstimatorStateCoder() {
      return InstantCoder.of();
    }
  }

  private final String observeName;
  private final Position position;
  private final long limit;
  private final boolean bounded;
  private final RepositoryFactory repoFactory;
  private final Factory<?> readerFactory;

  @VisibleForTesting
  CommitLogRead(
      String observeName,
      Position position,
      long limit,
      boolean bounded,
      RepositoryFactory repoFactory,
      CommitLogReader reader) {

    this.observeName = observeName;
    this.position = position;
    this.limit = limit;
    this.bounded = bounded;
    this.repoFactory = repoFactory;
    this.readerFactory = reader.asFactory();
  }

  @Override
  public PCollection<StreamElement> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(
            ParDo.of(
                bounded
                    ? new BoundedCommitLogReadFn(
                        observeName, position, limit, repoFactory, readerFactory)
                    : new UnboundedCommitLogReadFn(
                        observeName, position, limit, repoFactory, readerFactory)));
  }

  @VisibleForTesting
  BlockingQueueLogObserver.CommitLog newObserver(@Nullable String name, OffsetRange restriction) {
    return BlockingQueueLogObserver.createCommitLog(
        name != null ? name : UUID.randomUUID().toString(),
        restriction.getTotalLimit(),
        Watermarks.MIN_WATERMARK);
  }
}
