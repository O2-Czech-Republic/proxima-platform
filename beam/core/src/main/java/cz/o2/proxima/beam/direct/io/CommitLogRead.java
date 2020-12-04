/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import cz.o2.proxima.beam.direct.io.BlockingQueueLogObserver.UnifiedContext;
import cz.o2.proxima.beam.direct.io.OffsetRestrictionTracker.OffsetRange;
import cz.o2.proxima.beam.direct.io.OffsetRestrictionTracker.OffsetWatermarkEstimator;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.CommitLogReader.Factory;
import cz.o2.proxima.direct.commitlog.LogObserver;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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

    return new CommitLogRead(observeName, position, limit, repositoryFactory, reader);
  }

  @DoFn.UnboundedPerElement
  private static class CommitLogReadFn extends DoFn<byte[], StreamElement> {

    private final String name;
    private final Position position;
    private final RepositoryFactory repositoryFactory;
    private final CommitLogReader.Factory<?> readerFactory;
    private final long limit;

    private transient boolean externalizableOffsets = false;
    private transient BlockingQueueLogObserver observer;

    private CommitLogReadFn(
        @Nullable String name,
        Position position,
        long limit,
        RepositoryFactory repositoryFactory,
        CommitLogReader.Factory<?> readerFactory) {

      this.name = createObserverNameFrom(name);
      this.position = position;
      this.repositoryFactory = repositoryFactory;
      this.readerFactory = readerFactory;
      this.limit = limit;
    }

    private String createObserverNameFrom(@Nullable String observerName) {
      return observerName == null ? UUID.randomUUID().toString() : observerName;
    }

    @ProcessElement
    public ProcessContinuation process(
        RestrictionTracker<OffsetRange, Offset> tracker,
        OutputReceiver<StreamElement> output,
        BundleFinalizer finalizer) {

      AtomicReference<UnifiedContext> readContext = new AtomicReference<>();
      AtomicReference<UnifiedContext> lastWrittenContext = new AtomicReference<>();
      BundleFinalizer.Callback bundleFinalize =
          () -> {
            Optional.ofNullable(readContext.get()).ifPresent(UnifiedContext::confirm);
            Optional.ofNullable(lastWrittenContext.get()).ifPresent(UnifiedContext::nack);
          };
      if (!externalizableOffsets) {
        // we confirm only processing of CommitLogReaders with non-externalizable offsets
        // externalizable offsets are persisted and reloaded during recovery
        finalizer.afterBundleCommit(BoundedWindow.TIMESTAMP_MAX_VALUE, bundleFinalize);
      }

      try (ObserveHandle handle = startObserve(tracker.currentRestriction())) {
        int i = 0;
        StreamElement element;
        while (!Thread.currentThread().isInterrupted()
            && observer.getWatermark() < Watermarks.MAX_WATERMARK
            && (element = observer.take()) != null) {

          boolean isFirst = i++ == 0;
          UnifiedContext currentReadContext = Objects.requireNonNull(observer.getLastReadContext());
          Offset offset = Objects.requireNonNull(currentReadContext.getOffset());
          if (externalizableOffsets
              && !tracker.currentRestriction().isStartInclusive()
              && isFirst) {
            // skip the exclusive first offset
            continue;
          }
          if (!tracker.tryClaim(offset)) {
            return ProcessContinuation.stop();
          }
          output.outputWithTimestamp(element, Instant.ofEpochMilli(element.getStamp()));
          readContext.set(currentReadContext);
        }
      } finally {
        // nack all buffered but unprocessed data iff we have not emitted any output
        // otherwise, we have to wait till bundle finalization
        boolean shouldNackAllUnread = readContext.get() == null;
        observer.stop(shouldNackAllUnread);
        lastWrittenContext.set(observer.getLastWrittenContext());
        if (externalizableOffsets) {
          ExceptionUtils.unchecked(bundleFinalize::onBundleSuccess);
        }
      }
      Optional.ofNullable(observer.getError())
          .ifPresent(ExceptionUtils::rethrowAsIllegalStateException);
      boolean terminated =
          tracker.currentRestriction().isLimitConsumed()
              || observer.getWatermark() >= Watermarks.MAX_WATERMARK;
      return terminated ? ProcessContinuation.stop() : ProcessContinuation.resume();
    }

    private ObserveHandle startObserve(OffsetRange restriction) {
      CommitLogReader reader = readerFactory.apply(repositoryFactory.apply());
      this.externalizableOffsets = reader.hasExternalizableOffsets();
      observer =
          BlockingQueueLogObserver.create(
              name, restriction.getTotalLimit(), Watermarks.MIN_WATERMARK);
      return reader.observeBulkOffsets(
          Collections.singletonList(restriction.getStartOffset()), observer);
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction() {
      return OffsetRange.initialRestriction(limit);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> splits) {

      if (restriction.isInitial()) {
        CommitLogReader reader = readerFactory.apply(repositoryFactory.apply());
        // compute starting offsets from commit log reader
        List<Partition> partitions = reader.getPartitions();
        // create a no-op observer, just start observing to fetch offsets
        try (ObserveHandle handle =
            reader.observeBulkPartitions(partitions, position, noopObserver())) {
          ExceptionUtils.ignoringInterrupted(handle::waitUntilReady);
          handle
              .getCurrentOffsets()
              .forEach(
                  o -> splits.output(OffsetRange.startingFrom(o, restriction.getTotalLimit())));
        }
      } else {
        splits.output(restriction);
      }
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> getRestrictionCoder() {
      return SerializableCoder.of(OffsetRange.class);
    }

    @NewWatermarkEstimator
    public OffsetWatermarkEstimator newWatermarkEstimator(@Restriction OffsetRange restriction) {
      return new OffsetWatermarkEstimator(restriction);
    }

    @GetWatermarkEstimatorStateCoder
    public Coder<Void> getWatermarkEstimatorStateCoder() {
      return VoidCoder.of();
    }

    private LogObserver noopObserver() {
      return new LogObserver() {
        @Override
        public boolean onError(Throwable error) {
          return false;
        }

        @Override
        public boolean onNext(StreamElement ingest, OnNextContext context) {
          context.nack();
          return false;
        }
      };
    }
  }

  private final String observeName;
  private final Position position;
  private final long limit;
  private final RepositoryFactory repoFactory;
  private final Factory<?> readerFactory;

  private CommitLogRead(
      String observeName,
      Position position,
      long limit,
      RepositoryFactory repoFactory,
      CommitLogReader reader) {

    this.observeName = observeName;
    this.position = position;
    this.limit = limit;
    this.repoFactory = repoFactory;
    this.readerFactory = reader.asFactory();
  }

  @Override
  public PCollection<StreamElement> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(
            ParDo.of(
                new CommitLogReadFn(observeName, position, limit, repoFactory, readerFactory)));
  }
}
