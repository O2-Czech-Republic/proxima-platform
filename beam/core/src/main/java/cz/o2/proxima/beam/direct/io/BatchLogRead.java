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
import com.google.common.collect.Lists;
import cz.o2.proxima.beam.direct.io.BatchRestrictionTracker.PartitionList;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.BatchLogReader.Factory;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A {@link PTransform} that reads from a {@link BatchLogReader} using splittable DoFn. */
public class BatchLogRead extends PTransform<PBegin, PCollection<StreamElement>> {

  /**
   * Create the {@link BatchLogRead} transform.
   *
   * @param attributes the attributes to read
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repo repository
   * @param reader the reader
   * @return {@link BatchLogRead} transform for the commit log
   */
  public static BatchLogRead of(
      List<AttributeDescriptor<?>> attributes, long limit, Repository repo, BatchLogReader reader) {

    return of(attributes, limit, repo.asFactory(), reader);
  }

  /**
   * Create the {@link BatchLogRead} transform.
   *
   * @param attributes the attributes to read
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repositoryFactory repository factory
   * @param reader the reader
   * @return {@link CommitLogRead} transform for the commit log
   */
  public static BatchLogRead of(
      List<AttributeDescriptor<?>> attributes,
      long limit,
      RepositoryFactory repositoryFactory,
      BatchLogReader reader) {

    return new BatchLogRead(attributes, limit, repositoryFactory, reader);
  }

  @DoFn.BoundedPerElement
  private class BatchLogReadFn extends DoFn<byte[], StreamElement> {

    private final List<AttributeDescriptor<?>> attributes;
    private final RepositoryFactory repositoryFactory;
    private final BatchLogReader.Factory<?> readerFactory;
    private final long limit;

    private BatchLogReadFn(
        List<AttributeDescriptor<?>> attributes,
        long limit,
        RepositoryFactory repositoryFactory,
        BatchLogReader.Factory<?> readerFactory) {

      this.attributes = Objects.requireNonNull(attributes);
      this.repositoryFactory = repositoryFactory;
      this.readerFactory = readerFactory;
      this.limit = limit;
    }

    @ProcessElement
    public ProcessContinuation process(
        RestrictionTracker<PartitionList, Partition> tracker,
        OutputReceiver<StreamElement> output) {

      if (tracker.currentRestriction().isEmpty()) {
        return ProcessContinuation.stop();
      }

      while (!tracker.currentRestriction().isFinished()) {

        PartitionList restriction = Objects.requireNonNull(tracker.currentRestriction());
        Partition part = Objects.requireNonNull(restriction.getFirstPartition());

        BlockingQueueLogObserver observer =
            newObserver("observer-" + part.getId(), restriction.getTotalLimit());

        if (!tracker.tryClaim(part)) {
          return ProcessContinuation.stop();
        }
        try (ObserveHandle handle = startObserve(part, observer)) {
          while (observer.getWatermark() < Watermarks.MAX_WATERMARK
              && !restriction.isLimitConsumed()) {

            StreamElement element = observer.takeBlocking(30, TimeUnit.SECONDS);
            if (element != null) {
              restriction.reportConsumed();
              output.outputWithTimestamp(element, Instant.ofEpochMilli(element.getStamp()));
            }
          }
          Optional.ofNullable(observer.getError())
              .ifPresent(ExceptionUtils::rethrowAsIllegalStateException);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      boolean terminated = tracker.currentRestriction().isFinished();
      return terminated
          ? ProcessContinuation.stop()
          : ProcessContinuation.resume().withResumeDelay(Duration.millis(100));
    }

    private ObserveHandle startObserve(Partition partition, BatchLogObserver observer) {
      BatchLogReader reader = readerFactory.apply(repositoryFactory.apply());
      return reader.observe(Collections.singletonList(partition), attributes, observer);
    }

    @GetInitialRestriction
    public PartitionList initialRestriction() {
      BatchLogReader reader = readerFactory.apply(repoFactory.apply());
      return PartitionList.initialRestriction(reader.getPartitions(), limit);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction PartitionList restriction, OutputReceiver<PartitionList> splits) {

      if (!restriction.isEmpty()) {
        restriction
            .getPartitions()
            .forEach(
                p ->
                    splits.output(PartitionList.ofSinglePartition(p, restriction.getTotalLimit())));
      } else {
        splits.output(restriction);
      }
    }

    @GetRestrictionCoder
    public Coder<PartitionList> getRestrictionCoder() {
      return SerializableCoder.of(PartitionList.class);
    }

    @NewWatermarkEstimator
    public Manual newWatermarkEstimator(@WatermarkEstimatorState Instant initialWatemark) {
      return new Manual(ensureInBounds(initialWatemark));
    }

    private Instant ensureInBounds(Instant instant) {
      if (instant.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
        return BoundedWindow.TIMESTAMP_MIN_VALUE;
      }
      if (instant.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
      return instant;
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @GetWatermarkEstimatorStateCoder
    public Coder<Instant> getWatermarkEstimatorStateCoder() {
      return InstantCoder.of();
    }
  }

  private final List<AttributeDescriptor<?>> attributes;
  private final long limit;
  private final RepositoryFactory repoFactory;
  private final Factory<?> readerFactory;

  @VisibleForTesting
  BatchLogRead(
      List<AttributeDescriptor<?>> attributes,
      long limit,
      RepositoryFactory repoFactory,
      BatchLogReader reader) {

    this.attributes = Lists.newArrayList(Objects.requireNonNull(attributes));
    this.limit = limit;
    this.repoFactory = repoFactory;
    this.readerFactory = reader.asFactory();
  }

  @Override
  public PCollection<StreamElement> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(ParDo.of(new BatchLogReadFn(attributes, limit, repoFactory, readerFactory)));
  }

  @VisibleForTesting
  BlockingQueueLogObserver newObserver(String name, long limit) {
    return BlockingQueueLogObserver.create(name, limit, Watermarks.MIN_WATERMARK);
  }
}
