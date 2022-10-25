/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import cz.o2.proxima.beam.direct.io.BatchRestrictionTracker.PartitionList;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
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

/** A {@link PTransform} that reads from a {@link BatchLogReader} using splittable DoFn. */
@Slf4j
public class BatchLogRead extends PTransform<PBegin, PCollection<StreamElement>> {

  public interface BatchLogReadPipelineOptions extends PipelineOptions {
    @Default.Long(0L)
    long getStartBatchReadDelayMs();

    void setStartBatchReadDelayMs(long readDelayMs);
  }

  @AutoService(PipelineOptionsRegistrar.class)
  public static class BatchLogReadOptionsFactory implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return Collections.singletonList(BatchLogReadPipelineOptions.class);
    }
  }

  /**
   * Create the {@link BatchLogRead} transform that reads from {@link BatchLogReader} in batch
   * manner.
   *
   * @param attributes the attributes to read
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repo repository
   * @param reader the reader
   * @return {@link BatchLogRead} transform for the commit log
   */
  public static BatchLogRead of(
      List<AttributeDescriptor<?>> attributes, long limit, Repository repo, BatchLogReader reader) {

    return of(attributes, limit, repo, reader, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /**
   * Create the {@link BatchLogRead} transform that reads from {@link BatchLogReader} in batch
   * manner.
   *
   * @param attributes the attributes to read
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repo repository
   * @param reader the reader
   * @param startStamp starting stamp (inclusive)
   * @param endStamp ending stamp (exclusive)
   * @return {@link BatchLogRead} transform for the commit log
   */
  public static BatchLogRead of(
      List<AttributeDescriptor<?>> attributes,
      long limit,
      Repository repo,
      BatchLogReader reader,
      long startStamp,
      long endStamp) {

    return of(
        attributes, limit, repo.asFactory(), reader, startStamp, endStamp, Collections.emptyMap());
  }

  /**
   * Create the {@link BatchLogRead} transform that reads from {@link BatchLogReader} in batch
   * manner.
   *
   * @param attributes the attributes to read
   * @param limit limit (use {@link Long#MAX_VALUE} for unbounded
   * @param repositoryFactory repository factory
   * @param reader the reader
   * @param startStamp starting stamp (inclusive)
   * @param endStamp ending stamp (exclusive)
   * @param cfg configuration of the family
   * @return {@link CommitLogRead} transform for the commit log
   */
  public static BatchLogRead of(
      List<AttributeDescriptor<?>> attributes,
      long limit,
      RepositoryFactory repositoryFactory,
      BatchLogReader reader,
      long startStamp,
      long endStamp,
      Map<String, Object> cfg) {

    return new BatchLogRead(
        attributes, limit, repositoryFactory, reader.asFactory(), startStamp, endStamp, cfg);
  }

  @BoundedPerElement
  @VisibleForTesting
  class BatchLogReadFn extends DoFn<byte[], StreamElement> {

    private final List<AttributeDescriptor<?>> attributes;
    private final RepositoryFactory repositoryFactory;
    private final BatchLogReader.Factory<?> readerFactory;
    private final long limit;
    private final long startDelayMs;
    private long startedAt;

    BatchLogReadFn(
        List<AttributeDescriptor<?>> attributes,
        long limit,
        long startDelayMs,
        RepositoryFactory repositoryFactory,
        BatchLogReader.Factory<?> readerFactory) {

      this.attributes = Objects.requireNonNull(attributes);
      this.repositoryFactory = repositoryFactory;
      this.readerFactory = readerFactory;
      this.limit = limit;
      this.startDelayMs = startDelayMs;
    }

    @Setup
    public void setUp() {
      startedAt = System.currentTimeMillis();
    }

    @ProcessElement
    public ProcessContinuation process(
        RestrictionTracker<PartitionList, Partition> tracker,
        OutputReceiver<StreamElement> output,
        ManualWatermarkEstimator<Instant> watermarkEstimator) {

      if (tracker.currentRestriction().isEmpty()) {
        return ProcessContinuation.stop();
      }

      if (startDelayMs > 0) {
        long remaining = System.currentTimeMillis() - startedAt - startDelayMs;
        if (remaining < 0) {
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(startDelayMs));
        }
      }

      watermarkEstimator.setWatermark(tracker.currentRestriction().getMinTimestamp());

      PartitionList restriction = Objects.requireNonNull(tracker.currentRestriction());
      Partition part = Objects.requireNonNull(restriction.getFirstPartition());

      log.debug("Starting to process partition {} from restriction {}", part, restriction);

      final BlockingQueueLogObserver.BatchLogObserver observer =
          newObserver("observer-" + part.getId(), restriction.getTotalLimit());

      if (!tracker.tryClaim(part)) {
        return ProcessContinuation.stop();
      }
      try (ObserveHandle handle = startObserve(part, observer)) {
        if (!handle.isReadyForProcessing()) {
          log.debug("Delaying processing of partition {} due to limiter.", part);
          tracker.currentRestriction().reclaim(part);
          return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
        }
        // disable any potential rate limit, we need to pass through the partition as quickly
        // as possible
        handle.disableRateLimiting();
        while (observer.getWatermark() < Watermarks.MAX_WATERMARK
            && !restriction.isLimitConsumed()) {

          StreamElement element = observer.takeBlocking(100, TimeUnit.MILLISECONDS);
          if (element != null) {
            restriction.reportConsumed();
            output.outputWithTimestamp(element, Instant.ofEpochMilli(element.getStamp()));
          }
        }
        log.debug("Finished processing partition {} from restriction {}", part, restriction);
        Optional.ofNullable(observer.getError())
            .ifPresent(ExceptionUtils::rethrowAsIllegalStateException);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      watermarkEstimator.setWatermark(tracker.currentRestriction().getMinTimestamp());

      boolean terminated = tracker.currentRestriction().isFinished();
      return terminated
          ? ProcessContinuation.stop()
          : ProcessContinuation.resume().withResumeDelay(Duration.millis(100));
    }

    private ObserveHandle startObserve(
        Partition partition, cz.o2.proxima.direct.batch.BatchLogObserver observer) {

      BatchLogReader reader = readerFactory.apply(repositoryFactory.apply());
      return reader.observe(Collections.singletonList(partition), attributes, observer);
    }

    @GetInitialRestriction
    public PartitionList initialRestriction() {
      BatchLogReader reader = readerFactory.apply(repoFactory.apply());
      return PartitionList.initialRestriction(reader.getPartitions(startStamp, endStamp), limit);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction PartitionList restriction, OutputReceiver<PartitionList> output) {

      if (!restriction.isEmpty()) {
        List<PartitionList> splits = new ArrayList<>();
        List<Partition> partitions = restriction.getPartitions();
        partitions.sort(Comparator.comparing(Partition::getMinTimestamp));
        int pos = 0;
        int reduced = (int) Math.sqrt(partitions.size());
        if (maxInitialSplits > 0 && reduced > maxInitialSplits) {
          reduced = maxInitialSplits;
        }
        log.info(
            "Splitting initial restriction of attributes {} into {} downstream parts.",
            attributes,
            reduced);
        for (Partition p : partitions) {
          if (splits.size() <= pos) {
            splits.add(
                PartitionList.ofSinglePartition(
                    p, restriction.getTotalLimit() / partitions.size()));
          } else {
            splits.get(pos).add(p);
          }
          pos = (pos + 1) % reduced;
        }
        splits.forEach(output::output);
      } else {
        output.output(restriction);
      }
    }

    @GetRestrictionCoder
    public Coder<PartitionList> getRestrictionCoder() {
      return SerializableCoder.of(PartitionList.class);
    }

    @NewWatermarkEstimator
    public Manual newWatermarkEstimator(@WatermarkEstimatorState Instant initialWatemark) {
      return SDFUtils.rangeCheckedManualEstimator(initialWatemark);
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
  private final long startStamp;
  private final long endStamp;
  private final int maxInitialSplits;

  @VisibleForTesting
  BatchLogRead(
      List<AttributeDescriptor<?>> attributes,
      long limit,
      RepositoryFactory repoFactory,
      BatchLogReader.Factory<?> readerFactory,
      long startStamp,
      long endStamp,
      Map<String, Object> cfg) {

    this.attributes = Lists.newArrayList(Objects.requireNonNull(attributes));
    this.limit = limit;
    this.repoFactory = repoFactory;
    this.readerFactory = readerFactory;
    this.startStamp = startStamp;
    this.endStamp = endStamp;
    this.maxInitialSplits = readInitialSplits(cfg);
  }

  private int readInitialSplits(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get("batch.max-initial-splits"))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(-1);
  }

  @Override
  public PCollection<StreamElement> expand(PBegin input) {
    long delayMs =
        input
            .getPipeline()
            .getOptions()
            .as(BatchLogReadPipelineOptions.class)
            .getStartBatchReadDelayMs();
    return input
        .apply(Impulse.create())
        .apply(
            ParDo.of(new BatchLogReadFn(attributes, limit, delayMs, repoFactory, readerFactory)));
  }

  @VisibleForTesting
  BlockingQueueLogObserver.BatchLogObserver newObserver(String name, long limit) {
    return BlockingQueueLogObserver.createBatchLogObserver(name, limit, Watermarks.MIN_WATERMARK);
  }
}
