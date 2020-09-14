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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogObservers {

  /**
   * Create {@link LogObserver} that observes data in event time order.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBuffer(LogObserver upstream, Duration allowedLateness) {

    return withSortBuffer(upstream, allowedLateness, lateDataLoggingConsumer(allowedLateness));
  }

  /**
   * Create {@link LogObserver} that observes data in event time order.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @param latecomerConsumer consumer of data that had to be dropped due to allowed lateness *
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBuffer(
      LogObserver upstream, Duration allowedLateness, Consumer<StreamElement> latecomerConsumer) {

    return withSortBuffer(
        upstream,
        allowedLateness,
        (el, ctx) -> {
          latecomerConsumer.accept(el);
          ctx.confirm();
        });
  }

  /**
   * Create {@link LogObserver} that observes data in event time order.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @param latecomerConsumer consumer of data that had to be dropped due to allowed lateness *
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBuffer(
      LogObserver upstream,
      Duration allowedLateness,
      BiConsumer<StreamElement, OnNextContext> latecomerConsumer) {

    return new SortedLogObserver(upstream, allowedLateness, latecomerConsumer);
  }

  /**
   * Create {@link LogObserver} that observes data in event time order per partition.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBufferWithinPartition(
      LogObserver upstream, Duration allowedLateness) {

    return withSortBufferWithinPartition(
        upstream, allowedLateness, lateDataLoggingConsumer(allowedLateness));
  }

  /**
   * Create {@link LogObserver} that observes data in event time order per partition.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @param latecomerConsumer consumer of data that had to be dropped due to allowed lateness *
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBufferWithinPartition(
      LogObserver upstream, Duration allowedLateness, Consumer<StreamElement> latecomerConsumer) {
    return withSortBufferWithinPartition(
        upstream,
        allowedLateness,
        (el, ctx) -> {
          latecomerConsumer.accept(el);
          ctx.confirm();
        });
  }

  /**
   * Create {@link LogObserver} that observes data in event time order per partition.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @param latecomerConsumer consumer of data that had to be dropped due to allowed lateness *
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBufferWithinPartition(
      LogObserver upstream,
      Duration allowedLateness,
      BiConsumer<StreamElement, OnNextContext> latecomerConsumer) {

    return new SinglePartitionSortedLogObserver(upstream, allowedLateness, latecomerConsumer);
  }

  private LogObservers() {}

  private abstract static class AbstractSortedLogObserver implements LogObserver {

    final LogObserver upstream;
    final long allowedLatenessMs;
    final BiConsumer<StreamElement, OnNextContext> latecomerConsumer;

    public AbstractSortedLogObserver(
        LogObserver upstream,
        Duration allowedLateness,
        BiConsumer<StreamElement, OnNextContext> latecomerConsumer) {

      this.upstream = upstream;
      this.allowedLatenessMs = allowedLateness.toMillis();
      this.latecomerConsumer = latecomerConsumer;
    }

    @Override
    public void onCompleted() {
      onCompletedDrainQueue();
      upstream.onCompleted();
    }

    @Override
    public void onCancelled() {
      reassignPartitions(Collections.emptyList());
      upstream.onCancelled();
    }

    @Override
    public boolean onError(Throwable error) {
      return upstream.onError(error);
    }

    @Override
    public boolean onNext(StreamElement ingest, OnNextContext context) {
      long watermark = getWatermark(context);
      if (watermark - allowedLatenessMs <= ingest.getStamp()) {
        enqueue(ingest, context);
      } else {
        latecomerConsumer.accept(ingest, context);
      }
      return onNextDrainQueue(context);
    }

    @Override
    public void onRepartition(OnRepartitionContext context) {
      reassignPartitions(context.partitions());
      upstream.onRepartition(context);
    }

    @Override
    public void onIdle(OnIdleContext context) {
      onIdleDrainQueue(context);
      upstream.onIdle(context);
    }

    abstract void enqueue(StreamElement ingest, OnNextContext context);

    abstract void onCompletedDrainQueue();

    abstract boolean onNextDrainQueue(OnNextContext context);

    abstract void onIdleDrainQueue(OnIdleContext context);

    protected abstract void reassignPartitions(Collection<Partition> partitions);

    abstract long getWatermark(OnNextContext context);

    boolean drainQueue(PriorityQueue<Pair<StreamElement, OnNextContext>> queue, long maxTimestamp) {
      boolean cont = true;
      while (!queue.isEmpty() && cont) {
        cont = false;
        if (queue.peek().getFirst().getStamp() < maxTimestamp) {
          Pair<StreamElement, OnNextContext> p = queue.poll();
          if (!upstream.onNext(p.getFirst(), p.getSecond())) {
            queue.clear();
            return false;
          }
          cont = true;
        }
      }
      return true;
    }
  }

  private static class SortedLogObserver extends AbstractSortedLogObserver {

    private final PriorityQueue<Pair<StreamElement, OnNextContext>> queue;

    public SortedLogObserver(
        LogObserver upstream,
        Duration allowedLateness,
        BiConsumer<StreamElement, OnNextContext> latecomerConsumer) {

      super(upstream, allowedLateness, latecomerConsumer);
      this.queue = new PriorityQueue<>(Comparator.comparing(p -> p.getFirst().getStamp()));
    }

    @Override
    void enqueue(StreamElement ingest, OnNextContext context) {
      queue.add(Pair.of(ingest, context));
    }

    boolean onNextDrainQueue(OnNextContext context) {
      return drainQueue(queue, context.getWatermark());
    }

    @Override
    void onIdleDrainQueue(OnIdleContext context) {
      drainQueue(queue, context.getWatermark() - allowedLatenessMs);
    }

    @Override
    protected void reassignPartitions(Collection<Partition> partitions) {
      queue.clear();
    }

    @Override
    void onCompletedDrainQueue() {
      drainQueue(queue, Long.MAX_VALUE);
    }

    @Override
    long getWatermark(OnNextContext context) {
      return context.getWatermark();
    }
  }

  private static class SinglePartitionSortedLogObserver extends AbstractSortedLogObserver {

    private final Map<Integer, AtomicLong> watermarkMap;
    private final Map<Integer, PriorityQueue<Pair<StreamElement, OnNextContext>>> queueMap;

    public SinglePartitionSortedLogObserver(
        LogObserver upstream,
        Duration allowedLateness,
        BiConsumer<StreamElement, OnNextContext> latecomerConsumer) {

      super(upstream, allowedLateness, latecomerConsumer);
      this.watermarkMap = new HashMap<>();
      this.queueMap = new HashMap<>();
    }

    @Override
    void enqueue(StreamElement ingest, OnNextContext context) {
      // move global watermark
      watermarkMap.values().forEach(w -> w.accumulateAndGet(context.getWatermark(), Math::max));
      // move partition watermark
      watermarkMap
          .get(context.getPartition().getId())
          .accumulateAndGet(ingest.getStamp(), Math::max);
      queueMap.get(context.getPartition().getId()).add(Pair.of(ingest, context));
    }

    @Override
    boolean onNextDrainQueue(OnNextContext context) {
      // drain all queues, because of possible global watermark move
      return queueMap
          .entrySet()
          .stream()
          .map(
              entry ->
                  drainQueue(
                      entry.getValue(), watermarkMap.get(entry.getKey()).get() - allowedLatenessMs))
          .reduce(Boolean::logicalAnd)
          .orElse(true);
    }

    @Override
    void onIdleDrainQueue(OnIdleContext context) {
      queueMap
          .values()
          .stream()
          .forEach(queue -> drainQueue(queue, context.getWatermark() - allowedLatenessMs));
    }

    @Override
    protected void reassignPartitions(Collection<Partition> partitions) {
      queueMap.clear();
      watermarkMap.clear();
      partitions.forEach(
          part -> {
            queueMap.put(
                part.getId(),
                new PriorityQueue<>(Comparator.comparing(p -> p.getFirst().getStamp())));
            watermarkMap.put(part.getId(), new AtomicLong(Long.MIN_VALUE + allowedLatenessMs));
          });
    }

    @Override
    void onCompletedDrainQueue() {
      queueMap.values().stream().forEach(queue -> drainQueue(queue, Long.MAX_VALUE));
    }

    @Override
    long getWatermark(OnNextContext context) {
      return watermarkMap.get(context.getPartition().getId()).get();
    }
  }

  private static Consumer<StreamElement> lateDataLoggingConsumer(Duration allowedLateness) {
    long allowedLatenessMs = allowedLateness.toMillis();
    return el ->
        log.warn("Element {} dropped due to allowed lateness {}", el.dump(), allowedLatenessMs);
  }
}
