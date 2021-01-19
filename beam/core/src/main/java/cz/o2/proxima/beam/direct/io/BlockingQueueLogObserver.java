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
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkSupplier;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** A {@link LogObserver} that caches data in {@link BlockingQueue}. */
@Slf4j
final class BlockingQueueLogObserver implements LogObserver, BatchLogObserver {

  private static final long serialVersionUID = 1L;

  static BlockingQueueLogObserver create(String name, long startingWatermark) {
    return create(name, Long.MAX_VALUE, startingWatermark);
  }

  static BlockingQueueLogObserver create(String name, long limit, long startingWatermark) {
    return new BlockingQueueLogObserver(name, limit, startingWatermark);
  }

  static BlockingQueueLogObserver create(
      String name, long limit, long startingWatermark, int queueCapacity) {
    return new BlockingQueueLogObserver(name, limit, startingWatermark, queueCapacity);
  }

  @Internal
  interface UnifiedContext extends OffsetCommitter, WatermarkSupplier {

    boolean isBounded();

    @Nullable
    Offset getOffset();
  }

  @ToString
  @VisibleForTesting
  static class LogObserverUnifiedContext implements UnifiedContext {

    private static final long serialVersionUID = 1L;

    private final LogObserver.OnNextContext context;

    LogObserverUnifiedContext(LogObserver.OnNextContext context) {
      this.context = context;
    }

    @Override
    public void commit(boolean success, Throwable error) {
      context.commit(success, error);
    }

    @Override
    public void nack() {
      context.nack();
    }

    @Override
    public long getWatermark() {
      return context.getWatermark();
    }

    @Override
    public boolean isBounded() {
      return false;
    }

    @Nullable
    @Override
    public Offset getOffset() {
      return context.getOffset();
    }
  }

  @ToString
  private static class BatchLogObserverUnifiedContext implements UnifiedContext {

    private static final long serialVersionUID = 1L;

    private final BatchLogObserver.OnNextContext context;

    private BatchLogObserverUnifiedContext(BatchLogObserver.OnNextContext context) {
      this.context = context;
    }

    @Override
    public void commit(boolean success, Throwable error) {
      if (error != null) {
        throw new RuntimeException(error);
      }
    }

    @Override
    public void nack() {
      // nop
    }

    @Override
    public long getWatermark() {
      return context.getWatermark();
    }

    @Override
    public boolean isBounded() {
      return true;
    }

    @Nullable
    @Override
    public Offset getOffset() {
      return null;
    }
  }

  @Getter private final String name;
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final AtomicLong watermark;

  private final BlockingQueue<Pair<StreamElement, UnifiedContext>> queue;

  private volatile boolean stopped = false;
  private volatile boolean nackAllIncoming = false;
  @Getter @Nullable private UnifiedContext lastWrittenContext;
  @Getter @Nullable private UnifiedContext lastReadContext;
  @Nullable private Pair<StreamElement, UnifiedContext> peekElement = null;
  private long limit;
  private boolean cancelled = false;
  private transient CountDownLatch cancelledLatch = new CountDownLatch(1);

  private BlockingQueueLogObserver(String name, long limit, long startingWatermark) {
    this(name, limit, startingWatermark, 100);
  }

  private BlockingQueueLogObserver(String name, long limit, long startingWatermark, int capacity) {
    Preconditions.checkArgument(limit >= 0, "Please provide non-negative limit");

    this.name = Objects.requireNonNull(name);
    this.watermark = new AtomicLong(startingWatermark);
    this.limit = limit;
    queue = new ArrayBlockingQueue<>(capacity);
    log.debug("Created {}", this);
  }

  @Override
  public boolean onError(Throwable error) {
    this.error.set(error);
    // unblock any waiting thread
    ExceptionUtils.unchecked(() -> putToQueue(null, null));
    return false;
  }

  @Override
  public boolean onNext(StreamElement ingest, LogObserver.OnNextContext context) {
    if (log.isDebugEnabled()) {
      log.debug(
          "{}: Received next element {} at watermark {} offset {}",
          name,
          ingest,
          context.getWatermark(),
          context.getOffset());
    }
    return enqueue(ingest, new LogObserverUnifiedContext(context));
  }

  @Override
  public boolean onNext(StreamElement element, BatchLogObserver.OnNextContext context) {
    log.debug(
        "{}: Received next element {} on partition {}", name, element, context.getPartition());
    return enqueue(element, new BatchLogObserverUnifiedContext(context));
  }

  private boolean enqueue(StreamElement element, UnifiedContext context) {
    try {
      Preconditions.checkArgument(element != null && context != null);
      lastWrittenContext = context;
      if (limit-- > 0) {
        return putToQueue(element, context);
      }
      log.debug(
          "{}: Terminating consumption due to limit {} while enqueuing {}", name, limit, element);
    } catch (InterruptedException ex) {
      log.warn("Interrupted while putting element {} to queue", element, ex);
      Thread.currentThread().interrupt();
    } finally {
      if (nackAllIncoming && context != null) {
        context.nack();
      }
    }
    return false;
  }

  @Override
  public void onCancelled() {
    cancelled = true;
    cancelledLatch.countDown();
    log.debug("{}: Cancelled consumption by request.", name);
  }

  @Override
  public void onCompleted() {
    try {
      log.debug("{}: Finished reading from observer", name);
      putToQueue(null, null);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      log.warn("{}: Interrupted while passing end-of-stream.", name, ex);
    }
  }

  private boolean putToQueue(@Nullable StreamElement element, @Nullable UnifiedContext context)
      throws InterruptedException {

    Pair<StreamElement, UnifiedContext> p = Pair.of(element, context);
    while (!stopped) {
      if (queue.offer(p, 50, TimeUnit.MILLISECONDS)) {
        return true;
      }
    }
    log.debug("{}: Finishing consumption due to source being stopped", name);
    return false;
  }

  @Override
  public void onIdle(OnIdleContext context) {
    if (queue.isEmpty()) {
      updateAndLogWatermark(context.getWatermark());
    }
  }

  /**
   * Take next element without blocking.
   *
   * @return element that was taken without blocking or {@code null} otherwise
   */
  @Nullable
  StreamElement take() {
    if (!stopped) {
      try {
        if (peekElement(0, TimeUnit.MILLISECONDS)) {
          return consumePeek();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }

  @Nullable
  public UnifiedContext getPeekContext() {
    if (peekElement != null) {
      return peekElement.getSecond();
    }
    return null;
  }

  /**
   * Take next element waiting for input if necessary.
   *
   * @return element that was taken or {@code null} on end of input
   */
  @Nullable
  StreamElement takeBlocking(long timeout, TimeUnit unit) throws InterruptedException {
    if (!stopped && peekElement(timeout, unit)) {
      return consumePeek();
    }
    return null;
  }

  /**
   * Take next element waiting for input if necessary.
   *
   * @return element that was taken or {@code null} on end of input
   */
  @Nullable
  StreamElement takeBlocking() throws InterruptedException {
    while (!stopped) {
      if (peekElement(50, TimeUnit.MILLISECONDS)) {
        return consumePeek();
      }
    }
    return null;
  }

  /**
   * Peek element or return {@code null} if queue is empty.
   *
   * @return {@code true} if queue is not empty after the call
   */
  public boolean peekElement() {
    try {
      return peekElement(0, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  private boolean peekElement(long timeout, TimeUnit unit) throws InterruptedException {
    if (peekElement == null) {
      peekElement = queue.poll(timeout, unit);
    }
    if (peekElement != null && peekElement.getFirst() == null) {
      // we have end of input mark
      consumePeek();
      return false;
    }
    return peekElement != null;
  }

  @Nullable
  private StreamElement consumePeek() {
    @Nullable Pair<StreamElement, UnifiedContext> taken = peekElement;
    peekElement = null;
    if (taken != null && taken.getFirst() != null) {
      lastReadContext = taken.getSecond();
      if (lastReadContext != null) {
        updateAndLogWatermark(lastReadContext.getWatermark());
      }
      log.debug(
          "{}: Consuming taken element {} with offset {}",
          name,
          taken.getFirst(),
          lastReadContext != null ? lastReadContext.getOffset() : null);
      return taken.getFirst();
    } else if (taken != null) {
      // we have read the finalizing marker
      updateAndLogWatermark(Long.MAX_VALUE);
      stopped = true;
    }
    return null;
  }

  @Nullable
  Throwable getError() {
    return error.get();
  }

  long getWatermark() {
    return watermark.get();
  }

  void stop() {
    stop(true);
  }

  void stop(boolean nack) {
    nackAllIncoming = nack;
    stopped = true;
    if (nack) {
      List<Pair<StreamElement, UnifiedContext>> drop = new ArrayList<>();
      if (peekElement != null) {
        drop.add(peekElement);
        peekElement = null;
      }
      queue.drainTo(drop);
      drop.stream().map(Pair::getSecond).filter(Objects::nonNull).forEach(UnifiedContext::nack);
    }
    if (getWatermark() < Watermarks.MAX_WATERMARK) {
      ExceptionUtils.ignoringInterrupted(() -> cancelledLatch.await(1, TimeUnit.SECONDS));
    }
  }

  void clearIncomingQueue() {
    queue.clear();
  }

  private void updateAndLogWatermark(long newWatermark) {
    if (!cancelled) {
      if (log.isDebugEnabled() && watermark.get() < newWatermark) {
        log.debug(
            "{}: Watermark updated from {} to {}",
            name,
            Instant.ofEpochMilli(watermark.get()),
            Instant.ofEpochMilli(newWatermark));
      }
      watermark.set(newWatermark);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("limit", limit).toString();
  }

  private Object readResolve() {
    this.cancelledLatch = new CountDownLatch(1);
    return this;
  }
}
