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

import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** A {@link LogObserver} that caches data in {@link BlockingQueue}. */
@Slf4j
class BlockingQueueLogObserver implements LogObserver, BatchLogObserver {

  static BlockingQueueLogObserver create(long startingWatermark) {
    return create(null, Long.MAX_VALUE, startingWatermark);
  }

  static BlockingQueueLogObserver create(String name, long limit, long startingWatermark) {
    return new BlockingQueueLogObserver(name, limit, startingWatermark);
  }

  @Nullable private final String name;
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final AtomicLong watermark;
  private final BlockingQueue<Pair<StreamElement, OnNextContext>> queue;
  AtomicBoolean stopped = new AtomicBoolean();
  @Getter @Nullable private OnNextContext lastWrittenContext;
  @Getter @Nullable private OnNextContext lastReadContext;
  private long limit;

  private BlockingQueueLogObserver(String name, long limit, long startingWatermark) {

    this.name = name;
    this.watermark = new AtomicLong(startingWatermark);
    this.limit = limit;
    queue = new ArrayBlockingQueue<>(100);
  }

  @Override
  public boolean onError(Throwable error) {
    this.error.set(error);
    // unblock any waiting thread
    ExceptionUtils.unchecked(() -> putToQueue(null, null));
    return false;
  }

  @Override
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    watermark.set(context.getWatermark());
    log.trace("Received next element {} at watermark {}", ingest, watermark);
    return enqueue(ingest, context);
  }

  @Override
  public boolean onNext(StreamElement element, Partition partition) {
    log.trace("Received next element {}", element);
    return enqueue(element, null);
  }

  private boolean enqueue(StreamElement element, OnNextContext context) {
    try {
      lastWrittenContext = context;
      if (limit-- > 0) {
        return putToQueue(element, context);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  @Override
  public void onCancelled() {
    log.debug("Cancelled {} consumption by request.", name);
  }

  @Override
  public void onCompleted() {
    try {
      putToQueue(null, null);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted while passing end-of-stream.", ex);
    }
  }

  private boolean putToQueue(@Nullable StreamElement element, @Nullable OnNextContext context)
      throws InterruptedException {

    Pair<StreamElement, OnNextContext> p = Pair.of(element, context);
    while (!stopped.get()) {
      if (queue.offer(p, 50, TimeUnit.MILLISECONDS)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void onIdle(OnIdleContext context) {
    watermark.set(context.getWatermark());
  }

  /**
   * Take next element without blocking.
   *
   * @return {@code} element that was taken without blocking or {@code null} otherwise
   */
  @Nullable
  StreamElement take() {
    Pair<StreamElement, OnNextContext> taken = null;
    if (!stopped.get()) {
      taken = queue.poll();
    }
    return consumeTaken(taken);
  }

  /**
   * Take next element waiting for input if necessary.
   *
   * @return {@code} element that was taken or {@code null} on end of input
   */
  @Nullable
  StreamElement takeBlocking() throws InterruptedException {
    Pair<StreamElement, OnNextContext> taken = null;
    if (!stopped.get()) {
      taken = queue.take();
    }
    return consumeTaken(taken);
  }

  private StreamElement consumeTaken(@Nullable Pair<StreamElement, OnNextContext> taken) {
    if (taken != null && taken.getFirst() != null) {
      lastReadContext = taken.getSecond();
      return taken.getFirst();
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
    stopped.set(true);
    List<Pair<StreamElement, OnNextContext>> drop = new ArrayList<>();
    queue.drainTo(drop);
    drop.forEach(p -> p.getSecond().nack());
  }

  void clearIncomingQueue() {
    queue.clear();
  }
}
