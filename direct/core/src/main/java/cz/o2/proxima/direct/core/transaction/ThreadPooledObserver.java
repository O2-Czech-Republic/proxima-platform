/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.transaction;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Internal
@Slf4j
public class ThreadPooledObserver implements CommitLogObserver {

  private final CommitLogObserver delegate;
  private final List<BlockingQueue<Pair<StreamElement, OnNextContext>>> workQueues =
      new ArrayList<>();
  private final List<AtomicBoolean> processing = new ArrayList<>();
  private final List<Future<?>> futures = new ArrayList<>();

  public ThreadPooledObserver(
      ExecutorService executorService, CommitLogObserver requestObserver, int parallelism) {

    this.delegate = requestObserver;
    for (int i = 0; i < parallelism; i++) {
      BlockingQueue<Pair<StreamElement, OnNextContext>> queue = new ArrayBlockingQueue<>(50);
      workQueues.add(queue);
      AtomicBoolean processingFlag = new AtomicBoolean();
      processing.add(processingFlag);
      futures.add(executorService.submit(() -> processQueue(queue, processingFlag)));
    }
  }

  private void processQueue(
      BlockingQueue<Pair<StreamElement, OnNextContext>> queue, AtomicBoolean processingFlag) {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        ExceptionUtils.ignoringInterrupted(
            () -> {
              Pair<StreamElement, OnNextContext> polled = queue.take();
              processingFlag.set(true);
              delegate.onNext(polled.getFirst(), polled.getSecond());
              processingFlag.set(false);
            });
      }
    } catch (Throwable err) {
      log.error("Error processing input queue.", err);
      onError(err);
    }
  }

  @Override
  public void onCompleted() {
    waitTillQueueEmpty();
    exitThreads();
    if (!Thread.currentThread().isInterrupted()) {
      delegate.onCompleted();
    }
  }

  @Override
  public void onCancelled() {
    waitTillQueueEmpty();
    exitThreads();
    if (!Thread.currentThread().isInterrupted()) {
      delegate.onCancelled();
    }
  }

  @Override
  public boolean onError(Throwable error) {
    waitTillQueueEmpty();
    exitThreads();
    return delegate.onError(error);
  }

  private void exitThreads() {
    futures.forEach(f -> f.cancel(true));
  }

  @Override
  public boolean onNext(StreamElement element, OnNextContext context) {
    return !ExceptionUtils.ignoringInterrupted(
        () ->
            workQueues
                .get((element.getKey().hashCode() & Integer.MAX_VALUE) % workQueues.size())
                .put(Pair.of(element, context)));
  }

  @Override
  public void onRepartition(OnRepartitionContext context) {
    waitTillQueueEmpty();
    delegate.onRepartition(context);
  }

  @Override
  public void onIdle(OnIdleContext context) {
    if (workQueueEmpty()) {
      delegate.onIdle(context);
    }
  }

  private boolean workQueueEmpty() {
    return workQueues.stream().allMatch(BlockingQueue::isEmpty);
  }

  private boolean anyInProgress() {
    return processing.stream().anyMatch(AtomicBoolean::get);
  }

  private void waitTillQueueEmpty() {
    while (anyInProgress() && !workQueueEmpty() && !Thread.currentThread().isInterrupted()) {
      ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(100));
    }
  }
}
