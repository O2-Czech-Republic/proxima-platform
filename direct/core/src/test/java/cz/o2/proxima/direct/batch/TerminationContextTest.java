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
package cz.o2.proxima.direct.batch;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;

/** Test {@link TerminationContext}. */
public class TerminationContextTest {

  private final List<StreamElement> observed = new ArrayList<>();
  private boolean completed = false;
  private boolean cancelled = false;
  private Throwable error = null;
  private boolean retryError = false;
  private final BatchLogObserver observer =
      new BatchLogObserver() {

        boolean completed;

        @Override
        public boolean onNext(StreamElement element, OnNextContext context) {
          observed.add(element);
          return true;
        }

        @Override
        public void onCompleted() {
          completed = true;
        }

        @Override
        public void onCancelled() {
          cancelled = true;
        }

        @Override
        public boolean onError(Throwable error) {
          TerminationContextTest.this.error = error;
          return retryError;
        }
      };

  @Before
  public void setUp() {
    observed.clear();
    completed = false;
    cancelled = false;
    error = null;
    retryError = false;
  }

  @Test
  public void testIsCancelled() {
    TerminationContext context = new TerminationContext(observer);
    assertFalse(context.isCancelled());
    Thread.currentThread().interrupt();
    assertFalse(context.isCancelled());
    clearInterrupted();
    context.setRunningThread();
    Thread.currentThread().interrupt();
    assertTrue(context.isCancelled());
    clearInterrupted();
    assertFalse(context.isCancelled());
    context.cancel();
    assertTrue(context.isCancelled());
    context.finished();
    assertTrue(cancelled);
  }

  @Test
  public void testCancelFuture() throws InterruptedException {
    TerminationContext context = new TerminationContext(observer);
    ExecutorService pool = Executors.newCachedThreadPool();
    AtomicBoolean cancelFlag = new AtomicBoolean();
    CountDownLatch cancelLatch = new CountDownLatch(1);
    CountDownLatch initLatch = new CountDownLatch(1);
    pool.submit(
        () -> {
          context.setRunningThread();
          initLatch.countDown();
          while (!context.isCancelled()) {
            ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(10));
          }
          cancelFlag.set(Thread.currentThread().isInterrupted());
          context.finished();
          cancelLatch.countDown();
        });
    initLatch.await();
    context.cancel();
    assertTrue(cancelLatch.await(1, TimeUnit.SECONDS));
    assertTrue(cancelled);
    assertTrue(cancelFlag.get());
  }

  @Test
  public void testFinished() throws InterruptedException {
    TerminationContext context = new TerminationContext(observer);
    ExecutorService pool = Executors.newCachedThreadPool();
    AtomicBoolean cancelFlag = new AtomicBoolean();
    CountDownLatch finishedLatch = new CountDownLatch(1);
    CountDownLatch initLatch = new CountDownLatch(1);
    BlockingQueue<Thread> thread = new SynchronousQueue<>();
    pool.submit(
        () -> {
          ExceptionUtils.ignoringInterrupted(() -> thread.put(Thread.currentThread()));
          context.setRunningThread();
          initLatch.countDown();
          int rounds = 0;
          while (!context.isCancelled() && rounds++ < 10) {
            ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(10));
          }
          cancelFlag.set(Thread.currentThread().isInterrupted());
          context.finished();
          finishedLatch.countDown();
        });
    final Thread runningThread;
    try (ObserveHandle handle = context.asObserveHandle()) {
      runningThread = thread.take();
      initLatch.await();
      assertTrue(finishedLatch.await(1, TimeUnit.SECONDS));
    }
    assertFalse(runningThread.isInterrupted());
    assertFalse(cancelled);
    assertFalse(cancelFlag.get());
  }

  @Test
  public void testErrorCaught() {
    TerminationContext context = new TerminationContext(observer);
    AtomicBoolean called = new AtomicBoolean();
    context.handleErrorCaught(new InterruptedException(), () -> called.set(true));
    assertTrue(cancelled);
    cancelled = false;
    assertFalse(called.get());
    context.handleErrorCaught(new InterruptedException(), () -> called.set(true));
    context.handleErrorCaught(new RuntimeException(), () -> called.set(true));
    assertFalse(called.get());
    retryError = true;
    context.handleErrorCaught(new RuntimeException(), () -> called.set(true));
    assertTrue(called.get());
  }

  private void clearInterrupted() {
    try {
      Thread.currentThread().interrupt();
      TimeUnit.MILLISECONDS.sleep(10);
    } catch (InterruptedException ex) {
      // nop
    }
  }
}
