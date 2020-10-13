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
package cz.o2.proxima.direct.batch;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/** A context that guards correct termination of batch observation process. */
@Internal
@Slf4j
public class TerminationContext {

  private final BatchLogObserver observer;
  private final CountDownLatch terminateLatch = new CountDownLatch(1);
  private volatile boolean cancelled = false;
  private volatile Thread runningThread;

  public TerminationContext(BatchLogObserver observer) {
    this.observer = observer;
  }

  public boolean isCancelled() {
    return cancelled || runningThread == Thread.currentThread() && runningThread.isInterrupted();
  }

  /** Force cancellation of the observe. */
  public void cancel() {
    setCancelled();
    while (!Thread.currentThread().isInterrupted()) {
      ExceptionUtils.ignoringInterrupted(() -> terminateLatch.await(1, TimeUnit.SECONDS));
      if (terminateLatch.getCount() <= 0) {
        break;
      }
      Optional.ofNullable(runningThread).ifPresent(Thread::interrupt);
    }
  }

  /** Set the thread running the task to be terminated to the calling thread. */
  public void setRunningThread() {
    runningThread = Thread.currentThread();
  }

  public void finished() {
    if (isCancelled()) {
      observer.onCancelled();
    } else {
      observer.onCompleted();
    }
    terminateLatch.countDown();
  }

  public ObserveHandle asObserveHandle() {
    return () -> {
      cancel();
      ExceptionUtils.ignoringInterrupted(terminateLatch::await);
    };
  }

  public void handleErrorCaught(Throwable err, Runnable retry) {
    if (ExceptionUtils.isInterrupted(err)) {
      setCancelled();
      finished();
    } else {
      log.warn("Exception while running batch observe", err);
      if (observer.onError(err)) {
        retry.run();
      } else {
        terminateLatch.countDown();
      }
    }
  }

  private void setCancelled() {
    this.cancelled = true;
  }
}
