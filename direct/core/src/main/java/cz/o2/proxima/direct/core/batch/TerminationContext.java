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
package cz.o2.proxima.direct.core.batch;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.util.ExceptionUtils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** A context that guards correct termination of batch observation process. */
@Internal
@Slf4j
public class TerminationContext implements ObserveHandle {

  private final BatchLogObserver observer;
  private final CountDownLatch terminateLatch = new CountDownLatch(1);
  @Getter private volatile boolean cancelled = false;

  public TerminationContext(BatchLogObserver observer) {
    this.observer = observer;
  }

  /** Force cancellation of {@link BatchLogReader#observe}. */
  public void cancel() {
    cancelled = true;
    while (terminateLatch.getCount() > 0 && !Thread.currentThread().isInterrupted()) {
      ExceptionUtils.ignoringInterrupted(() -> terminateLatch.await(100, TimeUnit.MILLISECONDS));
    }
  }

  public void finished() {
    try {
      if (isCancelled()) {
        observer.onCancelled();
      } else {
        observer.onCompleted();
      }
    } finally {
      markAsDone();
    }
  }

  public void handleErrorCaught(Throwable err, Runnable retry) {
    if (ExceptionUtils.isInterrupted(err)) {
      cancelled = true;
      finished();
    } else {
      log.warn("Exception while running batch observe", err);
      if (observer.onError(err)) {
        retry.run();
      } else {
        markAsDone();
      }
    }
  }

  @VisibleForTesting
  void markAsDone() {
    terminateLatch.countDown();
  }

  @Override
  public void close() {
    cancel();
  }
}
