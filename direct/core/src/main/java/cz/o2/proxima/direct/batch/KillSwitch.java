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
import java.util.concurrent.atomic.AtomicBoolean;

/** A kill switch to terminate observation of events. */
@Internal
public class KillSwitch {

  private final AtomicBoolean flag = new AtomicBoolean(false);

  /** Fire the kill switch. */
  public void fire() {
    flag.set(true);
  }

  /** @return {@code true} if the switch is fired. */
  public boolean isFired() {
    return flag.get();
  }

  /**
   * Fire the switch if not already fired.
   *
   * @return {@code true} if the switch was not already fired
   */
  boolean fireIfNotFired() {
    return !flag.getAndSet(true);
  }

  /**
   * Terminate if {@link Thread#currentThread()} is interrupted. Notify provided observer by call to
   * {@link BatchLogObserver#onCancelled()}.
   *
   * @param observer the observer to cancel if current thread is interrupted
   */
  @Internal
  public void cancelIfInterrupted(BatchLogObserver observer) {
    if (Thread.currentThread().isInterrupted() && fireIfNotFired()) {
      observer.onCancelled();
    }
  }
}
