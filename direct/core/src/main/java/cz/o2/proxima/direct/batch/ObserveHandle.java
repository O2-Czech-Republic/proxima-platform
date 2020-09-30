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
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** A interface for handling progress and control consumption of running observe process. */
@Stable
public interface ObserveHandle extends AutoCloseable {

  /**
   * Return {@link ObserveHandle} that does nothing.
   *
   * @return a no-op {@link ObserveHandle}
   */
  @Internal
  static ObserveHandle noop() {
    return () -> {};
  }

  /**
   * Wrap given {@link AtomicBoolean} used as kill switch to {@link ObserveHandle}.
   *
   * @param killSwitch a {@link AtomicBoolean} to set to {@code true} on cancel to stop and to
   *     prevent restarting the observe thread
   * @param observer the observer that observes the reader
   * @return the {@link ObserveHandle}
   */
  @Internal
  static ObserveHandle createFrom(
      AtomicBoolean killSwitch, CountDownLatch terminateLatch, BatchLogObserver observer) {

    return () -> {
      killSwitch.set(true);
      ExceptionUtils.ignoringInterrupted(terminateLatch::await);
      observer.onCancelled();
    };
  }

  /** Stop the consumption. */
  @Override
  void close();
}
