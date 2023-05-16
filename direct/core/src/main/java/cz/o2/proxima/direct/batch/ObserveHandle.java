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
package cz.o2.proxima.direct.batch;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.annotations.Stable;

/** A interface for handling progress and control consumption of running observe process. */
@Stable
public interface ObserveHandle extends AutoCloseable {

  /** @return an {@link ObserveHandle} which does nothing. */
  @Internal
  static ObserveHandle noop() {
    return () -> {};
  }

  /**
   * Verify if we can read without any (known in advance) delays defined by any possible {@link
   * cz.o2.proxima.core.storage.ThroughputLimiter}.
   */
  default boolean isReadyForProcessing() {
    return true;
  }

  /** Stop the consumption. */
  @Override
  void close();

  /** Disable any rate limit for this handle. */
  default void disableRateLimiting() {
    // nop
  }
}
