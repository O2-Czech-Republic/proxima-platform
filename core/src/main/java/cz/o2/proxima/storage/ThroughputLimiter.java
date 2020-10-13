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
package cz.o2.proxima.storage;

import cz.o2.proxima.annotations.Evolving;
import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/** A limiter of data rate coming from various sources. */
@Evolving
public interface ThroughputLimiter extends Serializable, Closeable {

  class NoOpThroughputLimiter implements ThroughputLimiter {

    private static final long serialVersionUID = 1L;

    public static final ThroughputLimiter INSTANCE = new NoOpThroughputLimiter();

    @Override
    public Duration getPauseTime(Context context) {
      return Duration.ZERO;
    }

    @Override
    public void close() {
      // nop
    }
  }

  /** A context of {@link #getPauseTime} method. */
  interface Context {

    /**
     * Get collection of {@link Partition Partitions} associated with reader reading with this
     * limiter
     *
     * @return list of associated partitions
     */
    Collection<Partition> getConsumedPartitions();

    /**
     * Retrieve current reader's watermark (minimum of all partitions).
     *
     * @return minimal watermark within partitions
     */
    long getMinWatermark();
  }

  /**
   * Setup the limiter with given configuration.
   *
   * @param cfg configuration (scoped to a (operator) defined prefix)
   */
  default void setup(Map<String, Object> cfg) {
    // nop
  }

  /**
   * Retrieve the amount of time the source should pause processing for. If the reader should
   * proceed without pausing return {@link Duration#ZERO}. Note that this method is called for each
   * input element and {@code must} be therefore cheap.
   *
   * @param context context for the limiter
   * @return the amount of time to pause the source for.
   */
  Duration getPauseTime(Context context);

  @Override
  void close();
}
