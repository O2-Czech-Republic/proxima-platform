/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.metrics;

import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.tdunning.math.stats.TDigest;
import java.util.Arrays;

/** An approximation of 1st, 10th, 30th, 50th, 70th, 90th and 99th percentile. */
@Stable
public class ApproxPercentileMetric extends Metric<Stats> implements ApproxPercentileMetricMXBean {

  private static final long serialVersionUID = 1L;

  Pair<TDigest, Long>[] digests;
  final long window;
  final int maxDigests;
  int current = 0;

  /**
   * Construct the metric.
   *
   * @param group group name
   * @param name metric name
   * @param duration total duration of the statistic in ms
   * @param window window size in ms
   */
  ApproxPercentileMetric(String group, String name, long duration, long window) {
    super(group, name);
    Preconditions.checkArgument(window > 0, "Window must be non-zero length");
    this.maxDigests = (int) (duration / window);
    Preconditions.checkArgument(
        maxDigests > 0, "Duration must be at least of length of the window");
    this.window = window;
    _reset();
  }

  private TDigest createDigest() {
    return TDigest.createMergingDigest(100.0);
  }

  @Override
  public synchronized void increment(double d) {
    if (System.currentTimeMillis() - digests[current].getSecond() > window) {
      addDigest();
    }
    digests[current].getFirst().add(d);
  }

  @Override
  public synchronized Stats getValue() {
    TDigest result = createDigest();
    Arrays.stream(digests, 0, current + 1).forEach(p -> result.add(p.getFirst()));
    return new Stats(
        new double[] {
          result.quantile(0.01),
          result.quantile(0.1),
          result.quantile(0.3),
          result.quantile(0.5),
          result.quantile(0.7),
          result.quantile(0.9),
          result.quantile(0.99),
        });
  }

  @Override
  public synchronized void reset() {
    _reset();
  }

  private void addDigest() {
    Pair<TDigest, Long> newDigest = createNewDigest();
    if (current + 1 < maxDigests) {
      digests[++current] = newDigest;
    } else {
      // move the array
      System.arraycopy(digests, 1, digests, 0, maxDigests - 1);
      digests[current] = newDigest;
    }
  }

  @SuppressWarnings("unchecked")
  private synchronized void _reset() {
    this.digests = new Pair[maxDigests];
    this.digests[0] = createNewDigest();
    this.current = 0;
  }

  private Pair<TDigest, Long> createNewDigest() {
    return Pair.of(createDigest(), System.currentTimeMillis());
  }
}
