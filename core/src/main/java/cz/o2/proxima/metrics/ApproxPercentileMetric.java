/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.metrics;

import com.google.common.base.Preconditions;
import com.tdunning.math.stats.TDigest;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.util.Pair;
import java.util.Arrays;

/** An approximation of 1st, 10th, 30th, 50th, 70th, 90th and 99th percentile. */
@Stable
public class ApproxPercentileMetric extends Metric<Stats> implements ApproxPercentileMetricMXBean {

  private static final long serialVersionUID = 1L;

  /**
   * Construct the metric.
   *
   * @param group group name
   * @param name metric name
   * @param duration total duration of the statistic in ms
   * @param window windowNs size in ms
   * @return the metric
   */
  public static ApproxPercentileMetric of(String group, String name, long duration, long window) {

    return new ApproxPercentileMetric(group, name, duration, window);
  }

  Pair<TDigest, Long>[] digests;
  final long windowNs;
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
    this.windowNs = window * 1_000_000L;
    _reset();
  }

  private TDigest createDigest() {
    return TDigest.createMergingDigest(100.0);
  }

  @Override
  public synchronized void increment(double d) {
    if (System.nanoTime() - digests[current].getSecond() > windowNs) {
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
    if (current + 1 < maxDigests) {
      digests[++current] = Pair.of(createDigest(), System.nanoTime());
    } else {
      // move the array
      System.arraycopy(digests, 1, digests, 0, maxDigests - 1);
      digests[current] = Pair.of(createDigest(), System.nanoTime());
    }
  }

  @SuppressWarnings("unchecked")
  private synchronized void _reset() {
    this.digests = new Pair[maxDigests];
    this.digests[0] = Pair.of(createDigest(), System.nanoTime());
    this.current = 0;
  }
}
