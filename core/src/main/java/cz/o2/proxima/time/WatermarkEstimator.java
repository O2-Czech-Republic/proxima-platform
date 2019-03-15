/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import java.io.Serializable;
import java.util.Objects;

/**
 * Estimator of watermark based on timestamps of flowing elements.
 */
@Internal
public class WatermarkEstimator implements WatermarkSupplier {

  @FunctionalInterface
  interface TimestampSupplier extends Serializable {
    long get();
  }

  /**
   * Create estimator of watermark with given parameters.
   * @param durationMs duration of the window for aggregation of timestamps
   * @param stepMs step (window slide)
   * @return the estimator
   */
  public static WatermarkEstimator of(long durationMs, long stepMs) {
    return new WatermarkEstimator(durationMs, stepMs, System::currentTimeMillis);
  }

  final long durationMs;
  final long stepMs;
  final TimestampSupplier timestampSupplier;
  final long[] steps;
  long lastRotate;
  int rotatesToInitialize;

  @VisibleForTesting
  WatermarkEstimator(long durationMs, long stepMs, TimestampSupplier supplier) {
    this.durationMs = durationMs;
    this.stepMs = stepMs;
    this.timestampSupplier = Objects.requireNonNull(supplier);
    Preconditions.checkArgument(durationMs > 0, "durationMs must be positive");
    Preconditions.checkArgument(stepMs > 0, "stepMs must be positive");
    Preconditions.checkArgument(
        durationMs / stepMs * stepMs == durationMs,
        "durationMs must be divisible by stepMs");
    steps = new long[(int) (durationMs / stepMs) + 1];
    for (int i = 0; i < steps.length; i++) {
      steps[i] = Long.MAX_VALUE;
    }
    rotatesToInitialize = steps.length - 1;
    lastRotate = supplier.get() - stepMs;
  }

  /**
   * Accumulate given timestamp at current processing time.
   * @param stamp the stamp to accumulate
   */
  public void add(long stamp) {
    long now = timestampSupplier.get();
    if (now - lastRotate >= stepMs) {
      rotate(now);
    }
    if (steps[0] > stamp) {
      steps[0] = stamp;
    }
  }

  private void rotate(long now) {
    System.arraycopy(steps, 0, steps, 1, steps.length - 1);
    if (rotatesToInitialize > 0) {
      rotatesToInitialize--;
    }
    steps[0] = Long.MAX_VALUE;
    lastRotate = now;
  }

  /**
   * Retrieve watermark estimate.
   * @return the watermark estimate
   */
  @Override
  public long getWatermark() {
    if (rotatesToInitialize > 0) {
      return Long.MIN_VALUE;
    }
    long ret = Long.MAX_VALUE;
    for (int pos = 0; pos < steps.length - 1; pos++) {
      if (steps[pos] < ret) {
        ret = steps[pos];
      }
    }
    return ret;
  }

}
