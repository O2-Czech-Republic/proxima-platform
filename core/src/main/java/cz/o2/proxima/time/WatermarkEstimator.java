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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Estimator of watermark based on timestamps of flowing elements.
 */
@Slf4j
@Internal
public class WatermarkEstimator implements WatermarkSupplier {

  @Internal
  @FunctionalInterface
  public interface TimestampSupplier extends Serializable {
    long get();
  }

  /**
   * Create estimator of watermark with given parameters.
   * @param durationMs duration of the window for aggregation of timestamps
   * @param stepMs step (window slide)
   * @return the estimator
   */
  public static WatermarkEstimator of(long durationMs, long stepMs) {
    return of(durationMs, stepMs, Long.MIN_VALUE);
  }

  /**
   * Create estimator of watermark with given parameters.
   * @param durationMs duration of the window for aggregation of timestamps
   * @param stepMs step (window slide)
   * @param watermark minimal watermark that has already passed at time of
   * creation of this estimator
   * @return the estimator
   */
  public static WatermarkEstimator of(long durationMs, long stepMs, long watermark) {
    return new WatermarkEstimator(durationMs, stepMs, System::currentTimeMillis);
  }

  /**
   * Create estimator of watermark with given parameters.
   * @param durationMs duration of the window for aggregation of timestamps
   * @param stepMs step (window slide)
   * @param watermark minimal watermark that has already passed at time of
   * creation of this estimator
   * @param timestampSupplier supplier of time (used in testing)
   * @return the estimator
   */
  public static WatermarkEstimator of(
      long durationMs, long stepMs, long watermark,
      TimestampSupplier timestampSupplier) {

    return new WatermarkEstimator(durationMs, stepMs, timestampSupplier);
  }


  private final long stepMs;
  private final TimestampSupplier timestampSupplier;
  private final long[] steps;
  private final AtomicLong lastRotate;
  private final AtomicInteger rotatesToInitialize;
  private final AtomicLong watermark;

  @VisibleForTesting
  WatermarkEstimator(
      long durationMs, long stepMs, TimestampSupplier supplier) {

    this(durationMs, stepMs, supplier, Long.MIN_VALUE);
  }

  WatermarkEstimator(
      long durationMs, long stepMs, TimestampSupplier supplier,
      long minWatermark) {

    this.stepMs = stepMs;
    this.timestampSupplier = Objects.requireNonNull(supplier);
    this.watermark = new AtomicLong(minWatermark);
    Preconditions.checkArgument(durationMs > 0, "durationMs must be positive");
    Preconditions.checkArgument(stepMs > 0, "stepMs must be positive");
    Preconditions.checkArgument(
        durationMs / stepMs * stepMs == durationMs,
        "durationMs must be divisible by stepMs");
    steps = new long[(int) (durationMs / stepMs) + 1];
    for (int i = 0; i < steps.length; i++) {
      steps[i] = Long.MAX_VALUE;
    }
    rotatesToInitialize = new AtomicInteger(steps.length - 1);
    lastRotate = new AtomicLong(supplier.get() - stepMs);
  }

  /**
   * Accumulate given timestamp at current processing time.
   * @param stamp the stamp to accumulate
   */
  public void add(long stamp) {
    rotateIfNeeded();
    if (steps[0] > stamp) {
      steps[0] = stamp;
    }
  }

  /**
   * Retrieve watermark estimate.
   * @return the watermark estimate
   */
  @Override
  public long getWatermark() {
    rotateIfNeeded();
    if (rotatesToInitialize.get() > 0) {
      return Long.MIN_VALUE;
    }
    long ret = Long.MAX_VALUE;
    for (int pos = 0; pos < steps.length - 1; pos++) {
      if (steps[pos] < ret) {
        ret = steps[pos];
      }
    }
    if (ret < Long.MAX_VALUE) {
      return watermark.accumulateAndGet(ret, Math::max);
    }
    return watermark.get();
  }

  private void rotateIfNeeded() {
    long now = timestampSupplier.get();
    if (now > lastRotate.get() + stepMs) {
      rotate(now, (int) ((now - lastRotate.get()) / stepMs));
    }
  }

  private void rotate(long now, int moveCount) {
    moveCount = Math.min(steps.length - 1, moveCount);
    System.arraycopy(steps, 0, steps, moveCount, steps.length - moveCount);
    if (rotatesToInitialize.get() > 0) {
      rotatesToInitialize.addAndGet(-moveCount);
    }
    for (int i = 0; i < moveCount; i++) {
      steps[i] = now;
    }
    lastRotate.set(now);
  }

}
