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
package cz.o2.proxima.time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Estimator of watermark based on timestamps of flowing elements. The estimator tries to estimate
 * when it reaches near-realtime consumption of events and holds the watermark until then.
 */
@Slf4j
@Internal
public class WatermarkEstimator implements WatermarkSupplier {

  @VisibleForTesting static final long MIN_WATERMARK = Long.MIN_VALUE + 365 * 86400000L;

  @Internal
  @FunctionalInterface
  public interface TimestampSupplier extends Serializable {
    long get();
  }

  /** Builder of the {@link WatermarkEstimator}. */
  public static class Builder {

    private final long durationMs;
    private final long stepMs;
    private final long allowedTimestampSkew;
    private final long minWatermark;
    private final TimestampSupplier timestampSupplier;

    Builder() {
      this(10000, 200, 200, MIN_WATERMARK, System::currentTimeMillis);
    }

    private Builder(
        long durationMs,
        long stepMs,
        long allowedTimestampSkew,
        long minWatermark,
        TimestampSupplier timestampSupplier) {

      this.durationMs = durationMs;
      this.stepMs = stepMs;
      this.allowedTimestampSkew = allowedTimestampSkew;
      this.minWatermark = minWatermark;
      this.timestampSupplier = timestampSupplier;
    }

    public Builder withDurationMs(long durationMs) {
      return new Builder(durationMs, stepMs, allowedTimestampSkew, minWatermark, timestampSupplier);
    }

    public Builder withStepMs(long stepMs) {
      return new Builder(durationMs, stepMs, allowedTimestampSkew, minWatermark, timestampSupplier);
    }

    public Builder withAllowedTimestampSkew(long allowedTimestampSkew) {
      return new Builder(durationMs, stepMs, allowedTimestampSkew, minWatermark, timestampSupplier);
    }

    public Builder withMinWatermark(long minWatermark) {
      return new Builder(durationMs, stepMs, allowedTimestampSkew, minWatermark, timestampSupplier);
    }

    public Builder withTimestampSupplier(TimestampSupplier timestampSupplier) {
      return new Builder(durationMs, stepMs, allowedTimestampSkew, minWatermark, timestampSupplier);
    }

    public WatermarkEstimator build() {
      return new WatermarkEstimator(
          durationMs, stepMs, allowedTimestampSkew, minWatermark, timestampSupplier);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final long stepMs;
  private final TimestampSupplier timestampSupplier;
  private final long[] stepDiffs;
  private final long allowedTimestampSkew;
  private final AtomicLong lastRotate;
  private final AtomicInteger rotatesToInitialize;
  private final AtomicLong watermark;
  private final AtomicLong lastStatLogged = new AtomicLong();

  @VisibleForTesting
  WatermarkEstimator(
      long durationMs,
      long stepMs,
      long allowedTimestampSkew,
      long minWatermark,
      TimestampSupplier supplier) {

    this.stepMs = stepMs;
    this.allowedTimestampSkew = allowedTimestampSkew;
    this.timestampSupplier = Objects.requireNonNull(supplier);
    this.watermark = new AtomicLong(minWatermark);
    Preconditions.checkArgument(durationMs > 0, "durationMs must be positive");
    Preconditions.checkArgument(stepMs > 0, "stepMs must be positive");
    Preconditions.checkArgument(
        durationMs / stepMs * stepMs == durationMs, "durationMs must be divisible by stepMs");
    stepDiffs = new long[(int) (durationMs / stepMs) + 1];
    for (int i = 0; i < stepDiffs.length; i++) {
      stepDiffs[i] = 0;
    }
    rotatesToInitialize = new AtomicInteger(stepDiffs.length - 1);
    lastRotate = new AtomicLong(supplier.get() - stepMs);
  }

  /**
   * Accumulate given timestamp at current processing time.
   *
   * @param stamp the stamp to accumulate
   */
  public void add(long stamp) {
    rotateIfNeeded();
    long diff = timestampSupplier.get() - stamp;
    if (stepDiffs[0] < diff) {
      stepDiffs[0] = diff;
    }
  }

  /**
   * Retrieve watermark estimate.
   *
   * @return the watermark estimate
   */
  @Override
  public long getWatermark() {
    rotateIfNeeded();
    if (rotatesToInitialize.get() <= 0) {
      boolean isProcessingBacklog =
          Arrays.stream(stepDiffs).anyMatch(diff -> diff > allowedTimestampSkew);
      if (!isProcessingBacklog) {
        watermark.accumulateAndGet(timestampSupplier.get() - allowedTimestampSkew, Math::max);
      }
    }
    return watermark.get();
  }

  private void rotateIfNeeded() {
    long now = timestampSupplier.get();
    if (now > lastRotate.get() + stepMs) {
      rotate(now, (int) ((now - lastRotate.get()) / stepMs));
    }
    if (log.isDebugEnabled() && now - lastStatLogged.get() > 10000) {
      log.debug(
          "Watermark delay stats: {} with allowedTimestampSkew {}",
          Arrays.toString(stepDiffs),
          allowedTimestampSkew);
      lastStatLogged.set(now);
    }
  }

  private void rotate(long now, int moveCount) {
    moveCount = Math.min(stepDiffs.length - 1, moveCount);
    System.arraycopy(stepDiffs, 0, stepDiffs, moveCount, stepDiffs.length - moveCount);
    if (rotatesToInitialize.get() > 0) {
      rotatesToInitialize.addAndGet(-moveCount);
    }
    for (int i = 0; i < moveCount; i++) {
      stepDiffs[i] = 0;
    }
    lastRotate.set(now);
  }
}
