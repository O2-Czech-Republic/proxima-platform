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
package cz.o2.proxima.direct.time;

import static cz.o2.proxima.direct.time.WatermarkConfiguration.prefixedKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.AbstractWatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Estimator of watermark based on timestamps of flowing elements. The estimator tries to estimate
 * when it reaches near-realtime consumption of events and holds the watermark until then.
 */
@Slf4j
@Internal
public class UnboundedOutOfOrdernessWatermarkEstimator extends AbstractWatermarkEstimator {
  private static final long serialVersionUID = 1L;
  static final long DEFAULT_MIN_WATERMARK = Long.MIN_VALUE + 365 * 86400000L;
  public static final String MIN_WATERMARK = "min-watermark";
  public static final String ESTIMATE_DURATION_MS = "estimate-duration";
  public static final String STEP_MS = "step";
  public static final String ALLOWED_TIMESTAMP_SKEW = "allowed-timestamp-skew";
  public static final long DEFAULT_ESTIMATE_DURATION_MS = 10000;
  public static final long DEFAULT_STEP_MS = 200;
  public static final long DEFAULT_ALLOWED_TIMESTAMP_SKEW = 200;

  public static class Factory implements WatermarkEstimatorFactory {

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {

      long durationMs = getConfiguration(ESTIMATE_DURATION_MS, cfg, DEFAULT_ESTIMATE_DURATION_MS);
      long stepMs = getConfiguration(STEP_MS, cfg, DEFAULT_STEP_MS);
      long allowedTimestampSkew =
          getConfiguration(ALLOWED_TIMESTAMP_SKEW, cfg, DEFAULT_ALLOWED_TIMESTAMP_SKEW);
      long minWatermark = getConfiguration(MIN_WATERMARK, cfg, DEFAULT_MIN_WATERMARK);

      return UnboundedOutOfOrdernessWatermarkEstimator.newBuilder()
          .withAllowedTimestampSkew(allowedTimestampSkew)
          .withDurationMs(durationMs)
          .withStepMs(stepMs)
          .withMinWatermark(minWatermark)
          .withWatermarkIdlePolicy(idlePolicyFactory.create(cfg))
          .build();
    }

    private long getConfiguration(
        String configuration, Map<String, Object> cfg, long defaultValue) {
      return Optional.ofNullable(cfg.get(prefixedKey(configuration)))
          .map(v -> Long.valueOf(v.toString()))
          .orElse(defaultValue);
    }
  }

  /** Builder of the {@link UnboundedOutOfOrdernessWatermarkEstimator}. */
  public static class Builder {

    private final long durationMs;
    private final long stepMs;
    private final long allowedTimestampSkew;
    private final long minWatermark;
    private final TimestampSupplier timestampSupplier;
    private final WatermarkIdlePolicy watermarkIdlePolicy;

    Builder() {
      this(
          DEFAULT_ESTIMATE_DURATION_MS,
          DEFAULT_STEP_MS,
          DEFAULT_ALLOWED_TIMESTAMP_SKEW,
          DEFAULT_MIN_WATERMARK,
          System::currentTimeMillis,
          new NotProgressingWatermarkIdlePolicy());
    }

    private Builder(
        long durationMs,
        long stepMs,
        long allowedTimestampSkew,
        long minWatermark,
        TimestampSupplier timestampSupplier,
        WatermarkIdlePolicy idlePolicy) {

      this.durationMs = durationMs;
      this.stepMs = stepMs;
      this.allowedTimestampSkew = allowedTimestampSkew;
      this.minWatermark = minWatermark;
      this.timestampSupplier = timestampSupplier;
      this.watermarkIdlePolicy = idlePolicy;
    }

    public Builder withDurationMs(long durationMs) {
      return new Builder(
          durationMs,
          stepMs,
          allowedTimestampSkew,
          minWatermark,
          timestampSupplier,
          watermarkIdlePolicy);
    }

    public Builder withStepMs(long stepMs) {
      return new Builder(
          durationMs,
          stepMs,
          allowedTimestampSkew,
          minWatermark,
          timestampSupplier,
          watermarkIdlePolicy);
    }

    public Builder withAllowedTimestampSkew(long allowedTimestampSkew) {
      return new Builder(
          durationMs,
          stepMs,
          allowedTimestampSkew,
          minWatermark,
          timestampSupplier,
          watermarkIdlePolicy);
    }

    public Builder withMinWatermark(long minWatermark) {
      return new Builder(
          durationMs,
          stepMs,
          allowedTimestampSkew,
          minWatermark,
          timestampSupplier,
          watermarkIdlePolicy);
    }

    public Builder withTimestampSupplier(TimestampSupplier timestampSupplier) {
      return new Builder(
          durationMs,
          stepMs,
          allowedTimestampSkew,
          minWatermark,
          timestampSupplier,
          watermarkIdlePolicy);
    }

    public Builder withWatermarkIdlePolicy(WatermarkIdlePolicy watermarkIdlePolicy) {
      return new Builder(
          durationMs,
          stepMs,
          allowedTimestampSkew,
          minWatermark,
          timestampSupplier,
          watermarkIdlePolicy);
    }

    public UnboundedOutOfOrdernessWatermarkEstimator build() {
      return new UnboundedOutOfOrdernessWatermarkEstimator(
          durationMs,
          stepMs,
          allowedTimestampSkew,
          minWatermark,
          timestampSupplier,
          watermarkIdlePolicy);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Getter private final long stepMs;
  @Getter private final long estimateDurationMs;
  @Getter private final long allowedTimestampSkew;

  private final TimestampSupplier timestampSupplier;
  private final long[] stepDiffs;
  private final AtomicLong lastRotate;
  private final AtomicInteger rotatesToInitialize;
  private final AtomicLong watermark;
  private final AtomicLong lastStatLogged = new AtomicLong();

  @VisibleForTesting
  UnboundedOutOfOrdernessWatermarkEstimator(
      long estimateDurationMs,
      long stepMs,
      long allowedTimestampSkew,
      long minWatermark,
      TimestampSupplier supplier,
      WatermarkIdlePolicy idlePolicy) {
    super(idlePolicy);
    this.estimateDurationMs = estimateDurationMs;
    this.stepMs = stepMs;
    this.allowedTimestampSkew = allowedTimestampSkew;
    this.timestampSupplier = Objects.requireNonNull(supplier);
    this.watermark = new AtomicLong(minWatermark);
    Preconditions.checkArgument(estimateDurationMs > 0, "durationMs must be positive");
    Preconditions.checkArgument(stepMs > 0, "stepMs must be positive");
    Preconditions.checkArgument(
        estimateDurationMs / stepMs * stepMs == estimateDurationMs,
        "durationMs must be divisible by stepMs");
    stepDiffs = new long[(int) (estimateDurationMs / stepMs) + 1];
    Arrays.fill(stepDiffs, 0);
    rotatesToInitialize = new AtomicInteger(stepDiffs.length - 1);
    lastRotate = new AtomicLong(supplier.get() - stepMs);
  }

  @Override
  protected long estimateWatermark() {
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

  @Override
  public void updateWatermark(StreamElement element) {
    add(element.getStamp());
  }

  @Override
  public void setMinWatermark(long minWatermark) {
    watermark.accumulateAndGet(minWatermark, Math::max);
  }

  /**
   * Accumulate given timestamp at current processing time.
   *
   * @param stamp the stamp to accumulate
   */
  @VisibleForTesting
  void add(long stamp) {
    rotateIfNeeded();
    long diff = timestampSupplier.get() - stamp;
    if (stepDiffs[0] < diff) {
      stepDiffs[0] = diff;
    }
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
