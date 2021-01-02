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

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.AbstractWatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

/**
 * This estimators generates watermarks assuming that elements arrive out of order, but only to a
 * certain degree defined by configuration: watermark.max-out-of-orderness.
 */
public class BoundedOutOfOrdernessWatermarkEstimator extends AbstractWatermarkEstimator {
  private static final long serialVersionUID = 1L;

  public static final String MAX_OUT_OF_ORDERNESS_MS = "max-out-of-orderness";
  public static final long DEFAULT_MAX_OUT_OF_ORDERNESS_MS = 0L;

  @Getter private final long maxOutOfOrderness;
  @Getter private long minWatermark;
  private Long maxTimestamp;

  private BoundedOutOfOrdernessWatermarkEstimator(
      long maxOutOfOrderness, long minWatermark, WatermarkIdlePolicy idlePolicy) {
    super(idlePolicy);
    this.maxOutOfOrderness = maxOutOfOrderness;
    this.minWatermark = minWatermark;
  }

  public static BoundedOutOfOrdernessWatermarkEstimator.Builder newBuilder() {
    return new BoundedOutOfOrdernessWatermarkEstimator.Builder();
  }

  @Override
  protected long estimateWatermark() {
    if (maxTimestamp != null) {
      return Math.max(minWatermark, maxTimestamp - maxOutOfOrderness);
    }
    return minWatermark;
  }

  @Override
  public void updateWatermark(StreamElement element) {
    if (maxTimestamp != null) {
      maxTimestamp = Math.max(element.getStamp(), maxTimestamp);
    } else {
      maxTimestamp = element.getStamp();
    }
  }

  @Override
  public void setMinWatermark(long minWatermark) {
    this.minWatermark = minWatermark;
  }

  /**
   * Creates an instance of {@link
   * cz.o2.proxima.direct.time.BoundedOutOfOrdernessWatermarkEstimator}.
   */
  public static class Factory implements WatermarkEstimatorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {
      final long maxOutOfOrderness =
          Optional.ofNullable(cfg.get(prefixedKey(MAX_OUT_OF_ORDERNESS_MS)))
              .map(v -> Long.valueOf(v.toString()))
              .orElse(DEFAULT_MAX_OUT_OF_ORDERNESS_MS);

      return BoundedOutOfOrdernessWatermarkEstimator.newBuilder()
          .withMaxOutOfOrderness(maxOutOfOrderness)
          .withWatermarkIdlePolicy(idlePolicyFactory.create(cfg))
          .build();
    }
  }

  /** Builder of the {@link cz.o2.proxima.direct.time.BoundedOutOfOrdernessWatermarkEstimator}. */
  public static class Builder {
    private final long maxOutOfOrderness;
    private final long minWatermark;
    private final WatermarkIdlePolicy watermarkIdlePolicy;

    Builder() {
      this(
          DEFAULT_MAX_OUT_OF_ORDERNESS_MS,
          Watermarks.MIN_WATERMARK,
          new NotProgressingWatermarkIdlePolicy());
    }

    private Builder(long maxOutOfOrderness, long minWatermark, WatermarkIdlePolicy idlePolicy) {
      this.maxOutOfOrderness = maxOutOfOrderness;
      this.minWatermark = minWatermark;
      this.watermarkIdlePolicy = idlePolicy;
    }

    public BoundedOutOfOrdernessWatermarkEstimator.Builder withMaxOutOfOrderness(
        long maxOutOfOrderness) {
      return new BoundedOutOfOrdernessWatermarkEstimator.Builder(
          maxOutOfOrderness, minWatermark, watermarkIdlePolicy);
    }

    public BoundedOutOfOrdernessWatermarkEstimator.Builder withMinWatermark(long minWatermark) {
      return new BoundedOutOfOrdernessWatermarkEstimator.Builder(
          maxOutOfOrderness, minWatermark, watermarkIdlePolicy);
    }

    public BoundedOutOfOrdernessWatermarkEstimator.Builder withWatermarkIdlePolicy(
        WatermarkIdlePolicy watermarkIdlePolicy) {
      return new BoundedOutOfOrdernessWatermarkEstimator.Builder(
          maxOutOfOrderness, minWatermark, watermarkIdlePolicy);
    }

    public BoundedOutOfOrdernessWatermarkEstimator build() {
      return new BoundedOutOfOrdernessWatermarkEstimator(
          maxOutOfOrderness, minWatermark, watermarkIdlePolicy);
    }
  }
}
