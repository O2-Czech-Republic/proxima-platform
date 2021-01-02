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

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.AbstractWatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import java.util.Map;

/** Estimates watermark as processing time. */
public class ProcessingTimeWatermarkEstimator extends AbstractWatermarkEstimator {
  private static final long serialVersionUID = 1L;
  private final TimestampSupplier timestampSupplier;
  private long minWatermark;

  ProcessingTimeWatermarkEstimator(
      long minWatermark, TimestampSupplier timestampSupplier, WatermarkIdlePolicy idlePolicy) {
    super(idlePolicy);
    this.minWatermark = minWatermark;
    this.timestampSupplier = timestampSupplier;
  }

  /** Creates an instance of {@link cz.o2.proxima.direct.time.ProcessingTimeWatermarkEstimator}. */
  public static class Factory implements WatermarkEstimatorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {
      return ProcessingTimeWatermarkEstimator.newBuilder()
          .withWatermarkIdlePolicy(idlePolicyFactory.create(cfg))
          .build();
    }
  }

  /** Builder of the {@link ProcessingTimeWatermarkEstimator}. */
  public static class Builder {
    private final long minWatermark;
    private final TimestampSupplier timestampSupplier;
    private final WatermarkIdlePolicy watermarkIdlePolicy;

    Builder() {
      this(
          Watermarks.MIN_WATERMARK,
          System::currentTimeMillis,
          new NotProgressingWatermarkIdlePolicy());
    }

    private Builder(
        long minWatermark, TimestampSupplier timestampSupplier, WatermarkIdlePolicy idlePolicy) {
      this.minWatermark = minWatermark;
      this.timestampSupplier = timestampSupplier;
      this.watermarkIdlePolicy = idlePolicy;
    }

    public ProcessingTimeWatermarkEstimator.Builder withMinWatermark(long minWatermark) {
      return new ProcessingTimeWatermarkEstimator.Builder(
          minWatermark, timestampSupplier, watermarkIdlePolicy);
    }

    public ProcessingTimeWatermarkEstimator.Builder withTimestampSupplier(
        TimestampSupplier timestampSupplier) {
      return new ProcessingTimeWatermarkEstimator.Builder(
          minWatermark, timestampSupplier, watermarkIdlePolicy);
    }

    public ProcessingTimeWatermarkEstimator.Builder withWatermarkIdlePolicy(
        WatermarkIdlePolicy watermarkIdlePolicy) {
      return new ProcessingTimeWatermarkEstimator.Builder(
          minWatermark, timestampSupplier, watermarkIdlePolicy);
    }

    public ProcessingTimeWatermarkEstimator build() {
      return new ProcessingTimeWatermarkEstimator(
          minWatermark, timestampSupplier, watermarkIdlePolicy);
    }
  }

  public static ProcessingTimeWatermarkEstimator.Builder newBuilder() {
    return new ProcessingTimeWatermarkEstimator.Builder();
  }

  @Override
  protected long estimateWatermark() {
    return Math.max(timestampSupplier.get(), minWatermark);
  }

  @Override
  protected void updateWatermark(StreamElement element) {}

  @Override
  public void setMinWatermark(long minWatermark) {
    this.minWatermark = minWatermark;
  }
}
