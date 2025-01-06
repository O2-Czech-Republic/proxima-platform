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
package cz.o2.proxima.direct.core.time;

import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.WatermarkEstimatorFactory;
import cz.o2.proxima.core.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.core.time.Watermarks;
import java.util.Map;

/** Estimates watermark as processing time. */
public class ProcessingTimeWatermarkEstimator implements WatermarkEstimator {
  private static final long serialVersionUID = 1L;
  private final TimestampSupplier timestampSupplier;
  private long minWatermark;

  ProcessingTimeWatermarkEstimator(long minWatermark, TimestampSupplier timestampSupplier) {
    this.minWatermark = minWatermark;
    this.timestampSupplier = timestampSupplier;
  }

  /**
   * Creates an instance of {@link cz.o2.proxima.direct.core.time.ProcessingTimeWatermarkEstimator}.
   */
  public static class Factory implements WatermarkEstimatorFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public void setup(Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {}

    @Override
    public WatermarkEstimator create() {
      return ProcessingTimeWatermarkEstimator.newBuilder().build();
    }
  }

  /** Builder of the {@link ProcessingTimeWatermarkEstimator}. */
  public static class Builder {
    private final long minWatermark;
    private final TimestampSupplier timestampSupplier;

    Builder() {
      this(Watermarks.MIN_WATERMARK, System::currentTimeMillis);
    }

    private Builder(long minWatermark, TimestampSupplier timestampSupplier) {
      this.minWatermark = minWatermark;
      this.timestampSupplier = timestampSupplier;
    }

    public ProcessingTimeWatermarkEstimator.Builder withMinWatermark(long minWatermark) {
      return new ProcessingTimeWatermarkEstimator.Builder(minWatermark, timestampSupplier);
    }

    public ProcessingTimeWatermarkEstimator.Builder withTimestampSupplier(
        TimestampSupplier timestampSupplier) {

      return new ProcessingTimeWatermarkEstimator.Builder(minWatermark, timestampSupplier);
    }

    public ProcessingTimeWatermarkEstimator build() {
      return new ProcessingTimeWatermarkEstimator(minWatermark, timestampSupplier);
    }
  }

  public static ProcessingTimeWatermarkEstimator.Builder newBuilder() {
    return new ProcessingTimeWatermarkEstimator.Builder();
  }

  @Override
  public long getWatermark() {
    return Math.max(timestampSupplier.get(), minWatermark);
  }

  @Override
  public void setMinWatermark(long minWatermark) {
    this.minWatermark = minWatermark;
  }
}
