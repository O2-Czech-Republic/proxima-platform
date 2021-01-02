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
package cz.o2.proxima.direct.pubsub;

import static cz.o2.proxima.direct.time.UnboundedOutOfOrdernessWatermarkEstimator.ALLOWED_TIMESTAMP_SKEW;
import static cz.o2.proxima.direct.time.UnboundedOutOfOrdernessWatermarkEstimator.ESTIMATE_DURATION_MS;
import static cz.o2.proxima.direct.time.WatermarkConfiguration.prefixedKey;

import cz.o2.proxima.direct.time.SkewedProcessingTimeIdlePolicy;
import cz.o2.proxima.direct.time.UnboundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.direct.time.WatermarkConfiguration;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.util.HashMap;
import java.util.Map;

/** Watermark configuration for PubSub */
public class PubSubWatermarkConfiguration extends WatermarkConfiguration {

  private final long defaultEstimateDuration;
  private final long defaultAllowedTimestampSkew;

  public PubSubWatermarkConfiguration(
      Map<String, Object> cfg, long defaultEstimateDuration, long defaultAllowedTimestampSkew) {
    super(cfg);
    this.defaultEstimateDuration = defaultEstimateDuration;
    this.defaultAllowedTimestampSkew = defaultAllowedTimestampSkew;
    configure();
  }

  @Override
  protected WatermarkIdlePolicyFactory getDefaultIdlePolicyFactory() {
    return new SkewedProcessingTimeIdlePolicy.Factory();
  }

  @Override
  protected WatermarkEstimatorFactory getDefaultEstimatorFactory() {
    return new PubSubWatermarkEstimatorFactory(
        defaultEstimateDuration, defaultAllowedTimestampSkew);
  }

  static class PubSubWatermarkEstimatorFactory implements WatermarkEstimatorFactory {

    private static final String CFG_WATERMARK_ESTIMATE_DURATION =
        "pubsub.watermark.estimate-duration";
    private static final String CFG_ALLOWED_TIMESTAMP_SKEW =
        "pubsub.watermark.allowed-timestamp-skew";

    private final long defaultEstimateDuration;
    private final long defaultAllowedTimestampSkew;

    PubSubWatermarkEstimatorFactory(
        long defaultEstimateDuration, long defaultAllowedTimestampSkew) {
      this.defaultEstimateDuration = defaultEstimateDuration;
      this.defaultAllowedTimestampSkew = defaultAllowedTimestampSkew;
    }

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {
      // Preserves backward compatible behaviour by adding default values to config.
      HashMap<String, Object> newConfig = new HashMap<>(cfg);

      if (cfg.containsKey(CFG_WATERMARK_ESTIMATE_DURATION)) {
        newConfig.putIfAbsent(
            prefixedKey(ESTIMATE_DURATION_MS), cfg.get(CFG_WATERMARK_ESTIMATE_DURATION));
      }

      if (cfg.containsKey(CFG_ALLOWED_TIMESTAMP_SKEW)) {
        newConfig.putIfAbsent(
            prefixedKey(ALLOWED_TIMESTAMP_SKEW), cfg.get(CFG_ALLOWED_TIMESTAMP_SKEW));
      }

      newConfig.putIfAbsent(prefixedKey(ESTIMATE_DURATION_MS), defaultEstimateDuration);
      newConfig.putIfAbsent(prefixedKey(ALLOWED_TIMESTAMP_SKEW), defaultAllowedTimestampSkew);

      return new UnboundedOutOfOrdernessWatermarkEstimator.Factory()
          .create(newConfig, idlePolicyFactory);
    }
  }
}
