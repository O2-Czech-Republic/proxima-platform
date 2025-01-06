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
package cz.o2.proxima.direct.io.kafka;

import cz.o2.proxima.core.time.WatermarkEstimatorFactory;
import cz.o2.proxima.core.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.direct.core.time.BoundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.direct.core.time.SkewedProcessingTimeIdlePolicy;
import cz.o2.proxima.direct.core.time.WatermarkConfiguration;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import java.util.Map;

public class KafkaWatermarkConfiguration extends WatermarkConfiguration {

  public KafkaWatermarkConfiguration(Map<String, Object> cfg) {
    super(cfg);
    configure();
  }

  @Override
  protected WatermarkIdlePolicyFactory getDefaultIdlePolicyFactory() {
    return new SkewedProcessingTimeIdlePolicy.Factory();
  }

  @Override
  protected WatermarkEstimatorFactory getDefaultEstimatorFactory() {
    return new BoundedOutOfOrdernessWatermarkEstimator.Factory();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("defaultIdlePolicyFactory", getDefaultIdlePolicyFactory())
        .add("defaultEstimatorFactory", getDefaultEstimatorFactory())
        .add("currentIdlePolicyFactory", getWatermarkIdlePolicyFactory())
        .add("currentEstimatorFactory", getWatermarkEstimatorFactory())
        .toString();
  }
}
