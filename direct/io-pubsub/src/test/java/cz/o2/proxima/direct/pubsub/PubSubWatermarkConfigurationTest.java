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

import static java.util.Collections.emptyMap;
import static org.junit.Assert.*;

import cz.o2.proxima.direct.time.SkewedProcessingTimeIdlePolicy;
import cz.o2.proxima.direct.time.UnboundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.util.Map;
import org.junit.Test;

public class PubSubWatermarkConfigurationTest {

  private static final long ESTIMATE_DURATION = 20_000L;
  private static final long TIMESTAMP_SKEW = 10_000L;

  @Test
  public void testConfigureDefault() {
    // Check backward compatibility with legacy behaviour
    Map<String, Object> cfg = emptyMap();
    PubSubWatermarkConfiguration configuration =
        new PubSubWatermarkConfiguration(cfg, ESTIMATE_DURATION, TIMESTAMP_SKEW);
    WatermarkIdlePolicyFactory policyFactory = configuration.getWatermarkIdlePolicyFactory();
    UnboundedOutOfOrdernessWatermarkEstimator estimator =
        (UnboundedOutOfOrdernessWatermarkEstimator)
            configuration.getWatermarkEstimatorFactory().create(cfg, policyFactory);
    SkewedProcessingTimeIdlePolicy policy =
        (SkewedProcessingTimeIdlePolicy) policyFactory.create(cfg);

    assertNotNull(estimator);
    assertNotNull(policy);
    assertEquals(ESTIMATE_DURATION, estimator.getEstimateDurationMs());
    assertEquals(TIMESTAMP_SKEW, estimator.getAllowedTimestampSkew());
  }
}
