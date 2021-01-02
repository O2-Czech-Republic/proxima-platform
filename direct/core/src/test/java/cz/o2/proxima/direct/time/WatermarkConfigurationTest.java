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
import static org.junit.Assert.*;

import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class WatermarkConfigurationTest {

  private Map<String, Object> cfg;
  private CustomWatermarkConfiguration watermarkConfiguration;

  @Before
  public void setup() {
    cfg = new HashMap<>();
    watermarkConfiguration = new CustomWatermarkConfiguration(cfg);
  }

  @Test
  public void testConfigKey() {
    assertEquals("watermark.idle-policy-factory", prefixedKey("idle-policy-factory"));
    assertEquals("watermark.idle-policy-factory", prefixedKey("watermark.idle-policy-factory"));
  }

  @Test
  public void testConfigure() {
    cfg.put("watermark.idle-policy-factory", CustomIdlePolicyFactory.class.getName());
    cfg.put("watermark.estimator-factory", CustomWatermarkEstimatorFactory.class.getName());

    watermarkConfiguration = new CustomWatermarkConfiguration(cfg);

    assertEquals(
        CustomIdlePolicyFactory.class,
        watermarkConfiguration.getWatermarkIdlePolicyFactory().getClass());
    assertEquals(
        CustomWatermarkEstimatorFactory.class,
        watermarkConfiguration.getWatermarkEstimatorFactory().getClass());
  }

  @Test
  public void testConfigureDefault() {
    watermarkConfiguration = new CustomWatermarkConfiguration(cfg);

    assertEquals(
        CustomWatermarkConfiguration.IdlePolicy.class,
        watermarkConfiguration.getWatermarkIdlePolicyFactory().getClass());
    assertEquals(
        CustomWatermarkConfiguration.Estimator.class,
        watermarkConfiguration.getWatermarkEstimatorFactory().getClass());
  }

  static class CustomWatermarkConfiguration extends WatermarkConfiguration {

    public CustomWatermarkConfiguration(Map<String, Object> cfg) {
      super(cfg);
      configure();
    }

    @Override
    protected WatermarkIdlePolicyFactory getDefaultIdlePolicyFactory() {
      return new IdlePolicy();
    }

    @Override
    protected WatermarkEstimatorFactory getDefaultEstimatorFactory() {
      return new Estimator();
    }

    public static class IdlePolicy implements WatermarkIdlePolicyFactory {

      @Override
      public WatermarkIdlePolicy create(Map<String, Object> cfg) {
        return null;
      }
    }

    public static class Estimator implements WatermarkEstimatorFactory {

      @Override
      public WatermarkEstimator create(
          Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {
        return null;
      }
    }
  }

  public static class CustomIdlePolicyFactory implements WatermarkIdlePolicyFactory {

    @Override
    public WatermarkIdlePolicy create(Map<String, Object> cfg) {
      return null;
    }
  }

  public static class CustomWatermarkEstimatorFactory implements WatermarkEstimatorFactory {

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {
      return null;
    }
  }
}
