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

import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.util.Classpath;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Watermark configuration */
@Slf4j
public abstract class WatermarkConfiguration implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String CFG_PREFIX = "watermark.";
  public static final String CFG_ESTIMATOR_FACTORY = "estimator-factory";
  public static final String CFG_IDLE_POLICY_FACTORY = "idle-policy-factory";

  @Getter private final Map<String, Object> cfg;
  @Getter private WatermarkIdlePolicyFactory watermarkIdlePolicyFactory;
  @Getter private WatermarkEstimatorFactory watermarkEstimatorFactory;

  protected WatermarkConfiguration(Map<String, Object> cfg) {
    this.cfg = cfg;
  }

  /**
   * Returns configuration key with added watermark config prefix.
   *
   * @param cfgName config key name
   * @return full key name
   */
  public static String prefixedKey(String cfgName) {
    if (cfgName.startsWith(CFG_PREFIX)) {
      return cfgName;
    }
    return CFG_PREFIX + cfgName;
  }

  /**
   * Returns default idle policy factory when none user's factory is provided.
   *
   * @return {@link WatermarkIdlePolicyFactory} default idle watermark policy
   */
  protected abstract WatermarkIdlePolicyFactory getDefaultIdlePolicyFactory();

  /**
   * Returns default estimator factory when none user's factory is provided.
   *
   * @return {@link WatermarkEstimatorFactory} default watermark estimator factory.
   */
  protected abstract WatermarkEstimatorFactory getDefaultEstimatorFactory();

  protected void configure() {
    watermarkIdlePolicyFactory =
        Optional.ofNullable(cfg.get(prefixedKey(CFG_IDLE_POLICY_FACTORY)))
            .map(Object::toString)
            .map(cls -> Classpath.newInstance(cls, WatermarkIdlePolicyFactory.class))
            .orElse(getDefaultIdlePolicyFactory());

    watermarkEstimatorFactory =
        Optional.ofNullable(cfg.get(prefixedKey(CFG_ESTIMATOR_FACTORY)))
            .map(Object::toString)
            .map(cls -> Classpath.newInstance(cls, WatermarkEstimatorFactory.class))
            .orElse(getDefaultEstimatorFactory());

    log.info(
        "Configured watermark with " + "watermarkEstimatorFactory {}," + "idlePolicyFactory {}",
        watermarkEstimatorFactory.getClass(),
        watermarkIdlePolicyFactory.getClass());
  }
}
