/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.bulk;

import java.time.Duration;
import java.util.Map;

/** Factory creates default convention for naming files. */
public class DefaultNamingConventionFactory implements NamingConventionFactory {

  @Override
  public NamingConvention create(
      String cfgPrefix,
      Map<String, Object> cfg,
      Duration rollTimePeriod,
      String prefix,
      String suffix) {
    return NamingConvention.defaultConvention(rollTimePeriod, prefix, suffix);
  }
}
