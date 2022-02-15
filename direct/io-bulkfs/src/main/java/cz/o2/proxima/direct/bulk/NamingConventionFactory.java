/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/** Factory for creating convention for naming files. */
public interface NamingConventionFactory extends Serializable {
  /**
   * Create {@link cz.o2.proxima.direct.bulk.NamingConvention} that is used with {@link
   * cz.o2.proxima.direct.bulk.FileFormat}.
   *
   * @param cfgPrefix configuration prefix
   * @param cfg configuration
   * @param rollTimePeriod time rolling interval
   * @param prefix prefix of all names generated
   * @param suffix suffix of filenames
   * @return naming convention with given settings
   */
  NamingConvention create(
      String cfgPrefix,
      Map<String, Object> cfg,
      Duration rollTimePeriod,
      String prefix,
      String suffix);
}
