/**
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
package cz.o2.proxima.functional;

import cz.o2.proxima.annotations.Stable;
import java.io.Serializable;

/**
 * A factory function.
 *
 * @param <T> type of returned object
 */
@Stable
@FunctionalInterface
public interface Factory<T> extends Serializable {

  /**
   * Apply the factory function.
   *
   * @return the created object
   */
  T apply();
}
