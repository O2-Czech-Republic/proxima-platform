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
package cz.o2.proxima.core.transform;

import cz.o2.proxima.core.annotations.Evolving;
import cz.o2.proxima.core.repository.DataOperator;
import cz.o2.proxima.core.repository.Repository;
import java.util.Map;

@Evolving
public interface ContextualTransformation<OP extends DataOperator>
    extends Transformation, DataOperatorAware {

  @Override
  default boolean isContextual() {
    return true;
  }

  /**
   * Read the repository and setup descriptors of target entity and attributes.
   *
   * @param repo the repository
   * @param op {@link DataOperator} that delegated this Transformation
   * @param cfg transformation config map
   */
  void setup(Repository repo, OP op, Map<String, Object> cfg);

  /**
   * Called before starting processing after fresh start of processing or after recovery from error
   */
  default void onRestart() {
    // nop
  }
}
