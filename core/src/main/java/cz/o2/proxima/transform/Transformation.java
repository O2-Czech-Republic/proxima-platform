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
package cz.o2.proxima.transform;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.repository.DataOperator;

@Internal
public interface Transformation extends DataOperatorAware {

  /**
   * Convert this transformation to (operator specific) contextual transform.
   *
   * @param <OP> Operator
   * @return this transform converted to contextual transform.
   * @throws IllegalArgumentException on errors
   */
  @SuppressWarnings("unchecked")
  default <OP extends DataOperator> ContextualTransformation<OP> asContextualTransform() {
    return as(ContextualTransformation.class);
  }

  /**
   * Convert this transformation to element wise (stateless).
   *
   * @return this transform converted to stateless transform
   * @throws IllegalArgumentException on errors
   */
  default ElementWiseTransformation asElementWiseTransform() {
    return as(ElementWiseTransformation.class);
  }
}
