/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.repository.DataOperator;
import cz.o2.proxima.core.repository.DataOperatorFactory;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.Serializable;

@Internal
public interface DataOperatorAware extends Serializable {

  /**
   * Verify if this is {@link DataOperator} specific (contextual) object.
   *
   * @return boolean
   */
  default boolean isContextual() {
    return true;
  }

  /**
   * Verify if this Transformation belongs to given DataOperatorFactory
   *
   * @param operatorFactory the DataOperatorFactory
   * @return boolean
   */
  boolean isDelegateOf(DataOperatorFactory<?> operatorFactory);

  /**
   * Convert this transformation to a more ({@link DataOperator}) specific one.
   *
   * @param cls the class to check and convert this transform
   * @param <T> type parameter
   * @return cast transform
   */
  @SuppressWarnings("unchecked")
  default <T extends DataOperatorAware> T as(Class<T> cls) {
    Preconditions.checkArgument(
        cls.isAssignableFrom(getClass()),
        "Class %s is not %s",
        getClass().getName(),
        cls.getName());
    return (T) this;
  }
}
