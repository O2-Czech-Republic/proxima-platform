/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.repository;

/**
 * Factory for {@link DataOperator} with data accessing capabilities
 * provided by specific implementation.
 */
public interface DataOperatorFactory<T extends DataOperator> {

  /**
   * Retrieve symbolic name of the operator. This name can then be used
   * in call to {@link Repository#hasOperator(String)}.
   * @return name of the produced operator
   */
  String getOperatorName();


  /**
   * Check if the {@link DataOperator} produced by this factory is
   * of given class type.
   * @param cls the class type of the operator
   * @return {@code true} if this factory produces given type
   */
  boolean isOfType(Class<? extends DataOperator> cls);

  /**
   * Create the {@link DataOperator} instance.
   * @param repo repository
   * @return instance of the operator
   */
  T create(Repository repo);

}
