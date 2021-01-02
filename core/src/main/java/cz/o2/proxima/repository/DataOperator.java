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
package cz.o2.proxima.repository;

/**
 * Labeling interface for Repository implementations to be able to mark their respective operators.
 */
public interface DataOperator extends AutoCloseable {

  @Override
  void close();

  /**
   * Reload the operator after {@link Repository} has been changed.
   *
   * <p>This method is called automatically, when {@link ConfigRepository#reloadConfig} is called.
   */
  void reload();

  /**
   * Retrieve repository associated with the operator.
   *
   * @return {@link Repository} associated with the operator
   */
  Repository getRepository();
}
