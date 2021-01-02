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
package cz.o2.proxima.generator;

import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.DataOperatorFactory;
import java.util.Set;

/**
 * Factory for {@link DataOperator} specific enhancements to generated model. These enhancements are
 * encapsulated into nested subclass of the model with name
 *
 * <pre>[moduleName]Operator</pre>
 *
 * .
 */
public interface OperatorGenerator {

  /**
   * Retrieve {@link DataOperatorFactory} associated with this generator.
   *
   * @return {@link DataOperatorFactory} associated with this generator.
   */
  DataOperatorFactory<?> operatorFactory();

  /**
   * Retrieve imports for the operator module subclass.
   *
   * @return set of imports
   */
  Set<String> imports();

  /**
   * Retrieve class definition for the subclass.
   *
   * @return the subclass definition
   */
  String classDef();

  /**
   * Retrieve name of class that represents the {@link DataOperator} implementation.
   *
   * @return the class name
   */
  String getOperatorClassName();
}
