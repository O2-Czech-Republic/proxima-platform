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
package cz.o2.proxima.direct.core.transform;

import cz.o2.proxima.core.annotations.Experimental;
import cz.o2.proxima.core.repository.DataOperatorFactory;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transform.ContextualTransformation;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.DirectDataOperatorFactory;
import java.util.Map;

/** An element-wise transformation runnable in the context of {@link DirectDataOperator} only. */
@Experimental
public interface DirectElementWiseTransform
    extends ContextualTransformation<DirectDataOperator>, AutoCloseable {

  @Override
  default boolean isDelegateOf(DataOperatorFactory<?> operatorFactory) {
    return operatorFactory instanceof DirectDataOperatorFactory;
  }

  @Override
  void setup(Repository repo, DirectDataOperator directDataOperator, Map<String, Object> cfg);

  /**
   * Transform and output any transformed elements. The complete processing is defined by this
   * method.
   *
   * @param input the input {@link StreamElement}
   * @param commit asynchronous callback notifying about the result
   */
  void transform(StreamElement input, CommitCallback commit);
}
