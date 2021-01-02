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
package cz.o2.proxima.transform;

import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.Map;

/**
 * A stateless element-wise transformation applied on incoming data converting single {@code
 * StreamElement} to another {@code StreamElement}(s).
 */
@Evolving
public interface ElementWiseTransformation extends Transformation, DataOperatorAgnostic {

  /** Collector for outputs. */
  @FunctionalInterface
  interface Collector<T> extends Serializable {

    /**
     * Collect transformed value.
     *
     * @param value the value to collect
     */
    void collect(T value);
  }

  /**
   * Setup this transform.
   *
   * @param repo repository
   * @param cfg configuration
   */
  void setup(Repository repo, Map<String, Object> cfg);

  /**
   * Apply the transformation function.
   *
   * @param input the input stream element to transform
   * @param collector collector for outputs
   * @return how many invocations of collector to expect before the elements should be considered
   *     processed
   */
  int apply(StreamElement input, Collector<StreamElement> collector);
}
