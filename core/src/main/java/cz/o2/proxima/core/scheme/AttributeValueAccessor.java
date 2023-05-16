/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.scheme;

import cz.o2.proxima.core.annotations.Experimental;
import java.io.Serializable;

/**
 * Interface for value accessors allowed create and get value of attribute
 *
 * @param <InputT> input type
 * @param <OutputT> output type
 */
@Experimental
public interface AttributeValueAccessor<InputT, OutputT> extends Serializable {
  /** Accessor type */
  enum Type {
    PRIMITIVE,
    STRUCTURE,
    ARRAY,
    ENUM
  }

  /** Get accessor type */
  Type getType();

  /**
   * Get value of attribute.
   *
   * @param object input object
   * @return specific value of attribute
   */
  OutputT valueOf(InputT object);

  /**
   * Create value from input object.
   *
   * @param object input object
   * @return attribute value
   */
  InputT createFrom(OutputT object);
}
