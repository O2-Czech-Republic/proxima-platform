/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.scheme;

import java.io.Serializable;
import java.net.URI;

/**
 * Factory for {@code ValueSerializer}.
 * The serializer has a specific scheme (e.g. proto:).
 */
public interface ValueSerializerFactory<T> extends Serializable {

  /**
   * Retrieve scheme that of URI that this parser accepts.
   * @return name of acceptable scheme of this factory
   */
  String getAcceptableScheme();


  /**
   * Get {@code ValueSerializer} for given scheme.
   * @param specifier URI specifier of this data type
   * @return {@link ValueSerializer} for the scheme
   */
  ValueSerializer<T> getValueSerializer(URI specifier);

}
