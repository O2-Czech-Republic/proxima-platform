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
package cz.o2.proxima.scheme;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.transaction.TransactionSerializerSchemeProvider;
import java.io.Serializable;
import java.net.URI;

/** Factory for {@code ValueSerializer}. The serializer has a specific scheme (e.g. proto:). */
@Stable
public interface ValueSerializerFactory extends Serializable {

  /**
   * Retrieve scheme that of URI that this parser accepts.
   *
   * @return name of acceptable scheme of this factory
   */
  String getAcceptableScheme();

  /**
   * Get {@code ValueSerializer} for given scheme.
   *
   * @param <T> type of deserialized data
   * @param specifier URI specifier of this data type
   * @return {@link ValueSerializer} for the scheme
   */
  <T> ValueSerializer<T> getValueSerializer(URI specifier);

  /**
   * Retrieve class type for given scheme.
   *
   * @param specifier URI specifier of this data type
   * @return full name of class
   * @throws IllegalArgumentException in case of invalid specifier
   */
  default String getClassName(URI specifier) {
    final String type = specifier.getSchemeSpecificPart();
    if (type == null) {
      throw new IllegalArgumentException("Invalid specifier " + specifier.toString() + ".");
    }
    return type;
  }

  /**
   * @return {@code true} if this serializer can provide {@link
   *     TransactionSerializerSchemeProvider}.
   */
  default boolean canProvideTransactionSerializer() {
    return false;
  }

  default TransactionSerializerSchemeProvider createTransactionSerializerSchemeProvider() {
    throw new UnsupportedOperationException();
  }
}
