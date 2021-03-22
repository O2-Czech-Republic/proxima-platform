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
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.io.Serializable;
import java.util.Optional;

/** A serializer of values with specified scheme. */
@Stable
public interface ValueSerializer<T> extends Serializable {

  /**
   * Deserialize the bytes to materialized typed message. If the deserialization fails the returned
   * value is empty.
   *
   * @param input the serialized data
   * @return optional deserialized output
   */
  Optional<T> deserialize(byte[] input);

  /**
   * Serialize value to bytes.
   *
   * @param value the deserialized value
   * @return serialized bytes
   */
  byte[] serialize(T value);

  /**
   * Retrieve a default value for the type.
   *
   * @return default value of the type
   */
  T getDefault();

  /**
   * Check if given serializer can be used without exceptions.
   *
   * @return {@code} true if serializer is usable
   */
  default boolean isUsable() {
    byte[] serialized = serialize(getDefault());
    return deserialize(serialized).map(getDefault()::equals).orElse(false);
  }

  /**
   * Check if given input is valid by trying to parse it.
   *
   * @param input serialized data
   * @return {@code true} if this is valid byte representation
   */
  default boolean isValid(byte[] input) {
    return deserialize(input).isPresent();
  }

  /**
   * Convert given value to JSON representation (including quotation).
   *
   * @param value the value to encode
   * @return the JSON string
   */
  default String asJsonValue(T value) {
    throw new UnsupportedOperationException(
        getClass() + " is not ported to support JSON (de)serialization. Please fill issue.");
  }

  /**
   * Convert given JSON string to parsed object.
   *
   * @param json the JSON representation
   * @return parsed object
   */
  default T fromJsonValue(String json) {
    throw new UnsupportedOperationException(
        getClass() + " is not ported to support JSON (de)serialization. Please fill issue.");
  }

  /**
   * Create a (preferably single line) String representation of the value suitable for logging.
   *
   * @param value the value to log Convert value to {@link String} suitable for logging.
   * @return a String representation suitable for logging
   */
  default String getLogString(T value) {
    // by default use toString
    return value.toString();
  }

  /**
   * Provide {@link SchemaTypeDescriptor} for given attribute value.
   *
   * @return value descriptor
   */
  default SchemaTypeDescriptor<T> getValueSchemaDescriptor() {
    throw new UnsupportedOperationException(
        getClass() + " is not ported to provide a ValueSchemaDescriptor. Please fill issue.");
  }

  /**
   * Provide {@link AttributeValueAccessor} for give attribute value.
   *
   * @return value accessor
   */
  default <V> AttributeValueAccessor<T, V> getValueAccessor() {
    throw new UnsupportedOperationException(
        getClass() + " is not ported to provide a ValueAccessor. Please fill issue.");
  }
}
