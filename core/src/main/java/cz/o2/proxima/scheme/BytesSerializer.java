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

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.net.URI;
import java.util.Base64;
import java.util.Optional;

/** Validator of bytes scheme. */
@Stable
public class BytesSerializer implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  private static final byte[] DEFAULT = new byte[] {};

  @Override
  public String getAcceptableScheme() {
    return "bytes";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI scheme) {
    return (ValueSerializer)
        new ValueSerializer<byte[]>() {

          private static final long serialVersionUID = 1L;

          @Override
          public Optional<byte[]> deserialize(byte[] input) {
            return Optional.of(input);
          }

          @Override
          public byte[] getDefault() {
            return DEFAULT;
          }

          @Override
          public byte[] serialize(byte[] value) {
            return value;
          }

          @Override
          public String asJsonValue(byte[] value) {
            return '"' + Base64.getEncoder().encodeToString(value) + '"';
          }

          @Override
          public byte[] fromJsonValue(String json) {
            Preconditions.checkArgument(
                json.length() >= 2
                    && json.charAt(0) == json.charAt(json.length() - 1)
                    && json.charAt(0) == '"',
                "[%s] is not valid json string",
                json);
            return Base64.getDecoder().decode(json.substring(1, json.length() - 1));
          }

          @Override
          public SchemaTypeDescriptor<byte[]> getValueSchemaDescriptor() {
            return SchemaDescriptors.bytes();
          }

          @Override
          public AttributeValueAccessor<byte[], byte[]> getValueAccessor() {
            return PrimitiveValueAccessor.of(UnaryFunction.identity(), UnaryFunction.identity());
          }
        };
  }

  @Override
  public String getClassName(URI scheme) {
    return "byte[]";
  }
}
