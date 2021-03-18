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
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** Serializer factory for Longs. */
@Stable
@Slf4j
public class LongSerializer implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public String getAcceptableScheme() {
    return "long";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
    return (ValueSerializer)
        new ValueSerializer<Long>() {

          private static final long serialVersionUID = 1L;

          @Override
          public Optional<Long> deserialize(byte[] input) {
            try {
              ByteBuffer buffer = ByteBuffer.wrap(input);
              return Optional.ofNullable(buffer.getLong());
            } catch (Exception ex) {
              log.warn("Failed to parse bytes {}", Arrays.toString(input));
              return Optional.empty();
            }
          }

          @Override
          public byte[] serialize(Long value) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(value);
            return buffer.array();
          }

          @Override
          public Long getDefault() {
            return 0L;
          }

          @Override
          public String asJsonValue(Long value) {
            return String.valueOf(value);
          }

          @Override
          public Long fromJsonValue(String json) {
            return Long.valueOf(json);
          }

          @Override
          public SchemaTypeDescriptor<Long> getValueSchemaDescriptor() {
            return SchemaDescriptors.longs();
          }
        };
  }

  @Override
  public String getClassName(URI specifier) {
    return "Long";
  }
}
