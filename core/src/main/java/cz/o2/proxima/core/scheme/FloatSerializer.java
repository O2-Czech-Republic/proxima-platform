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

import com.google.auto.service.AutoService;
import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** {@link ValueSerializerFactory} for floats. */
@Stable
@Slf4j
@AutoService(ValueSerializerFactory.class)
public class FloatSerializer implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public String getAcceptableScheme() {
    return "float";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
    return (ValueSerializer)
        new PrimitiveValueSerializer<Float>() {

          private static final long serialVersionUID = 1L;

          @Override
          public Optional<Float> deserialize(byte[] input) {
            try {
              ByteBuffer buffer = ByteBuffer.wrap(input);
              return Optional.of(buffer.getFloat());
            } catch (Exception ex) {
              log.warn("Failed to parse bytes {}", Arrays.toString(input));
              return Optional.empty();
            }
          }

          @Override
          public byte[] serialize(Float value) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putFloat(value);
            return buffer.array();
          }

          @Override
          public Float getDefault() {
            return 0.0f;
          }

          @Override
          public String asJsonValue(Float value) {
            return String.valueOf(value);
          }

          @Override
          public Float fromJsonValue(String json) {
            return Float.valueOf(json);
          }

          @Override
          public SchemaTypeDescriptor<Float> getValueSchemaDescriptor() {
            return SchemaDescriptors.floats();
          }
        };
  }

  @Override
  public String getClassName(URI specifier) {
    return "Float";
  }
}
