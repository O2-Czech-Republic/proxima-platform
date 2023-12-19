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
import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(ValueSerializerFactory.class)
public class DoubleSerializer implements ValueSerializerFactory {

  @Override
  public String getAcceptableScheme() {
    return "double";
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
    return (ValueSerializer)
        new PrimitiveValueSerializer<Double>() {
          @Override
          public Optional<Double> deserialize(byte[] input) {
            try {
              ByteBuffer buf = ByteBuffer.wrap(input);
              return Optional.of(buf.getDouble());
            } catch (Exception ex) {
              log.warn("Failed to parse bytes {}", Arrays.toString(input), ex);
              return Optional.empty();
            }
          }

          @Override
          public byte[] serialize(Double value) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putDouble(value);
            return buffer.array();
          }

          @Override
          public Double getDefault() {
            return 0.0;
          }

          @Override
          public String asJsonValue(Double value) {
            return String.valueOf(value);
          }

          @Override
          public Double fromJsonValue(String json) {
            return Double.valueOf(json);
          }

          @Override
          public SchemaTypeDescriptor<Double> getValueSchemaDescriptor() {
            return SchemaDescriptors.doubles();
          }
        };
  }

  @Override
  public String getClassName(URI specifier) {
    return "Double";
  }
}
