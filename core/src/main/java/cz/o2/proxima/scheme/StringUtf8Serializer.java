/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** A {@link ValueSerializer} for Strings. */
public class StringUtf8Serializer implements ValueSerializerFactory {

  @Override
  public String getAcceptableScheme() {
    return "string";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
    return (ValueSerializer)
        new ValueSerializer<String>() {
          @Override
          public Optional<String> deserialize(byte[] input) {
            return Optional.of(new String(input, StandardCharsets.UTF_8));
          }

          @Override
          public byte[] serialize(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
          }

          @Override
          public String getDefault() {
            return "";
          }

          @Override
          public String asJsonValue(String value) {
            return "\"" + value + "\"";
          }

          @Override
          public String fromJsonValue(String json) {
            if (json.startsWith("\"") && json.endsWith("\"")) {
              return json.substring(1, json.length() - 1);
            }
            throw new IllegalArgumentException(json + "is not json string");
          }
        };
  }

  @Override
  public String getClassName(URI specifier) {
    return "String";
  }
}
