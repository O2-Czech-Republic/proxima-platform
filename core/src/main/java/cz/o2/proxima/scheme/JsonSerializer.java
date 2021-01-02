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

import java.net.URI;

/**
 * A serializer of JSON structures.
 *
 * <p>Not thet this serializer doesn't parse the JSON, it just enables handling the input String as
 * already serialized JSON type.
 */
public class JsonSerializer implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public String getAcceptableScheme() {
    return "json";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
    return (ValueSerializer<T>)
        new StringUtf8Serializer.StringValueSerializer() {

          private static final long serialVersionUID = 1L;

          @Override
          public String getDefault() {
            return "{}";
          }

          @Override
          public String asJsonValue(String value) {
            return value;
          }

          @Override
          public String fromJsonValue(String json) {
            return json;
          }
        };
  }

  @Override
  public String getClassName(URI specifier) {
    return String.class.getName();
  }
}
