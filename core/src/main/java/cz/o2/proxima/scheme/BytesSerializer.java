/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
import java.net.URI;
import java.util.Optional;

/** Validator of bytes scheme. */
@Stable
public class BytesSerializer implements ValueSerializerFactory {

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
        };
  }

  @Override
  public String getClassName(URI scheme) {
    return "byte[]";
  }
}
