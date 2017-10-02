/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
import java.util.Optional;

/**
 * Validator of bytes scheme.
 */
public class BytesSerializer implements ValueSerializerFactory<byte[]> {

  @Override
  public String getAcceptableScheme() {
    return "bytes";
  }

  @Override
  public ValueSerializer<byte[]> getValueSerializer(URI scheme) {
    return new ValueSerializer<byte[]>() {

      private final byte[] DEFAULT = new byte[] { };

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

}
