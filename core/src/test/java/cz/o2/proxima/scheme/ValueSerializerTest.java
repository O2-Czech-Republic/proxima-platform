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

import static org.junit.Assert.assertThrows;

import java.util.Optional;
import org.junit.Test;

public class ValueSerializerTest {

  final ValueSerializer<byte[]> serializer =
      new ValueSerializer<byte[]>() {
        @Override
        public Optional<byte[]> deserialize(byte[] input) {
          return Optional.empty();
        }

        @Override
        public byte[] serialize(byte[] value) {
          return new byte[0];
        }

        @Override
        public byte[] getDefault() {
          return new byte[0];
        }
      };

  @Test
  public void checkUnsupportedExceptionContract() {
    assertThrows(UnsupportedOperationException.class, () -> serializer.asJsonValue(new byte[] {}));
    assertThrows(UnsupportedOperationException.class, () -> serializer.fromJsonValue(""));
    assertThrows(UnsupportedOperationException.class, serializer::getValueSchemaDescriptor);
    assertThrows(UnsupportedOperationException.class, serializer::getValueAccessor);
  }
}
