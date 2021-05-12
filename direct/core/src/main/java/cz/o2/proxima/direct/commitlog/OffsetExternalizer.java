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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.scheme.SerializationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Interface provides methods for serializing commit log offset {@link
 * cz.o2.proxima.direct.commitlog.Offset} to external formats.
 */
public interface OffsetExternalizer {

  /**
   * Serializes offset to bytes, default implementation uses native java serialization.
   *
   * @param offset to serialize.
   * @return serialized offset in bytes
   */
  default byte[] toBytes(Offset offset) {
    try (final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        final ObjectOutputStream outputStream = new ObjectOutputStream(arrayOutputStream)) {

      outputStream.writeObject(offset);
      return arrayOutputStream.toByteArray();

    } catch (IOException e) {
      throw new SerializationException("Can't externalize offset to bytes", e);
    }
  }

  /**
   * Deserializes offset from bytes, default implementation uses native java serialization.
   *
   * @param bytes to be deserialized to offset.
   * @return deserialized offset
   */
  @SuppressWarnings("unchecked")
  default Offset fromBytes(byte[] bytes) {
    try (final ObjectInputStream inputStream =
        new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return (Offset) inputStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new SerializationException("Can't read externalized offset from bytes", e);
    }
  }

  /**
   * Serializes offset to JSON string.
   *
   * @param offset to serialize.
   * @return serialized offset in JSON string.
   */
  String toJson(Offset offset);

  /**
   * Deserializes offset from JSON string.
   *
   * @param json to be deserialized to offset.
   * @return deserialized offset.
   */
  Offset fromJson(String json);
}
