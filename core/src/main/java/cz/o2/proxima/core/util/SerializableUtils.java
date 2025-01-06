/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** Various serialization related utilities. */
public class SerializableUtils {

  /**
   * Clone given object using Java serialization.
   *
   * @param original object to clone
   * @param <T> type parameter
   * @return cloned value
   */
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T clone(T original) {
    final byte[] serialized;
    try {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(original);
        oos.flush();
        serialized = baos.toByteArray();
      }
      try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
          ObjectInputStream ois = new ObjectInputStream(bais)) {
        return (T) ois.readObject();
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private SerializableUtils() {}
}
