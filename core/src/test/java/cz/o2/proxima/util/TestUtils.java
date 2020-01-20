/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertTrue;

public class TestUtils {
  /**
   * Check if object is serializable
   *
   * @param object Object to check
   * @throws IOException
   */
  public static void assertSerializable(Object object) throws IOException {

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
      oos.writeObject(object);
    }
    assertTrue(
        String.format("Class '%s' isn't serializable.", object.getClass().getName()),
        buffer.toByteArray().length > 0);
  }
}
