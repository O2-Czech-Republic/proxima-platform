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
package cz.o2.proxima.beam.core.io;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.Repository;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Test suite for {@link StreamElementCoder}.
 */
public class StreamElementCoderTest {

  @Test
  public void testCoderSerializable() throws IOException, ClassNotFoundException {
    final byte[] serialized;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(StreamElementCoder.of(
          Repository.of(() -> ConfigFactory.load("test-reference.conf"))));
      serialized = baos.toByteArray();
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
         ObjectInputStream ois = new ObjectInputStream(bais)) {
      ois.readObject();
    }
    assertTrue(
        "Length of serialized bytes should be less than 2 KiB, got " + serialized.length,
        serialized.length < 2048);
  }

}
