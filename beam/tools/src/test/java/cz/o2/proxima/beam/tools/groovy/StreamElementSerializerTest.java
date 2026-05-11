/*
 * Copyright 2017-2026 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Input;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

public class StreamElementSerializerTest {

  @Test
  public void testSerialization() throws IOException {
    Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    StreamElementSerializer serializer = new StreamElementSerializer(repo.asFactory());
    EntityDescriptor event = repo.getEntity("event");
    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<byte[]> data = event.getAttribute("data");
    AttributeDescriptor<Object> rule = gateway.getAttribute("rule.*");

    {
      StreamElement el =
          StreamElement.upsert(
              event,
              data,
              "uuid",
              "key",
              data.getName(),
              System.currentTimeMillis(),
              new byte[] {1, 2, 3});
      checkSerialization(serializer, el);
    }
    {
      StreamElement el =
          StreamElement.upsert(
              event,
              data,
              1,
              "key",
              data.getName(),
              System.currentTimeMillis(),
              new byte[] {1, 2, 3});
      checkSerialization(serializer, el);
    }
    {
      StreamElement el =
          StreamElement.delete(
              event, data, "uuid", "key", data.getName(), System.currentTimeMillis());
      checkSerialization(serializer, el);
    }
    {
      StreamElement el =
          StreamElement.delete(event, data, 1, "key", data.getName(), System.currentTimeMillis());
      checkSerialization(serializer, el);
    }
    {
      StreamElement el =
          StreamElement.upsert(
              gateway,
              rule,
              "uuid",
              "key",
              "rule.1",
              System.currentTimeMillis(),
              new byte[] {1, 2, 3});
      checkSerialization(serializer, el);
    }
    {
      StreamElement el =
          StreamElement.upsert(
              gateway, rule, 1, "key", "rule.1", System.currentTimeMillis(), new byte[] {1, 2, 3});
      checkSerialization(serializer, el);
    }
    {
      StreamElement el =
          StreamElement.delete(gateway, rule, "uuid", "key", "rule.1", System.currentTimeMillis());
      checkSerialization(serializer, el);
    }
    {
      StreamElement el =
          StreamElement.delete(gateway, rule, 1, "key", "rule.1", System.currentTimeMillis());
      checkSerialization(serializer, el);
    }
  }

  private static void checkSerialization(StreamElementSerializer serializer, StreamElement el)
      throws IOException {
    byte[] serialized;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos)) {
      serializer.write(null, output, el);
      output.flush();
      serialized = baos.toByteArray();
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        Input input = new Input(bais)) {
      assertEquals(serializer.read(null, input, null), el);
    }
  }
}
