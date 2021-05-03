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
package cz.o2.proxima.direct.kafka.serializer;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

/** Test {@link ValueAsBytesSerializer}. */
public class ValueAsBytesSerializerTest {

  Repository repo = Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  EntityDescriptor event = repo.getEntity("event");
  EntityDescriptor gateway = repo.getEntity("gateway");
  long now = System.currentTimeMillis();

  @Test
  public void testDeserializer() {
    ValueAsBytesSerializer serializer = new ValueAsBytesSerializer("data");
    serializer.setup(event);
    StreamElement parsed =
        serializer.parseValue(
            event, ("my-input-string").getBytes(StandardCharsets.UTF_8), 1, 2, now);
    assertNotNull(parsed);
    assertTrue(parsed.getParsed().isPresent());
    assertEquals(now, parsed.getStamp());
  }

  @Test
  public void testDeserializerWithNullValue() {
    ValueAsBytesSerializer serializer = new ValueAsBytesSerializer("data");
    serializer.setup(event);
    StreamElement parsed = serializer.parseValue(event, null, 1, 2, now);
    assertNotNull(parsed);
    assertFalse(parsed.getParsed().isPresent());
    assertEquals(now, parsed.getStamp());
    assertTrue(parsed.isDelete());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testWriteFails() {
    ValueAsBytesSerializer serializer = new ValueAsBytesSerializer("data");
    serializer.write(
        "topic",
        1,
        StreamElement.upsert(
            event,
            event.getAttribute("data"),
            UUID.randomUUID().toString(),
            "key",
            "data",
            now,
            new byte[] {}));
  }

  @Test
  public void testSerdes() {
    ValueAsBytesSerializer serializer = new ValueAsBytesSerializer("data");
    assertEquals(Serdes.ByteArray().getClass(), serializer.keySerde().getClass());
    assertEquals(Serdes.ByteArray().getClass(), serializer.valueSerde().getClass());
  }

  @Test
  public void testParseValue() {
    ValueAsBytesSerializer serializer = new ValueAsBytesSerializer("data");
    serializer.setup(event);
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<byte[], byte[]>(
            "topic",
            1,
            2,
            now,
            TimestampType.CREATE_TIME,
            -1L,
            -1,
            -1,
            null,
            ("my-input-string").getBytes(StandardCharsets.UTF_8));
    StreamElement parsed = serializer.read(record, event);
    assertNotNull(parsed);
    assertTrue(parsed.getParsed().isPresent());
    assertEquals(now, parsed.getStamp());
    assertEquals("data", parsed.getAttribute());
  }

  @Test
  public void testParseValueWildcard() {
    ValueAsBytesSerializer serializer = new ValueAsBytesSerializer("device.*");
    serializer.setup(gateway);
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<byte[], byte[]>(
            "topic",
            1,
            2,
            now,
            TimestampType.CREATE_TIME,
            -1L,
            -1,
            -1,
            null,
            ("my-input-string").getBytes(StandardCharsets.UTF_8));
    StreamElement parsed = serializer.read(record, event);
    assertNotNull(parsed);
    assertTrue(parsed.getParsed().isPresent());
    assertEquals(now, parsed.getStamp());
    assertEquals("device.1:2", parsed.getAttribute());
  }
}
