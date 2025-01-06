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
package cz.o2.proxima.direct.io.kafka.serializer;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class ValueAsStringSerializerTest {

  Repository repo =
      Repository.ofTest(
          // change type to json
          ConfigFactory.parseMap(ImmutableMap.of("entities.event.attributes.data.scheme", "json"))
              .withFallback(ConfigFactory.load("test-reference.conf").resolve()));
  EntityDescriptor event = repo.getEntity("event");
  long now = System.currentTimeMillis();

  @Test
  public void testDeserializer() {
    ValueAsStringSerializer serializer = new ValueAsStringSerializer("data");
    serializer.setup(event);
    StreamElement parsed =
        serializer.parseValue(event, "{\"field\": \"my-input-string\"}", 1, 2, now);
    assertNotNull(parsed);
    assertTrue(parsed.getParsed().isPresent());
    assertEquals(now, parsed.getStamp());
    assertEquals("{\"field\": \"my-input-string\"}", parsed.getParsed().get());
  }

  @Test
  public void testDeserializerWithNullValue() {
    ValueAsStringSerializer serializer = new ValueAsStringSerializer("data");
    serializer.setup(event);
    StreamElement parsed = serializer.parseValue(event, null, 1, 2, now);
    assertNotNull(parsed);
    assertFalse(parsed.getParsed().isPresent());
    assertEquals(now, parsed.getStamp());
    assertTrue(parsed.isDelete());
  }

  @Test
  public void testWrite() {
    ValueAsStringSerializer serializer = new ValueAsStringSerializer("data");
    ProducerRecord<Void, String> rec =
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
                "test".getBytes(StandardCharsets.UTF_8)));
    assertEquals("test", rec.value());
  }

  @Test
  public void testSerdes() {
    ValueAsStringSerializer serializer = new ValueAsStringSerializer("data");
    assertEquals(Serdes.Void().getClass(), serializer.keySerde().getClass());
    assertEquals(Serdes.String().getClass(), serializer.valueSerde().getClass());
  }

  @Test
  public void testParseValue() {
    ValueAsStringSerializer serializer = new ValueAsStringSerializer("data");
    serializer.setup(event);
    ConsumerRecord<Void, String> rec =
        new ConsumerRecord<>(
            "topic", 1, 2, now, TimestampType.CREATE_TIME, -1L, -1, -1, null, "my-input-string");
    StreamElement parsed = serializer.read(rec, event);
    assertNotNull(parsed);
    assertTrue(parsed.getParsed().isPresent());
    assertEquals("my-input-string", parsed.getParsed().get());
    assertEquals(now, parsed.getStamp());
    assertEquals("data", parsed.getAttribute());
  }
}
