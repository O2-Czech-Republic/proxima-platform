/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.io.kafka.KafkaStreamElement.KafkaStreamElementSerializer;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

/** Test {@link cz.o2.proxima.direct.io.kafka.KafkaStreamElement.KafkaStreamElementSerializer} */
public class KafkaStreamElementSerializerTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<?> status = gateway.getAttribute("status");
  private final ElementSerializer<String, byte[]> serializer = new KafkaStreamElementSerializer();

  @Test
  public void testSerDe() {
    StreamElement elem =
        StreamElement.upsert(
            gateway,
            status,
            "topic#1#0",
            "key",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {});
    ProducerRecord<String, byte[]> record = serializer.write("topic", 1, elem);
    ConsumerRecord<String, byte[]> read = asConsumer(record, 0);
    StreamElement readElement = serializer.read(read, gateway);
    assertEquals(elem, readElement);
  }

  @Test
  public void testSerDeWithSeqId() {
    StreamElement elem =
        StreamElement.upsert(
            gateway,
            status,
            1L,
            "key",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {});
    ProducerRecord<String, byte[]> record = serializer.write("topic", 1, elem);
    ConsumerRecord<String, byte[]> read = asConsumer(record, 0);
    StreamElement readElement = serializer.read(read, gateway);
    assertEquals(elem, readElement);
  }

  @Test
  public void testInvalidUuidHeader() {
    ProducerRecord<String, byte[]> record =
        new ProducerRecord<>(
            "topic",
            0,
            System.currentTimeMillis(),
            "key#status",
            new byte[0],
            Collections.singletonList(new RecordHeader(KafkaAccessor.UUID_HEADER, "".getBytes())));
    StreamElement read = serializer.read(asConsumer(record, 0), gateway);
    assertEquals("key", read.getKey());
    assertFalse(read.getUuid().isEmpty());
  }

  private <K, V> ConsumerRecord<K, V> asConsumer(ProducerRecord<K, V> record, long offset) {
    return new ConsumerRecord<>(
        record.topic(),
        record.partition(),
        offset,
        record.timestamp(),
        TimestampType.CREATE_TIME,
        null,
        0,
        0,
        record.key(),
        record.value(),
        record.headers());
  }
}
