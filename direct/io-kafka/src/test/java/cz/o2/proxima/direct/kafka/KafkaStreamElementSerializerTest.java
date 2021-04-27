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
package cz.o2.proxima.direct.kafka;

import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.kafka.KafkaStreamElement.KafkaStreamElementSerializer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

/** Test {@link cz.o2.proxima.direct.kafka.KafkaStreamElement.KafkaStreamElementSerializer} */
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
