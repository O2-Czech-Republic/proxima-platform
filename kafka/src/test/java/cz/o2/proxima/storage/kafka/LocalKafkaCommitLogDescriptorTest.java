/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.storage.kafka;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterators;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@code LocalKafkaCommitLogDescriptorTest}.
 */
public class LocalKafkaCommitLogDescriptorTest {

  final Repository repo = Repository.Builder.ofTest(ConfigFactory.empty()).build();
  final AttributeDescriptor attr;
  final EntityDescriptor entity;
  final URI storageURI;

  LocalKafkaCommitLogDescriptor kafka;

  public LocalKafkaCommitLogDescriptorTest() throws Exception {
    this.attr = AttributeDescriptor
        .newBuilder(repo)
        .setEntity("entity")
        .setName("attr")
        .setSchemeURI(new URI("bytes:///"))
        .build();

    this.entity = EntityDescriptor.newBuilder()
        .setName("entity")
        .addAttribute(attr)
        .build();

    this.storageURI = new URI("kafka-test://dummy/topic");
  }

  @Before
  public void setUp() {
    kafka = new LocalKafkaCommitLogDescriptor();
  }

  @Test(timeout = 2000)
  public void testSinglePartitionWriteAndConsumeBySingleConsumerRunAfterWrite()
      throws InterruptedException {

    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
    CountDownLatch latch = new CountDownLatch(1);
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(),
        emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    latch.await();
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    ConsumerRecords<String, byte[]> polled = consumer.poll(1000);
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
    assertEquals(0, partition.partition());
    assertEquals("topic", partition.topic());
    int tested = 0;
    for (ConsumerRecord<String, byte[]> r : polled) {
      assertEquals("key#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), r.value());
      tested++;
    }
    assertEquals(1, tested);
  }

  @Test(timeout = 2000)
  public void testTwoPartitionsTwoWritesAndConsumeBySingleConsumerRunAfterWrite()
      throws InterruptedException {

    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    CountDownLatch latch = new CountDownLatch(2);
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    latch.await();
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    ConsumerRecords<String, byte[]> polled = consumer.poll(1000);
    assertEquals(2, polled.count());
    assertEquals(2, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());
    TopicPartition second = iterator.next();
    assertEquals(1, second.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<String, byte[]> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), r.value());
    }
    assertEquals(2, tested);
  }

  @Test
  public void testEmptyPoll() {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    assertTrue(consumer.poll(100).isEmpty());
  }

  @Test(timeout = 2000)
  public void testTwoPartitionsTwoWritesAndConsumeBySingleConsumerRunBeforeWrite()
      throws InterruptedException {

    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
    });
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
    });

    ConsumerRecords<String, byte[]> polled = consumer.poll(1000);
    assertEquals(2, polled.count());
    assertEquals(2, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());
    TopicPartition second = iterator.next();
    assertEquals(1, second.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<String, byte[]> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), r.value());
    }
    assertEquals(2, tested);
  }

  @Test(timeout = 4000)
  public void testTwoPartitionsTwoWritesAndTwoReads()
      throws InterruptedException {

    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
    });

    ConsumerRecords<String, byte[]> polled = consumer.poll(1000);
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());

    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
    });

    polled = consumer.poll(1000);
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
  }

  @Test(timeout = 4000)
  @SuppressWarnings("unchecked")
  public void testTwoIdependentConsumers() throws InterruptedException {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
    CountDownLatch latch = new CountDownLatch(1);
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    latch.await();
    KafkaConsumer<String, byte[]>[] consumers = new KafkaConsumer[] {
      accessor.createConsumerFactory().create("dumm1"),
      accessor.createConsumerFactory().create("dumm2"),
    };
    for (KafkaConsumer<String, byte[]> consumer : consumers) {
      ConsumerRecords<String, byte[]> polled = consumer.poll(1000);
      assertEquals(1, polled.count());
      assertEquals(1, polled.partitions().size());
      TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
      assertEquals(0, partition.partition());
      assertEquals("topic", partition.topic());
      int tested = 0;
      for (ConsumerRecord<String, byte[]> r : polled) {
        assertEquals("key#attr", r.key());
        assertEquals("topic", r.topic());
        assertArrayEquals(emptyValue(), r.value());
        tested++;
      }
      assertEquals(1, tested);
    }
  }

  @Test(timeout = 2000)
  public void testManualPartitionAssignment() throws InterruptedException {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    CountDownLatch latch = new CountDownLatch(2);
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    latch.await();
    KafkaConsumer<String, byte[]> consumer = accessor
        .createConsumerFactory().create(Arrays.asList((Partition) () -> 0));

    ConsumerRecords<String, byte[]> polled = consumer.poll(1000);
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<String, byte[]> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), r.value());
    }
    assertEquals(1, tested);
  }


  @Test
  public void testTwoPartitionsTwoConsumersRebalance() {
    String name = "consumer";
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    KafkaConsumer<String, byte[]> c1 = accessor.createConsumerFactory().create(name);
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    assertEquals(2, c1.assignment().size());
    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(1, poll.count());

    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    poll = c1.poll(1000);
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {{
      put(new TopicPartition("topic", 0), new OffsetAndMetadata(1));
      put(new TopicPartition("topic", 1), new OffsetAndMetadata(1));
    }});

    // create another consumer
    KafkaConsumer<String, byte[]> c2 = accessor.createConsumerFactory().create(name);

    // rebalanced
    assertEquals(1, c1.assignment().size());
    assertEquals(1, c2.assignment().size());

    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    poll = c1.poll(1000);
    assertTrue(poll.isEmpty());
    poll = c2.poll(1000);
    assertEquals(1, poll.count());


  }


  @Test
  public void testSinglePartitionTwoConsumersRebalance() {
    String name = "consumer";
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
    KafkaConsumer<String, byte[]> c1 = accessor.createConsumerFactory().create(name);
    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    assertEquals(1, c1.assignment().size());
    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(1, poll.count());

    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    poll = c1.poll(1000);
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {{
      put(new TopicPartition("topic", 0), new OffsetAndMetadata(2));
    }});

    // create another consumer
    KafkaConsumer<String, byte[]> c2 = accessor.createConsumerFactory().create(name);

    // not rebalanced (there are no free partitions)
    assertEquals(0, c1.assignment().size());
    assertEquals(1, c2.assignment().size());

    accessor.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    poll = c1.poll(1000);
    assertTrue(poll.isEmpty());
    poll = c2.poll(1000);
    assertEquals(1, poll.count());

  }



  private Map<String, Object> partitionsCfg(int partitions) {
    Map<String, Object> ret = new HashMap<>();
    ret.put(
        LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS,
        String.valueOf(partitions));
    return ret;
  }

  private byte[] emptyValue() {
    return new byte[] { };
  }

}
