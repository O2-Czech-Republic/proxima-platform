/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.kafka.LocalKafkaCommitLogDescriptor.Accessor;
import cz.o2.proxima.storage.kafka.LocalKafkaCommitLogDescriptor.LocalKafkaWriter;
import cz.o2.proxima.storage.kafka.partitioner.FirstPartitionPartitioner;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterators;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import javax.annotation.Nullable;
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
public class LocalKafkaCommitLogDescriptorTest implements Serializable {

  final transient Repository repo = Repository.Builder.ofTest(ConfigFactory.empty()).build();
  final AttributeDescriptorBase<?> attr;
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

    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
    LocalKafkaWriter writer = accessor.newWriter();

    CountDownLatch latch = new CountDownLatch(1);
    writer.write(StreamElement.update(
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

    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    LocalKafkaWriter writer = accessor.newWriter();
    CountDownLatch latch = new CountDownLatch(2);
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    writer.write(StreamElement.update(
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

    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
    });
    writer.write(StreamElement.update(
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

    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
    });

    ConsumerRecords<String, byte[]> polled = consumer.poll(1000);
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());

    writer.write(StreamElement.update(
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
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
    LocalKafkaWriter writer = accessor.newWriter();
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(StreamElement.update(
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
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    LocalKafkaWriter writer = accessor.newWriter();
    CountDownLatch latch = new CountDownLatch(2);
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    writer.write(StreamElement.update(
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
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<String, byte[]> c1 = accessor.createConsumerFactory().create(name);
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    assertEquals(2, c1.assignment().size());
    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(1, poll.count());

    writer.write(StreamElement.update(
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

    writer.write(StreamElement.update(
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
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<String, byte[]> c1 = accessor.createConsumerFactory().create(name);
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    assertEquals(1, c1.assignment().size());
    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(1, poll.count());

    writer.write(StreamElement.update(
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

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    poll = c1.poll(1000);
    assertTrue(poll.isEmpty());
    poll = c2.poll(1000);
    assertEquals(1, poll.count());
  }

  @Test(timeout = 2000)
  public void testPartitionedViewSinglePartition() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstPartitionPartitioner.class));
    LocalKafkaWriter writer = accessor.newWriter();
    PartitionedView view = accessor.getPartitionedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing partitioned view"));

    List<Partition> partitions = view.getPartitions();
    assertEquals(3, partitions.size());
    List<Partition> partition = Lists.newArrayList(partitions.subList(0, 1));
    final List<Partition> observed = new ArrayList<>();
    final BlockingQueue<StreamElement> ingests = new SynchronousQueue<>();
    Dataset<Void> result;
    result = view.observePartitions(partition, new PartitionedLogObserver<Void>() {

      @Override
      public void onRepartition(Collection<Partition> assigned) {
        observed.addAll(assigned);
      }

      @Override
      public boolean onNext(
          StreamElement ingest,
          PartitionedLogObserver.ConfirmCallback confirm,
          Partition partition,
          PartitionedLogObserver.Consumer<Void> collector) {

        assertEquals(0, partition.getId());
        confirm.confirm();
        try {
          ingests.put(ingest);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        return false;
      }

      @Override
      public void onCompleted() {

      }

      @Override
      public void onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });

    LocalExecutor runner = new LocalExecutor();
    runner.submit(result.getFlow());

    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(update, (succ, err) -> {
      assertTrue(succ);
      latch.countDown();
    });
    latch.await();
    StreamElement element = ingests.take();
    assertEquals(update.getKey(), element.getKey());
    assertEquals(update.getAttribute(), element.getAttribute());
    assertEquals(update.getAttributeDescriptor(), element.getAttributeDescriptor());
    assertEquals(update.getEntityDescriptor(), element.getEntityDescriptor());
    assertArrayEquals(update.getValue(), element.getValue());
    assertEquals(1, observed.size());

  }

  @Test(timeout = 2000)
  public void testPartitionedView() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    PartitionedView view = accessor.getPartitionedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing partitioned view"));

    final List<Partition> observed = new ArrayList<>();
    final BlockingQueue<StreamElement> ingests = new SynchronousQueue<>();
    Dataset<Void> result;
    result = view.observe("test", new PartitionedLogObserver<Void>() {

      @Override
      public void onRepartition(Collection<Partition> assigned) {
        observed.addAll(assigned);
      }

      @Override
      public boolean onNext(
          StreamElement ingest,
          PartitionedLogObserver.ConfirmCallback confirm,
          Partition partition,
          PartitionedLogObserver.Consumer<Void> collector) {

        confirm.confirm();
        try {
          ingests.put(ingest);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        return false;
      }

      @Override
      public void onCompleted() {

      }

      @Override
      public void onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });

    LocalExecutor runner = new LocalExecutor();
    runner.submit(result.getFlow());

    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(update, (succ, err) -> {
      assertTrue(succ);
      latch.countDown();
    });
    latch.await();
    StreamElement element = ingests.take();
    assertEquals(update.getKey(), element.getKey());
    assertEquals(update.getAttribute(), element.getAttribute());
    assertEquals(update.getAttributeDescriptor(), element.getAttributeDescriptor());
    assertEquals(update.getEntityDescriptor(), element.getEntityDescriptor());
    assertArrayEquals(update.getValue(), element.getValue());
    assertEquals(3, observed.size());

  }

  private static Map<String, Object> partitionsCfg(int partitions) {
    return partitionsCfg(partitions, null);
  }

  private static Map<String, Object> partitionsCfg(
      int partitions, @Nullable Class<? extends Partitioner> partitioner) {

    Map<String, Object> ret = new HashMap<>();
    ret.put(
        LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS,
        String.valueOf(partitions));
    if (partitioner != null) {
      ret.put(KafkaAccessor.PARTITIONER_CLASS, partitioner.getName());
    }
    return ret;
  }

  private static byte[] emptyValue() {
    return new byte[] { };
  }

  private static Context context() {
    return new Context(() -> Executors.newCachedThreadPool()) { };
  }

}
