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

import cz.o2.proxima.storage.commitlog.Partitioner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.commitlog.RetryableBulkObserver;
import cz.o2.proxima.storage.kafka.LocalKafkaCommitLogDescriptor.Accessor;
import cz.o2.proxima.storage.kafka.LocalKafkaCommitLogDescriptor.LocalKafkaWriter;
import cz.o2.proxima.storage.commitlog.FirstPartitionPartitioner;
import cz.o2.proxima.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.view.PartitionedCachedView;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import java.io.IOException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class LocalKafkaCommitLogDescriptorTest implements Serializable {

  private final transient Factory<ExecutorService> serviceFactory =
      () -> Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r);
        t.setUncaughtExceptionHandler((thr, exc) -> exc.printStackTrace(System.err));
        return t;
      });
  private final transient Repository repo = ConfigRepository.Builder
      .ofTest(ConfigFactory.empty())
      .withExecutorFactory(serviceFactory)
      .build();
  private final AttributeDescriptorBase<byte[]> attr;
  private final AttributeDescriptorBase<byte[]> attrWildcard;
  private final EntityDescriptor entity;
  private final URI storageURI;

  private LocalKafkaCommitLogDescriptor kafka;

  public LocalKafkaCommitLogDescriptorTest() throws Exception {
    this.attr = AttributeDescriptor
        .newBuilder(repo)
        .setEntity("entity")
        .setName("attr")
        .setSchemeURI(new URI("bytes:///"))
        .build();


    this.attrWildcard = AttributeDescriptor
        .newBuilder(repo)
        .setEntity("entity")
        .setName("wildcard.*")
        .setSchemeURI(new URI("bytes:///"))
        .build();

    this.entity = EntityDescriptor.newBuilder()
        .setName("entity")
        .addAttribute(attr)
        .addAttribute(attrWildcard)
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
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
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
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
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

  @Test
  public void testWriteNull() {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(2));
    OnlineAttributeWriter writer = accessor.getWriter(context()).get().online();
    long now = 1234567890000L;
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(), "key", attr.getName(),
        now, new byte[] { 1 }), (succ, exc) -> { });
    writer.write(StreamElement.delete(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), now + 1000), (succ, exc) -> { });
    ConsumerRecords<String, byte[]> polled = consumer.poll(100);
    assertEquals(2, polled.count());
    int matched = 0;
    for (ConsumerRecord<String, byte[]> r : polled) {
      if (r.timestamp() == now) {
        assertEquals(1, r.value().length);
        matched++;
      } if (r.timestamp() == now + 1000) {
        assertNull(r.value());
        matched++;
      }
    }
    assertEquals(2, matched);
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
    KafkaConsumer<String, byte[]>[] consumers = new KafkaConsumer[] {
      accessor.createConsumerFactory().create("dummy1"),
      accessor.createConsumerFactory().create("dummy2"),
    };
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
    KafkaConsumer<String, byte[]> consumer = accessor
        .createConsumerFactory().create(Arrays.asList((Partition) () -> 0));
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

  @Test(timeout = 2000)
  public void testPollAfterWrite() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
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
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    latch.await();
    KafkaConsumer<String, byte[]> consumer = accessor
        .createConsumerFactory().create(Arrays.asList((Partition) () -> 0));

    ConsumerRecords<String, byte[]> polled = consumer.poll(100);
    assertTrue(polled.isEmpty());
  }

  @Test(timeout = 2000)
  public void testPollWithSeek() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(1));
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
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
      assertTrue(succ);
      assertNull(exc);
      latch.countDown();
    });
    latch.await();
    KafkaConsumer<String, byte[]> consumer = accessor
        .createConsumerFactory().create(Arrays.asList((Partition) () -> 0));
    consumer.seek(new TopicPartition("topic", 0), 1);

    ConsumerRecords<String, byte[]> polled = consumer.poll(100);
    assertEquals(1, polled.count());
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

    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(2, c1.assignment().size());
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

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    poll = c2.poll(1000);
    assertEquals(1, poll.count());
    poll = c1.poll(1000);
    assertTrue(poll.isEmpty());

    // rebalanced
    assertEquals(1, c1.assignment().size());
    assertEquals(1, c2.assignment().size());


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

    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(1, c1.assignment().size());
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

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
    });

    poll = c2.poll(1000);
    assertEquals(1, poll.count());
    poll = c1.poll(1000);
    assertTrue(poll.isEmpty());

    // not rebalanced (there are no free partitions)
    assertEquals(0, c1.assignment().size());
    assertEquals(1, c2.assignment().size());

  }

  @Test(timeout = 5000)
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
          Consumer<Void> collector) {

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
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });

    LocalExecutor runner = new LocalExecutor();
    runner.submit(result.getFlow());

    // we don't have any way to wait only until the consumer is ready,
    // so we just wait for fixed amount of time
    Thread.sleep(500);
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
          Consumer<Void> collector) {

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
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });

    LocalExecutor runner = new LocalExecutor();
    runner.submit(result.getFlow());

    // need to wait here before the runner initializes
    Thread.sleep(500);

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

  @Test(timeout = 2000)
  public void testBulkObserveWithException() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    AtomicInteger restarts = new AtomicInteger();
    AtomicReference<Throwable> exc = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(2);
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    ObserveHandle handle = reader.observeBulk("test", Position.NEWEST, new BulkLogObserver() {

      @Override
      public boolean onNext(
          StreamElement ingest, BulkLogObserver.OffsetCommitter confirm) {
        restarts.incrementAndGet();
        throw new RuntimeException("FAIL!");
      }

      @Override
      public void onCompleted() {
        fail("This should not be called");
      }

      @Override
      public boolean onError(Throwable error) {
        exc.set(error);
        latch.countDown();
        throw new RuntimeException(error);
      }

    });

    writer.write(update, (succ, e) -> {
      assertTrue(succ);
      latch.countDown();
    });
    latch.await();
    assertEquals("FAIL!", exc.get().getMessage());
    assertEquals(1, restarts.get());
    assertEquals(3, handle.getCommittedOffsets().size());
    handle.getCurrentOffsets()
        .forEach(o -> assertEquals(0, ((TopicOffset) o).getOffset()));
  }

  @Test(timeout = 2000)
  public void testBulkObserveWithExceptionAndRetry() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger restarts = new AtomicInteger();
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    RetryableBulkObserver observer = new RetryableBulkObserver(3, "test", reader) {

      @Override
      protected void failure() {
        latch.countDown();
      }

      @Override
      protected boolean onNextInternal(
          StreamElement ingest, BulkLogObserver.OffsetCommitter confirm) {
        restarts.incrementAndGet();
        throw new RuntimeException("FAIL!");
      }

    };
    observer.start();
    Executors.newCachedThreadPool().execute(() -> {
      while (true) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException ex) {
          break;
        }
        writer.write(update, (succ, e) -> {
          assertTrue(succ);
        });
      }
    });
    latch.await();
    assertEquals(3, restarts.get());
  }


  @Test(timeout = 2000)
  public void testBulkObserveSuccess() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    AtomicInteger restarts = new AtomicInteger();
    AtomicReference<Throwable> exc = new AtomicReference<>();
    AtomicReference<StreamElement> input = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(2);
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    ObserveHandle handle = reader.observeBulk("test", Position.NEWEST, new BulkLogObserver() {

      @Override
      public void onRestart(List<Offset> offsets) {
        restarts.incrementAndGet();
      }

      @Override
      public boolean onNext(
          StreamElement ingest, BulkLogObserver.OffsetCommitter context) {
        input.set(ingest);
        context.confirm();
        latch.countDown();
        return true;
      }

      @Override
      public void onCompleted() {
        fail("This should not be called");
      }

      @Override
      public boolean onError(Throwable error) {
        exc.set(error);
        throw new RuntimeException(error);
      }

    });

    writer.write(update, (succ, e) -> {
      assertTrue(succ);
      latch.countDown();
    });
    latch.await();
    assertNull(exc.get());
    assertEquals(1, restarts.get());
    assertArrayEquals(update.getValue(), input.get().getValue());
    assertEquals(3, handle.getCommittedOffsets().size());
    assertEquals(1L, (long) handle.getCommittedOffsets()
        .stream()
        .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));
  }

  @Test(timeout = 2000)
  public void testBulkObservePartitionsSuccess() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    AtomicInteger restarts = new AtomicInteger();
    AtomicReference<Throwable> exc = new AtomicReference<>();
    AtomicReference<StreamElement> input = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(2);
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    ObserveHandle handle = reader.observeBulkPartitions(reader.getPartitions(), Position.NEWEST, new BulkLogObserver() {

      @Override
      public void onRestart(List<Offset> offsets) {
        restarts.incrementAndGet();
      }

      @Override
      public boolean onNext(
          StreamElement ingest, BulkLogObserver.OffsetCommitter context) {
        input.set(ingest);
        context.confirm();
        latch.countDown();
        return true;
      }

      @Override
      public void onCompleted() {
        fail("This should not be called");
      }

      @Override
      public boolean onError(Throwable error) {
        exc.set(error);
        throw new RuntimeException(error);
      }

    });

    writer.write(update, (succ, e) -> {
      assertTrue(succ);
      latch.countDown();
    });
    latch.await();
    assertNull(exc.get());
    assertEquals(1, restarts.get());
    assertArrayEquals(update.getValue(), input.get().getValue());
    assertEquals(3, handle.getCommittedOffsets().size());
    assertEquals(1L, (long) handle.getCommittedOffsets()
        .stream()
        .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));
  }

  @Test(timeout = 2000)
  public void testBulkObserveOffsets() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    List<KafkaStreamElement> input = new ArrayList<>();
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(3));
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    Map<Integer, Offset> currentOffsets = new HashMap<>();

    BulkLogObserver observer = new BulkLogObserver() {

      @Override
      public boolean onNext(
          StreamElement ingest, BulkLogObserver.OffsetCommitter context) {

        input.add((KafkaStreamElement) ingest);
        context.confirm();
        latch.get().countDown();
        // terminate after reading first record when running first time
        return !currentOffsets.isEmpty();
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    };
    ObserveHandle handle = reader.observeBulkPartitions(
        reader.getPartitions(), Position.NEWEST, observer);

    // write two elements
    for (int i = 0; i < 2; i++) {
      writer.write(update, (succ, e) -> {
        assertTrue(succ);
        latch.get().countDown();
      });
    }
    latch.get().await();
    latch.set(new CountDownLatch(1));

    handle.getCommittedOffsets().forEach(o -> currentOffsets.put(o.getPartition().getId(), o));
    handle.cancel();

    // each partitions has a record here
    assertEquals(3, currentOffsets.size());
    assertEquals(1L, (long) currentOffsets.values()
        .stream()
        .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));

    // restart from old offset
    handle = reader.observeBulkOffsets(
        Lists.newArrayList(currentOffsets.values()), observer);
    latch.get().await();
    assertEquals(2, input.size());
    assertEquals(0, input.get(0).getOffset());
    assertEquals(1, input.get(1).getOffset());
    // committed offset 1 and 2
    assertEquals(2L, (long) handle.getCommittedOffsets()
        .stream()
        .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));
  }

  @Test(timeout = 2000)
  public void testCachedView() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(entity, storageURI, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });
    writer.write(update, (succ, exc) -> {
      assertTrue(succ);
      latch.get().countDown();
    });
    latch.get().await();
    latch.set(new CountDownLatch(1));
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertArrayEquals(new byte[] { 1, 2 }, view.get("key", attr).get().getValue());
    update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2, 3 });
    writer.write(update, (succ, exc) -> {
      assertTrue(succ);
      latch.get().countDown();
    });
    latch.get().await();
    TimeUnit.SECONDS.sleep(1);
    assertArrayEquals(new byte[] { 1, 2, 3 }, view.get("key", attr).get().getValue());
  }

  @Test(timeout = 2000)
  public void testCachedViewReload() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstBytePartitioner.class));
    LocalKafkaWriter writer = accessor.newWriter();
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(2));
    List<StreamElement> updates = Arrays.asList(
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 }),
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key2", attr.getName(), System.currentTimeMillis(), new byte[] { 2, 3 }));
    updates.forEach(update -> writer.write(update, (succ, exc) -> {
      assertTrue(succ);
      latch.get().countDown();
    }));
    latch.get().await();
    latch.set(new CountDownLatch(1));
    view.assign(IntStream.range(1, 2)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertFalse(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2, 3 });
    writer.write(update, (succ, exc) -> {
      assertTrue(succ);
      latch.get().countDown();
    });
    latch.get().await();
    TimeUnit.SECONDS.sleep(1);
    view.assign(IntStream.range(1, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 2000)
  public void testCachedViewWrite() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstBytePartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    List<StreamElement> updates = Arrays.asList(
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 }),
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key2", attr.getName(), System.currentTimeMillis(), new byte[] { 2, 3 }));
    CountDownLatch latch = new CountDownLatch(2);
    updates.forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: " + exc, succ);
      latch.countDown();
    }));
    latch.await();
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 2000)
  public void testCachedViewWriteAndDelete() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstBytePartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    List<StreamElement> updates = Arrays.asList(
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), now - 1000, new byte[] { 1, 2 }),
      StreamElement.delete(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), now));
    CountDownLatch latch = new CountDownLatch(2);
    updates.forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: " + exc, succ);
      latch.countDown();
    }));
    latch.await();
    assertFalse(view.get("key1", attr).isPresent());
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertFalse(view.get("key1", attr).isPresent());
  }


  @Test(timeout = 2000)
  public void testCachedViewWriteAndDeleteWildcard() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstBytePartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.1", now - 1000, new byte[] { 1, 2 }),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.2", now - 500, new byte[] { 1, 2 }),
      StreamElement.deleteWildcard(
          entity, attrWildcard, UUID.randomUUID().toString(), "key1", now),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.1", now + 500, new byte[] { 2, 3 }),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.3", now - 500, new byte[] { 3, 4 })
    ).forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: " + exc, succ);
      latch.countDown();
    }));
    latch.await();
    assertTrue(view.get("key1", "wildcard.1", attrWildcard).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard).isPresent());
    assertArrayEquals(
        new byte[] { 2, 3 },
        view.get("key1", "wildcard.1", attrWildcard).get().getValue());
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertTrue(view.get("key1", "wildcard.1", attrWildcard).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard).isPresent());
    assertArrayEquals(
        new byte[] { 2, 3 },
        view.get("key1", "wildcard.1", attrWildcard).get().getValue());
  }

  @Test(timeout = 2000)
  public void testCachedViewWriteAndList() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstBytePartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), now - 1000, new byte[] { 1, 2 }),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.1", now - 1000, new byte[] { 1, 2 }),
      StreamElement.deleteWildcard(
          entity, attrWildcard, UUID.randomUUID().toString(), "key1", now - 500),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.2", now, new byte[] { 1, 2 }),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.3", now - 499, new byte[] { 3, 4 })
    ).forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: ", succ);
      latch.countDown();
    }));
    latch.await();
    List<KeyValue<byte[]>> res = new ArrayList<>();
    view.scanWildcard("key1", attrWildcard, res::add);
    assertEquals(2, res.size());
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    res.clear();
    view.scanWildcard("key1", attrWildcard, res::add);
    assertEquals(2, res.size());
  }

  @Test(timeout = 2000)
  public void testCachedViewWriteAndListAll() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstBytePartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), now - 2000, new byte[] { 0 }),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.1", now - 1000, new byte[] { 1, 2 }),
      StreamElement.deleteWildcard(
          entity, attrWildcard, UUID.randomUUID().toString(), "key1", now - 500),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.2", now, new byte[] { 1, 2 }),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.3", now - 499, new byte[] { 3, 4 })
    ).forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: ", succ);
      latch.countDown();
    }));
    latch.await();
    List<KeyValue<?>> res = new ArrayList<>();
    view.scanWildcardAll("key1", res::add);
    assertEquals(3, res.size());
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    res.clear();
    view.scanWildcardAll("key1", res::add);
    assertEquals(3, res.size());
  }

  @Test(timeout = 2000)
  public void testCachedViewWritePreUpdate() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, FirstBytePartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    List<StreamElement> updates = Arrays.asList(
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 }),
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key2", attr.getName(), System.currentTimeMillis(), new byte[] { 2, 3 }));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: " + exc, succ);
      latch.countDown();
    }));
    latch.await();
    AtomicInteger calls = new AtomicInteger();
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()),
        (e, c) -> calls.incrementAndGet());
    assertEquals(2, calls.get());
  }

  @Test(timeout = 2000)
  public void testCachedViewWritePreUpdateAndDeleteWildcard() throws InterruptedException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, KeyPartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    List<StreamElement> updates = Arrays.asList(
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.1", now, new byte[] { 1, 2 }),
      StreamElement.deleteWildcard(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", now + 1000L),
      StreamElement.update(
          entity, attrWildcard, UUID.randomUUID().toString(),
          "key1", "wildcard.2", now + 500L, new byte[] { 2, 3 }));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: " + exc, succ);
      latch.countDown();
    }));
    latch.await();
    AtomicInteger calls = new AtomicInteger();
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()),
        (e, c) -> calls.incrementAndGet());
    assertEquals(2, calls.get());
  }

  @Test
  public void testRewriteAndPrefetch() throws InterruptedException, IOException {
    Accessor accessor = kafka.getAccessor(
        entity, storageURI, partitionsCfg(3, KeyPartitioner.class));
    PartitionedCachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    List<StreamElement> updates = Arrays.asList(
      // store first value
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), now, new byte[] { 1, 2 }),
      // update the value at the same stamp
      StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key1", attr.getName(), now, new byte[] { 2, 3 }));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(update -> view.write(update, (succ, exc) -> {
      assertTrue("Exception: " + exc, succ);
      latch.countDown();
    }));
    latch.await();
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertArrayEquals(new byte[] { 2, 3 }, view.get("key1", attr).get().getValue());
    view.write(StreamElement.update(entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(), now, new byte[] { 3, 4 }), (succ, exc) -> {
          assertTrue(succ);
        });
    assertArrayEquals(new byte[] { 3, 4 }, view.get("key1", attr).get().getValue());
    view.close();
    assertFalse(view.get("key1", attr).isPresent());
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertArrayEquals(new byte[] { 3, 4 }, view.get("key1", attr).get().getValue());
  }

  private static Map<String, Object> partitionsCfg(int partitions) {
    return partitionsCfg(partitions, null);
  }

  private static Map<String, Object> partitionsCfg(
      int partitions,
      @Nullable Class<? extends Partitioner> partitioner) {

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

  private Context context() {
    return new Context(serviceFactory) { };
  }

  static final class FirstBytePartitioner implements Partitioner {
    @Override
    public int getPartitionId(StreamElement element) {
      if (!element.isDelete()) {
        return (int) element.getValue()[0];
      }
      return 0;
    }
  }

}
