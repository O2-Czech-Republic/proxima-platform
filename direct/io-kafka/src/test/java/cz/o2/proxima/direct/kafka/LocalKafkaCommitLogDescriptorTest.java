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
package cz.o2.proxima.direct.kafka;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.direct.commitlog.RetryableLogObserver;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.direct.kafka.LocalKafkaCommitLogDescriptor.Accessor;
import cz.o2.proxima.direct.kafka.LocalKafkaCommitLogDescriptor.LocalKafkaWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.functional.UnaryFunction;

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
      .build();
  private final DirectDataOperator direct = repo.asDataOperator(
      DirectDataOperator.class, op -> op.withExecutorFactory(serviceFactory));

  private final AttributeDescriptorBase<byte[]> attr;
  private final AttributeDescriptorBase<byte[]> attrWildcard;
  private final EntityDescriptor entity;
  private final URI storageUri;

  private LocalKafkaCommitLogDescriptor kafka;

  public LocalKafkaCommitLogDescriptorTest() throws Exception {
    this.attr = AttributeDescriptor
        .newBuilder(repo)
        .setEntity("entity")
        .setName("attr")
        .setSchemeUri(new URI("bytes:///"))
        .build();


    this.attrWildcard = AttributeDescriptor
        .newBuilder(repo)
        .setEntity("entity")
        .setName("wildcard.*")
        .setSchemeUri(new URI("bytes:///"))
        .build();

    this.entity = EntityDescriptor.newBuilder()
        .setName("entity")
        .addAttribute(attr)
        .addAttribute(attrWildcard)
        .build();

    this.storageUri = new URI("kafka-test://dummy/topic");
  }

  @Before
  public void setUp() {
    kafka = new LocalKafkaCommitLogDescriptor();
  }

  @Test(timeout = 10000)
  public void testSinglePartitionWriteAndConsumeBySingleConsumerRunAfterWrite()
      throws InterruptedException {

    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(1));
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

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndConsumeBySingleConsumerRunAfterWrite()
      throws InterruptedException {

    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<String, byte[]> consumer;
    final CountDownLatch latch = new CountDownLatch(2);

    consumer = accessor.createConsumerFactory().create();
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
    accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(2));
    KafkaConsumer<String, byte[]> consumer = accessor.createConsumerFactory().create();
    assertTrue(consumer.poll(100).isEmpty());
  }

  @Test
  public void testWriteNull() {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(2));
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
      } else if (r.timestamp() == now + 1000) {
        assertNull(r.value());
        matched++;
      }
    }
    assertEquals(2, matched);
  }

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndConsumeBySingleConsumerRunBeforeWrite()
      throws InterruptedException {

    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(2));
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

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndTwoReads()
      throws InterruptedException {

    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<String, byte[]> consumer;

    consumer = accessor.createConsumerFactory().create();
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

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void testTwoIdependentConsumers() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(1));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<String, byte[]>[] consumers = new KafkaConsumer[] {
      accessor.createConsumerFactory().create("dummy1"),
      accessor.createConsumerFactory().create("dummy2"),
    };
    final CountDownLatch latch = new CountDownLatch(1);
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
      TopicPartition partition = Iterators.getOnlyElement(
          polled.partitions().iterator());
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

  @Test(timeout = 10000)
  public void testManualPartitionAssignment() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<String, byte[]> consumer = accessor
        .createConsumerFactory().create(Arrays.asList((Partition) () -> 0));
    final CountDownLatch latch = new CountDownLatch(2);

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

  @Test(timeout = 10000)
  public void testPollAfterWrite() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(1));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CountDownLatch latch = new CountDownLatch(2);
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

  @Test(timeout = 10000)
  public void testPollWithSeek() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(1));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CountDownLatch latch = new CountDownLatch(2);
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
    final String name = "consumer";
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<String, byte[]> c1 = accessor.createConsumerFactory()
        .create(name);

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> { });

    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(2, c1.assignment().size());
    assertEquals(1, poll.count());

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> { });

    poll = c1.poll(1000);
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {
      {
        put(new TopicPartition("topic", 0), new OffsetAndMetadata(1));
        put(new TopicPartition("topic", 1), new OffsetAndMetadata(1));
      }
    });

    // create another consumer
    KafkaConsumer<String, byte[]> c2 = accessor.createConsumerFactory().create(name);

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> { });

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
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(1));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<String, byte[]> c1 = accessor.createConsumerFactory().create(name);
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> { });

    ConsumerRecords<String, byte[]> poll = c1.poll(1000);
    assertEquals(1, c1.assignment().size());
    assertEquals(1, poll.count());

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> { });

    poll = c1.poll(1000);
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {
      {
        put(new TopicPartition("topic", 0), new OffsetAndMetadata(2));
      }
    });

    // create another consumer
    KafkaConsumer<String, byte[]> c2 = accessor.createConsumerFactory().create(name);

    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key2", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> { });

    poll = c2.poll(1000);
    assertEquals(1, poll.count());
    poll = c1.poll(1000);
    assertTrue(poll.isEmpty());

    // not rebalanced (there are no free partitions)
    assertEquals(0, c1.assignment().size());
    assertEquals(1, c2.assignment().size());

  }

  @Test(timeout = 10000)
  public void testObserveSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    final ObserveHandle handle = reader.observe(
        "test", Position.NEWEST,
        new LogObserver() {

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
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
            throw new RuntimeException(error);
          }

        });

    writer.write(update, (succ, e) -> {
      assertTrue(succ);
      latch.countDown();
    });
    latch.await();
    assertEquals(3, handle.getCommittedOffsets().size());
    long sum = handle.getCommittedOffsets()
        .stream()
        .mapToLong(o -> {
          TopicOffset tpo = (TopicOffset) o;
          assertTrue(tpo.getOffset() <= 1);
          return tpo.getOffset();
        })
        .sum();

    // single partition has committed one element
    assertEquals(1, sum);
  }

  @Test(timeout = 10000)
  public void testObserveMovesWatermark() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    long now = System.currentTimeMillis();
    final UnaryFunction<Integer, StreamElement> update = pos -> StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key" + pos, attr.getName(), now + pos, new byte[] { 1, 2 });

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(100);
    reader.observe("test", Position.NEWEST, new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        watermark.set(context.getWatermark());
        latch.countDown();
        return true;
      }

      @Override
      public void onCompleted() {
        fail("This should not be called");
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    }).waitUntilReady();

    for (int i = 0; i < 100; i++) {
      writer.write(update.apply(i), (succ, e) -> { });
    }

    latch.await();

    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testEmptyPollMovesWatermark() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    long now = System.currentTimeMillis();
    final StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), now + 2000, new byte[] { 1, 2 });

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe("test", Position.NEWEST, new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        watermark.set(context.getWatermark());
        latch.countDown();
        return true;
      }

      @Override
      public void onCompleted() {
        fail("This should not be called");
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    }).waitUntilReady();

    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);

    // then we write single element
    writer.write(update, (succ, e) -> { });

    latch.await();

    // watermark should be moved
    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }


  @Test(timeout = 10000)
  public void testObserveWithException() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    final ObserveHandle handle = reader.observe(
        "test", Position.NEWEST,
        new LogObserver() {

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
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


  @Test(timeout = 10000)
  public void testBulkObserveWithException() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    final ObserveHandle handle = reader.observeBulk(
        "test", Position.NEWEST,
        new LogObserver() {

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
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

  @Test(timeout = 10000)
  public void testBulkObserveWithExceptionAndRetry() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger restarts = new AtomicInteger();
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    RetryableLogObserver observer = RetryableLogObserver.bulk(
        3, "test", reader, new LogObserver() {

          @Override
          public boolean onError(Throwable error) {
            latch.countDown();
            return false;
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext confirm) {
            restarts.incrementAndGet();
            throw new RuntimeException("FAIL!");
          }

        });
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


  @Test(timeout = 10000)
  public void testBulkObserveSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final AtomicReference<StreamElement> input = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    final ObserveHandle handle = reader.observeBulk(
        "test", Position.NEWEST,
        new LogObserver() {

          @Override
          public void onRepartition(OnRepartitionContext context) {
            restarts.incrementAndGet();
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
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

  @Test(timeout = 10000)
  public void testBulkObservePartitionsSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
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

    final ObserveHandle handle = reader.observeBulkPartitions(
        reader.getPartitions(),
        Position.NEWEST, new LogObserver() {

          @Override
          public void onRepartition(OnRepartitionContext context) {
            restarts.incrementAndGet();
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
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

  @Test(timeout = 10000)
  public void testBulkObserveOffsets() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CommitLogReader reader = accessor.getCommitLogReader(context()).orElseThrow(
        () -> new IllegalStateException("Missing commit log reader"));

    final List<KafkaStreamElement> input = new ArrayList<>();
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(
        new CountDownLatch(3));
    final StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 });

    final Map<Integer, Offset> currentOffsets = new HashMap<>();

    final LogObserver observer = new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        input.add((KafkaStreamElement) ingest);
        context.confirm();
        latch.get().countDown();
        // terminate after reading first record
        return false;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    };
    final ObserveHandle handle = reader.observeBulkPartitions(
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

    handle.getCommittedOffsets().forEach(o ->
        currentOffsets.put(o.getPartition().getId(), o));
    handle.cancel();

    // each partitions has a record here
    assertEquals(3, currentOffsets.size());
    assertEquals(1L, (long) currentOffsets.values()
        .stream()
        .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));

    // restart from old offset
    final ObserveHandle handle2 = reader.observeBulkOffsets(
        Lists.newArrayList(currentOffsets.values()), observer);
    latch.get().await();
    assertEquals(2, input.size());
    assertEquals(0, input.get(0).getOffset());
    assertEquals(1, input.get(1).getOffset());
    // committed offset 1 and 2
    assertEquals(2L, (long) handle2.getCommittedOffsets()
        .stream()
        .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));
  }

  @Test(timeout = 10000)
  public void testCachedView() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(
        new CountDownLatch(1));
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

  @Test(timeout = 10000)
  public void testCachedViewReload() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CachedView view = accessor.getCachedView(context()).orElseThrow(
        () -> new IllegalStateException("Missing cached view"));
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(
        new CountDownLatch(2));
    final List<StreamElement> updates = Arrays.asList(
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

  @Test(timeout = 10000)
  public void testCachedViewWrite() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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

  @Test(timeout = 10000)
  public void testCachedViewWriteAndDelete() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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


  @Test(timeout = 10000)
  public void testCachedViewWriteAndDeleteWildcard() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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
    assertTrue(view.get("key1", "wildcard.1", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard, now + 500).isPresent());
    assertArrayEquals(
        new byte[] { 2, 3 },
        view.get("key1", "wildcard.1", attrWildcard, now + 500).get().getValue());
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()));
    assertTrue(view.get("key1", "wildcard.1", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard, now + 500).isPresent());
    assertArrayEquals(
        new byte[] { 2, 3 },
        view.get("key1", "wildcard.1", attrWildcard, now + 500).get().getValue());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndList() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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

  @Test(timeout = 10000)
  public void testCachedViewWriteAndListAll() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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
      assertTrue("Exception: " + exc, succ);
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

  @Test(timeout = 10000)
  public void testCachedViewWritePreUpdate() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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

  @Test(timeout = 10000)
  public void testCachedViewWritePreUpdateAndDeleteWildcard()
      throws InterruptedException {

    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, KeyPartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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
      assertTrue("Ex1ception: " + exc, succ);
      latch.countDown();
    }));
    latch.await();
    AtomicInteger calls = new AtomicInteger();
    view.assign(IntStream.range(0, 3)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList()),
        (e, c) -> calls.incrementAndGet());
    assertEquals(3, calls.get());
  }

  @Test
  public void testRewriteAndPrefetch() throws InterruptedException, IOException {
    Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, partitionsCfg(3, KeyPartitioner.class));
    CachedView view = accessor.getCachedView(context()).orElseThrow(
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

  @Test(timeout = 5000)
  public void testMaxBytesPerSec() throws InterruptedException {
    long maxLatency = testSequentialConsumption(3);
    assertTrue(maxLatency > 500000000L);
  }

  @Test(timeout = 5000)
  public void testNoMaxBytesPerSec() throws InterruptedException {
    long maxLatency = testSequentialConsumption(Long.MAX_VALUE);
    assertTrue(maxLatency < 500000000L);
  }


  private long testSequentialConsumption(
      long maxBytesPerSec)
      throws InterruptedException {

    final Accessor accessor = kafka.createAccessor(
        direct, entity, storageUri, cfg(Pair.of(
            KafkaAccessor.MAX_BYTES_PER_SEC, maxBytesPerSec)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(
        new CountDownLatch(1));
    CommitLogReader reader = accessor.getCommitLogReader(context())
        .orElseThrow(() -> new IllegalStateException("Missing log reader"));
    final AtomicLong lastOnNext = new AtomicLong(System.nanoTime());
    final AtomicLong maxLatency = new AtomicLong(0);

    reader.observe("dummy", new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        latch.getAndSet(new CountDownLatch(1)).countDown();
        long now = System.nanoTime();
        long last = lastOnNext.getAndSet(now);
        long latency = now - last;
        maxLatency.getAndUpdate(old -> Math.max(old, latency));
        return true;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });
    CountDownLatch toWait = latch.get();
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });
    toWait.await();
    toWait = latch.get();
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key1", attr.getName(),
        System.currentTimeMillis(), emptyValue()), (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });

    toWait.await();
    return maxLatency.get();
  }

  private Context context() {
    return direct.getContext();
  }

  private static Map<String, Object> partitionsCfg(int partitions) {
    return partitionsCfg(partitions, null);
  }

  private static Map<String, Object> partitionsCfg(
      int partitions,
      @Nullable Class<? extends Partitioner> partitioner) {

    return cfg(Pair.of(
        LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS, String.valueOf(partitions)),
        partitioner != null
            ? Pair.of(KafkaAccessor.PARTITIONER_CLASS, partitioner.getName())
            : null);
  }

  @SafeVarargs
  private static Map<String, Object> cfg(
      Pair<String, Object>... pairs) {

    return Arrays.stream(pairs)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private static byte[] emptyValue() {
    return new byte[] { };
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
