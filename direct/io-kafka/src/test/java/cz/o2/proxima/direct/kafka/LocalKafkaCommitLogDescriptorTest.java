/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.commitlog.RetryableLogObserver;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.kafka.LocalKafkaCommitLogDescriptor.Accessor;
import cz.o2.proxima.direct.kafka.LocalKafkaCommitLogDescriptor.LocalKafkaLogReader;
import cz.o2.proxima.direct.kafka.LocalKafkaCommitLogDescriptor.LocalKafkaWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@code LocalKafkaCommitLogDescriptorTest}. */
@Slf4j
public class LocalKafkaCommitLogDescriptorTest implements Serializable {

  private final transient Factory<ExecutorService> serviceFactory =
      () ->
          Executors.newCachedThreadPool(
              r -> {
                Thread t = new Thread(r);
                t.setUncaughtExceptionHandler((thr, exc) -> exc.printStackTrace(System.err));
                return t;
              });
  private final transient Repository repo =
      ConfigRepository.Builder.ofTest(ConfigFactory.empty()).build();
  private final DirectDataOperator direct =
      repo.getOrCreateOperator(
          DirectDataOperator.class, op -> op.withExecutorFactory(serviceFactory));

  private final AttributeDescriptorBase<byte[]> attr;
  private final AttributeDescriptorBase<byte[]> attrWildcard;
  private final AttributeDescriptorBase<String> strAttr;
  private final EntityDescriptor entity;
  private final URI storageUri;

  private LocalKafkaCommitLogDescriptor kafka;

  public LocalKafkaCommitLogDescriptorTest() throws Exception {
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("attr")
            .setSchemeUri(new URI("bytes:///"))
            .build();

    this.attrWildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("wildcard.*")
            .setSchemeUri(new URI("bytes:///"))
            .build();

    this.strAttr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("strAttr")
            .setSchemeUri(new URI("string:///"))
            .build();

    this.entity =
        EntityDescriptor.newBuilder()
            .setName("entity")
            .addAttribute(attr)
            .addAttribute(attrWildcard)
            .addAttribute(strAttr)
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

    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(1));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
    assertEquals(0, partition.partition());
    assertEquals("topic", partition.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      assertEquals("key#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
      tested++;
    }
    assertEquals(1, tested);
  }

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndConsumeBySingleConsumerRunAfterWrite()
      throws InterruptedException {

    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> consumer;
    final CountDownLatch latch = new CountDownLatch(2);

    consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(2, polled.count());
    assertEquals(2, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());
    TopicPartition second = iterator.next();
    assertEquals(1, second.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
    }
    assertEquals(2, tested);
  }

  @Test
  public void testEmptyPoll() {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(2));
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    assertTrue(consumer.poll(Duration.ofMillis(100)).isEmpty());
  }

  @Test
  public void testWriteNull() {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(2));
    OnlineAttributeWriter writer = accessor.getWriter(context()).get().online();
    long now = 1234567890000L;
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "key", attr.getName(), now, new byte[] {1}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.delete(
            entity, attr, UUID.randomUUID().toString(), "key", attr.getName(), now + 1000),
        (succ, exc) -> {});
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(100));
    assertEquals(2, polled.count());
    int matched = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      if (r.timestamp() == now) {
        assertEquals(1, ((byte[]) r.value()).length);
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

    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(2));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(2, polled.count());
    assertEquals(2, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());
    TopicPartition second = iterator.next();
    assertEquals(1, second.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
    }
    assertEquals(2, tested);
  }

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndTwoReads() throws InterruptedException {

    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> consumer;

    consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });

    polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
  }

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void testTwoIdependentConsumers() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(1));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object>[] consumers =
        new KafkaConsumer[] {
          accessor.createConsumerFactory().create("dummy1"),
          accessor.createConsumerFactory().create("dummy2"),
        };
    final CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    for (KafkaConsumer<Object, Object> consumer : consumers) {
      ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
      assertEquals(1, polled.count());
      assertEquals(1, polled.partitions().size());
      TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
      assertEquals(0, partition.partition());
      assertEquals("topic", partition.topic());
      int tested = 0;
      for (ConsumerRecord<Object, Object> r : polled) {
        assertEquals("key#attr", r.key());
        assertEquals("topic", r.topic());
        assertArrayEquals(emptyValue(), (byte[]) r.value());
        tested++;
      }
      assertEquals(1, tested);
    }
  }

  @Test(timeout = 10000)
  public void testManualPartitionAssignment() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> consumer =
        accessor.createConsumerFactory().create(Arrays.asList((Partition) () -> 0));
    final CountDownLatch latch = new CountDownLatch(2);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
    }
    assertEquals(1, tested);
  }

  @Test(timeout = 10000)
  public void testPollAfterWrite() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(1));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CountDownLatch latch = new CountDownLatch(2);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    KafkaConsumer<Object, Object> consumer =
        accessor.createConsumerFactory().create(Arrays.asList((Partition) () -> 0));

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(100));
    assertTrue(polled.isEmpty());
  }

  @Test(timeout = 10000)
  public void testPollWithSeek() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(1));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CountDownLatch latch = new CountDownLatch(2);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    KafkaConsumer<Object, Object> consumer =
        accessor.createConsumerFactory().create(Arrays.asList((Partition) () -> 0));
    consumer.seek(new TopicPartition("topic", 0), 1);

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(100));
    assertEquals(1, polled.count());
  }

  @Test
  public void testTwoPartitionsTwoConsumersRebalance() {
    final String name = "consumer";
    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(2));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> c1 = accessor.createConsumerFactory().create(name);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    ConsumerRecords<Object, Object> poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(2, c1.assignment().size());
    assertEquals(1, poll.count());

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(
        new HashMap<TopicPartition, OffsetAndMetadata>() {
          {
            put(new TopicPartition("topic", 0), new OffsetAndMetadata(1));
            put(new TopicPartition("topic", 1), new OffsetAndMetadata(1));
          }
        });

    // create another consumer
    KafkaConsumer<Object, Object> c2 = accessor.createConsumerFactory().create(name);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c2.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());
    poll = c1.poll(Duration.ofMillis(1000));
    assertTrue(poll.isEmpty());

    // rebalanced
    assertEquals(1, c1.assignment().size());
    assertEquals(1, c2.assignment().size());
  }

  @Test
  public void testSinglePartitionTwoConsumersRebalance() {
    String name = "consumer";
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(1));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<Object, Object> c1 = accessor.createConsumerFactory().create(name);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    ConsumerRecords<Object, Object> poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(1, c1.assignment().size());
    assertEquals(1, poll.count());

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(
        new HashMap<TopicPartition, OffsetAndMetadata>() {
          {
            put(new TopicPartition("topic", 0), new OffsetAndMetadata(2));
          }
        });

    // create another consumer
    KafkaConsumer<Object, Object> c2 = accessor.createConsumerFactory().create(name);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c2.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());
    poll = c1.poll(Duration.ofMillis(1000));
    assertTrue(poll.isEmpty());

    // not rebalanced (there are no free partitions)
    assertEquals(0, c1.assignment().size());
    assertEquals(1, c2.assignment().size());
  }

  @Test(timeout = 10000)
  public void testObserveSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observe(
            "test",
            Position.NEWEST,
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

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertEquals(3, handle.getCommittedOffsets().size());
    long sum =
        handle
            .getCommittedOffsets()
            .stream()
            .mapToLong(
                o -> {
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
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    long now = System.currentTimeMillis();
    final UnaryFunction<Integer, StreamElement> update =
        pos ->
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key" + pos,
                attr.getName(),
                now + pos,
                new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(100);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new LogObserver() {

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
            })
        .waitUntilReady();

    for (int i = 0; i < 100; i++) {
      writer.write(update.apply(i), (succ, e) -> {});
    }

    latch.await();

    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testEmptyPollMovesWatermark() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000"))));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    long now = System.currentTimeMillis();
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            now + 2000,
            new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(2);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new LogObserver() {

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
            })
        .waitUntilReady();

    // then we write single element
    writer.write(update, (succ, e) -> {});

    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);

    // finally, last update to save watermark
    writer.write(update, (succ, e) -> {});

    latch.await();

    // watermark should be moved
    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testEmptyPollWithNoDataMovesWatermark() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000"))));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    final long now = System.currentTimeMillis();
    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(30);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new LogObserver() {

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
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

              @Override
              public void onIdle(OnIdleContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
              }
            })
        .waitUntilReady();

    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);

    latch.await();

    // watermark should be moved
    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testSlowPollMovesWatermarkSlowly() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000"))));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    final long now = System.currentTimeMillis();
    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(30);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new LogObserver() {

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
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

              @Override
              public void onIdle(OnIdleContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
              }
            })
        .waitUntilReady();

    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);

    latch.await();

    // watermark should be moved
    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testPollFromMoreConsumersThanPartitionsMovesWatermark() throws InterruptedException {

    Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            and(
                partitionsCfg(3),
                cfg(
                    Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000"),
                    Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, "1"))));
    int numObservers = 4;

    testPollFromNConsumersMovesWatermarkWithNoWrite(accessor, numObservers);
    writeData(accessor);
    testPollFromNConsumersMovesWatermark(accessor, numObservers);
  }

  @Test(timeout = 100_000)
  public void testPollFromManyMoreConsumersThanPartitionsMovesWatermark()
      throws InterruptedException {

    Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            and(
                partitionsCfg(3),
                cfg(
                    Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000"),
                    Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, "1"))));

    int numObservers = 400;
    testPollFromNConsumersMovesWatermarkWithNoWrite(accessor, numObservers);
    writeData(accessor);
    testPollFromNConsumersMovesWatermark(accessor, numObservers);
  }

  void writeData(Accessor accessor) {
    LocalKafkaWriter writer = accessor.newWriter();
    long now = 1500000000000L;
    writer.write(
        StreamElement.upsert(
            entity, attr, "key", UUID.randomUUID().toString(), attr.getName(), now, new byte[] {1}),
        (succ, exc) -> {});
  }

  void testPollFromNConsumersMovesWatermark(Accessor accessor, int numObservers)
      throws InterruptedException {
    testPollFromNConsumersMovesWatermark(accessor, numObservers, true);
  }

  void testPollFromNConsumersMovesWatermarkWithNoWrite(Accessor accessor, int numObservers)
      throws InterruptedException {
    testPollFromNConsumersMovesWatermark(accessor, numObservers, false);
  }

  void testPollFromNConsumersMovesWatermark(
      Accessor accessor, int numObservers, boolean expectMoved) throws InterruptedException {

    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    final long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(numObservers);
    Set<LogObserver> movedConsumers = Collections.synchronizedSet(new HashSet<>());
    Map<LogObserver, Long> observerWatermarks = new ConcurrentHashMap<>();
    AtomicInteger readyObservers = new AtomicInteger();
    for (int i = 0; i < numObservers; i++) {
      reader
          .observe(
              "test-" + expectMoved,
              Position.OLDEST,
              new LogObserver() {

                @Override
                public boolean onNext(StreamElement ingest, OnNextContext context) {
                  log.debug("Received element {} on watermark {}", ingest, context.getWatermark());
                  return true;
                }

                @Override
                public boolean onError(Throwable error) {
                  throw new RuntimeException(error);
                }

                @Override
                public void onIdle(OnIdleContext context) {
                  if (readyObservers.get() == numObservers) {
                    observerWatermarks.compute(
                        this,
                        (k, v) ->
                            Math.max(
                                MoreObjects.firstNonNull(v, Long.MIN_VALUE),
                                context.getWatermark()));
                    if ((!expectMoved || observerWatermarks.get(this) > 0)
                        && movedConsumers.add(this)) {
                      latch.countDown();
                    }
                  }
                }
              })
          .waitUntilReady();
      readyObservers.incrementAndGet();
    }

    latch.await();

    assertEquals(numObservers, observerWatermarks.size());
    long watermark = observerWatermarks.values().stream().min(Long::compare).orElse(Long.MIN_VALUE);

    // watermark should be moved
    assertTrue(!expectMoved || watermark > 0);
    assertTrue(
        "Watermark should not be too far, got "
            + watermark
            + " calculated from "
            + observerWatermarks,
        watermark < now * 10);
  }

  @Test(timeout = 10000)
  public void testObserveBulkCommitsCorrectly() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            cfg(
                Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, 1L),
                Pair.of(LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS, 3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      StreamElement update =
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "key-" + i,
              attr.getName(),
              now + 2000,
              new byte[] {1, 2});
      // then we write single element
      writer.write(update, (succ, e) -> {});
    }
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observeBulk(
            "test",
            Position.OLDEST,
            true,
            new LogObserver() {

              int processed = 0;

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                if (++processed == 100) {
                  context.confirm();
                }
                return true;
              }

              @Override
              public void onCompleted() {
                latch.countDown();
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });

    latch.await();

    long offsetSum =
        handle.getCommittedOffsets().stream().mapToLong(o -> ((TopicOffset) o).getOffset()).sum();

    assertEquals(100, offsetSum);

    KafkaConsumer<Object, Object> consumer =
        ((LocalKafkaCommitLogDescriptor.LocalKafkaLogReader) reader).getConsumer();
    String topic = accessor.getTopic();

    assertEquals(
        100,
        consumer
            .committed(
                handle
                    .getCommittedOffsets()
                    .stream()
                    .map(o -> new TopicPartition(topic, o.getPartition().getId()))
                    .collect(Collectors.toSet()))
            .values()
            .stream()
            .mapToLong(OffsetAndMetadata::offset)
            .sum());
  }

  @Test(timeout = 100000)
  public void testOnlineObserveWithRebalanceResetsOffsetCommitter() throws InterruptedException {
    int numWrites = 5;
    Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            cfg(
                Pair.of(LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS, 3),
                // poll single record to commit it in atomic way
                Pair.of(KafkaAccessor.MAX_POLL_RECORDS, 1)));
    final CountDownLatch latch = new CountDownLatch(numWrites);
    AtomicInteger consumed = new AtomicInteger();
    List<OnNextContext> unconfirmed = Collections.synchronizedList(new ArrayList<>());

    LogObserver observer =
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            switch (consumed.getAndIncrement()) {
              case 0:
                // we must confirm the first message to create a committed position
                context.confirm();
                break;
              case 2:
                throw new RuntimeException("Failing first consumer!");
              default:
                unconfirmed.add(context);
                break;
            }
            if (consumed.get() == numWrites) {
              unconfirmed.forEach(OnNextContext::confirm);
            }
            latch.countDown();
            return true;
          }

          @Override
          public void onCompleted() {}

          @Override
          public boolean onError(Throwable error) {
            return true;
          }
        };
    testOnlineObserveWithRebalanceResetsOffsetCommitterWithObserver(observer, accessor, numWrites);
    latch.await();
    assertEquals(
        "Invalid committed offests: " + accessor.committedOffsets,
        3,
        accessor.committedOffsets.values().stream().mapToInt(e -> e.get()).sum());
  }

  private void testOnlineObserveWithRebalanceResetsOffsetCommitterWithObserver(
      LogObserver observer, Accessor accessor, int numWrites) throws InterruptedException {

    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    AtomicInteger consumed = new AtomicInteger();
    List<OnNextContext> unconfirmed = Collections.synchronizedList(new ArrayList<>());

    // observe from two observers
    final ObserveHandle[] handles = {
      reader.observe("test", Position.NEWEST, observer),
      reader.observe("test", Position.NEWEST, observer)
    };

    for (int i = 0; i < numWrites; i++) {
      writer.write(update, (succ, e) -> assertTrue(succ));
    }
  }

  @Test(timeout = 10000)
  public void testObserveWithException() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observe(
            "test",
            Position.NEWEST,
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

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertEquals("FAIL!", exc.get().getMessage());
    assertEquals(1, restarts.get());
    assertEquals(3, handle.getCommittedOffsets().size());
    handle.getCurrentOffsets().forEach(o -> assertEquals(0, ((TopicOffset) o).getOffset()));
  }

  @Test(timeout = 10000)
  public void testBulkObserveWithException() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observeBulk(
            "test",
            Position.NEWEST,
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

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertEquals("FAIL!", exc.get().getMessage());
    assertEquals(1, restarts.get());
    assertEquals(3, handle.getCommittedOffsets().size());
    handle.getCurrentOffsets().forEach(o -> assertEquals(0, ((TopicOffset) o).getOffset()));
  }

  @Test(timeout = 10000)
  public void testBulkObserveWithExceptionAndRetry() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger restarts = new AtomicInteger();
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    RetryableLogObserver observer =
        RetryableLogObserver.bulk(
            3,
            "test",
            reader,
            new LogObserver() {

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
    Executors.newCachedThreadPool()
        .execute(
            () -> {
              while (true) {
                try {
                  TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ex) {
                  break;
                }
                writer.write(
                    update,
                    (succ, e) -> {
                      assertTrue(succ);
                    });
              }
            });
    latch.await();
    assertEquals(3, restarts.get());
  }

  @Test(timeout = 10000)
  public void testBulkObserveSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final AtomicReference<StreamElement> input = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observeBulk(
            "test",
            Position.NEWEST,
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

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertNull(exc.get());
    assertEquals(1, restarts.get());
    assertArrayEquals(update.getValue(), input.get().getValue());
    assertEquals(3, handle.getCommittedOffsets().size());
    assertEquals(
        1L,
        (long)
            (Long)
                handle
                    .getCommittedOffsets()
                    .stream()
                    .mapToLong(o -> ((TopicOffset) o).getOffset())
                    .sum());
  }

  @Test(timeout = 10000)
  public void testBulkObservePartitionsFromOldestSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    AtomicInteger consumed = new AtomicInteger();
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    for (int i = 0; i < 1000; i++) {
      writer.write(
          update,
          (succ, e) -> {
            assertTrue(succ);
          });
    }
    CountDownLatch latch = new CountDownLatch(1);
    reader.observeBulkPartitions(
        reader.getPartitions(),
        Position.OLDEST,
        true,
        new LogObserver() {

          @Override
          public void onRepartition(OnRepartitionContext context) {}

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            consumed.incrementAndGet();
            context.confirm();
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
            ;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });

    latch.await();
    assertEquals(1000, consumed.get());
  }

  @Test(timeout = 10000)
  public void testBulkObservePartitionsSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    AtomicInteger restarts = new AtomicInteger();
    AtomicReference<Throwable> exc = new AtomicReference<>();
    AtomicReference<StreamElement> input = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(2);
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observeBulkPartitions(
            reader.getPartitions(),
            Position.NEWEST,
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

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertNull(exc.get());
    assertEquals(1, restarts.get());
    assertArrayEquals(update.getValue(), input.get().getValue());
    assertEquals(3, handle.getCommittedOffsets().size());
    assertEquals(
        1L,
        (long)
            (Long)
                handle
                    .getCommittedOffsets()
                    .stream()
                    .mapToLong(o -> ((TopicOffset) o).getOffset())
                    .sum());
  }

  @Test(timeout = 10000)
  public void testBulkObservePartitionsResetOffsetsSuccess() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    AtomicInteger restarts = new AtomicInteger();
    AtomicReference<Throwable> exc = new AtomicReference<>();
    AtomicReference<StreamElement> input = new AtomicReference<>();
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(2));
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observePartitions(
            reader.getPartitions(),
            Position.NEWEST,
            new LogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                restarts.incrementAndGet();
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                input.set(ingest);
                context.confirm();
                latch.get().countDown();
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                exc.set(error);
                throw new RuntimeException(error);
              }
            });

    handle.waitUntilReady();
    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.get().countDown();
        });

    latch.get().await();
    latch.set(new CountDownLatch(1));
    handle.resetOffsets(
        reader
            .getPartitions()
            .stream()
            .map(p -> new TopicOffset(p.getId(), 0, Watermarks.MIN_WATERMARK))
            .collect(Collectors.toList()));
    latch.get().await();
    assertEquals(
        1L,
        (long)
            (Long)
                handle
                    .getCommittedOffsets()
                    .stream()
                    .mapToLong(o -> ((TopicOffset) o).getOffset())
                    .sum());
  }

  @Test
  public void testObserveOnNonExistingTopic() throws InterruptedException {
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    LocalKafkaLogReader reader = accessor.newReader(context());
    try {
      // need this to initialize the consumer
      assertNotNull(reader.getPartitions());
      reader.validateTopic(reader.getConsumer(), "non-existing-topic");
      fail("Should throw exception");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "Received null or empty partitions for topic [non-existing-topic]. "
              + "Please check that the topic exists and has at least one partition.",
          ex.getMessage());
      return;
    }
    fail("Should throw IllegalArgumentException");
  }

  @Test(timeout = 10000)
  public void testBulkObserveOffsets() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    final List<KafkaStreamElement> input = new ArrayList<>();
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(3));
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final Map<Integer, Offset> currentOffsets = new HashMap<>();

    final LogObserver observer =
        new LogObserver() {

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
    try (final ObserveHandle handle =
        reader.observeBulkPartitions(reader.getPartitions(), Position.NEWEST, observer)) {

      // write two elements
      for (int i = 0; i < 2; i++) {
        writer.write(
            update,
            (succ, e) -> {
              assertTrue(succ);
              latch.get().countDown();
            });
      }
      latch.get().await();
      latch.set(new CountDownLatch(1));

      handle.getCommittedOffsets().forEach(o -> currentOffsets.put(o.getPartition().getId(), o));
    }

    // each partitions has a record here
    assertEquals(3, currentOffsets.size());
    assertEquals(
        1L,
        (long)
            currentOffsets
                .values()
                .stream()
                .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));

    // restart from old offset
    final ObserveHandle handle2 =
        reader.observeBulkOffsets(Lists.newArrayList(currentOffsets.values()), observer);
    latch.get().await();
    assertEquals(2, input.size());
    assertEquals(0, input.get(0).getOffset());
    assertEquals(1, input.get(1).getOffset());
    // committed offset 1 and 2
    assertEquals(
        2L,
        (long)
            handle2
                .getCommittedOffsets()
                .stream()
                .collect(Collectors.summingLong(o -> ((TopicOffset) o).getOffset())));
  }

  @Test(timeout = 10000)
  public void testCachedView() throws InterruptedException {
    final Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    writer.write(
        update,
        (succ, exc) -> {
          assertTrue(succ);
          latch.get().countDown();
        });
    latch.get().await();
    latch.set(new CountDownLatch(1));
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertArrayEquals(new byte[] {1, 2}, view.get("key", attr).get().getValue());
    update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    writer.write(
        update,
        (succ, exc) -> {
          assertTrue(succ);
          latch.get().countDown();
        });
    latch.get().await();
    TimeUnit.SECONDS.sleep(1);
    assertArrayEquals(new byte[] {1, 2, 3}, view.get("key", attr).get().getValue());
  }

  @Test(timeout = 10000)
  public void testCachedViewReload() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(
            direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(2));
    final List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key2",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {2, 3}));
    updates.forEach(
        update ->
            writer.write(
                update,
                (succ, exc) -> {
                  assertTrue(succ);
                  latch.get().countDown();
                }));
    latch.get().await();
    latch.set(new CountDownLatch(1));
    view.assign(
        IntStream.range(1, 2).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertFalse(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    writer.write(
        update,
        (succ, exc) -> {
          assertTrue(succ);
          latch.get().countDown();
        });
    latch.get().await();
    TimeUnit.SECONDS.sleep(1);
    view.assign(
        IntStream.range(1, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 10000)
  public void testCachedViewWrite() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key2",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(2);
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndDelete() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.delete(
                entity, attr, UUID.randomUUID().toString(), "key1", attr.getName(), now));
    CountDownLatch latch = new CountDownLatch(2);
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    assertFalse(view.get("key1", attr).isPresent());
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertFalse(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndDeleteWildcard() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now - 500,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now + 500,
                new byte[] {2, 3}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.3",
                now - 500,
                new byte[] {3, 4}))
        .forEach(
            update ->
                view.write(
                    update,
                    (succ, exc) -> {
                      assertTrue("Exception: " + exc, succ);
                      latch.countDown();
                    }));
    latch.await();
    assertTrue(view.get("key1", "wildcard.1", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard, now + 500).isPresent());
    assertArrayEquals(
        new byte[] {2, 3},
        view.get("key1", "wildcard.1", attrWildcard, now + 500).get().getValue());
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertTrue(view.get("key1", "wildcard.1", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard, now + 500).isPresent());
    assertArrayEquals(
        new byte[] {2, 3},
        view.get("key1", "wildcard.1", attrWildcard, now + 500).get().getValue());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndList() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now - 500),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.3",
                now - 499,
                new byte[] {3, 4}))
        .forEach(
            update ->
                view.write(
                    update,
                    (succ, exc) -> {
                      assertTrue("Exception: ", succ);
                      latch.countDown();
                    }));
    latch.await();
    List<KeyValue<byte[]>> res = new ArrayList<>();
    view.scanWildcard("key1", attrWildcard, res::add);
    assertEquals(2, res.size());
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    res.clear();
    view.scanWildcard("key1", attrWildcard, res::add);
    assertEquals(2, res.size());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndListAll() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now - 2000,
                new byte[] {0}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now - 500),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.3",
                now - 499,
                new byte[] {3, 4}))
        .forEach(
            update ->
                view.write(
                    update,
                    (succ, exc) -> {
                      assertTrue("Exception: " + exc, succ);
                      latch.countDown();
                    }));
    latch.await();
    List<KeyValue<?>> res = new ArrayList<>();
    view.scanWildcardAll("key1", res::add);
    assertEquals(3, res.size());
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    res.clear();
    view.scanWildcardAll("key1", res::add);
    assertEquals(3, res.size());
  }

  @Test(timeout = 10000)
  public void testCachedViewWritePreUpdate() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key2",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    AtomicInteger calls = new AtomicInteger();
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()),
        (e, c) -> calls.incrementAndGet());
    assertEquals(2, calls.get());
  }

  @Test(timeout = 10000)
  public void testCachedViewWritePreUpdateAndDeleteWildcard() throws InterruptedException {

    Accessor accessor =
        kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3, KeyPartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now + 1000L),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now + 500L,
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Ex1ception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    AtomicInteger calls = new AtomicInteger();
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()),
        (e, c) -> calls.incrementAndGet());
    assertEquals(3, calls.get());
  }

  @Test(timeout = 10000)
  public void testRewriteAndPrefetch() throws InterruptedException, IOException {
    Accessor accessor =
        kafka.createAccessor(direct, entity, storageUri, partitionsCfg(3, KeyPartitioner.class));
    CachedView view =
        accessor
            .getCachedView(context())
            .orElseThrow(() -> new IllegalStateException("Missing cached view"));
    long now = System.currentTimeMillis();
    List<StreamElement> updates =
        Arrays.asList(
            // store first value
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now,
                new byte[] {1, 2}),
            // update the value at the same stamp
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now,
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertArrayEquals(new byte[] {2, 3}, view.get("key1", attr).get().getValue());
    view.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            now,
            new byte[] {3, 4}),
        (succ, exc) -> {
          assertTrue(succ);
        });
    assertArrayEquals(new byte[] {3, 4}, view.get("key1", attr).get().getValue());
    view.close();
    assertFalse(view.get("key1", attr).isPresent());
    view.assign(
        IntStream.range(0, 3).mapToObj(i -> (Partition) () -> i).collect(Collectors.toList()));
    assertArrayEquals(new byte[] {3, 4}, view.get("key1", attr).get().getValue());
  }

  @Test(timeout = 5000)
  public void testMaxBytesPerSec() throws InterruptedException {
    long maxLatency = testSequentialConsumption(3);
    long expectedNanos = TimeUnit.MILLISECONDS.toNanos(500);
    assertTrue(
        String.format("maxLatency should be greater than %d, got %d", expectedNanos, maxLatency),
        maxLatency > expectedNanos);
  }

  @Test(timeout = 5000)
  public void testNoMaxBytesPerSec() throws InterruptedException {
    long maxLatency = testSequentialConsumption(Long.MAX_VALUE);
    assertTrue(maxLatency < 500_000_000L);
  }

  @Test(timeout = 10000)
  public void testCustomElementSerializer() throws InterruptedException {
    kafka =
        new LocalKafkaCommitLogDescriptor(
            accessor ->
                new Accessor(
                    accessor,
                    Collections.singletonMap(
                        KafkaAccessor.SERIALIZER_CLASS, SingleAttrSerializer.class.getName())));
    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, partitionsCfg(1));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            entity,
            strAttr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            "this is test".getBytes()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
    assertEquals(0, partition.partition());
    assertEquals("topic", partition.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      assertEquals("key", r.key());
      assertEquals("topic", r.topic());
      assertEquals("this is test", r.value());
      tested++;
    }
    assertEquals(1, tested);
  }

  @Test(timeout = 10000)
  public void testCustomWatermarkEstimator() throws InterruptedException {
    Map<String, Object> cfg = partitionsCfg(3);
    cfg.put("watermark.estimator-factory", FixedWatermarkEstimatorFactory.class.getName());

    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, cfg);
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    long now = System.currentTimeMillis();
    final UnaryFunction<Integer, StreamElement> update =
        pos ->
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key" + pos,
                attr.getName(),
                now + pos,
                new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(100);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new LogObserver() {

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
            })
        .waitUntilReady();

    for (int i = 0; i < 100; i++) {
      writer.write(update.apply(i), (succ, e) -> {});
    }

    latch.await();

    assertEquals(FixedWatermarkEstimatorFactory.FIXED_WATERMARK, watermark.get());
  }

  @Test(timeout = 10000)
  public void testCustomIdlePolicy() throws InterruptedException {
    Map<String, Object> cfg =
        and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000")));
    cfg.put("watermark.idle-policy-factory", FixedWatermarkIdlePolicyFactory.class.getName());

    Accessor accessor = kafka.createAccessor(direct, entity, storageUri, cfg);
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));

    long now = System.currentTimeMillis();
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            now + 2000,
            new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(2);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new LogObserver() {

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
            })
        .waitUntilReady();

    // then we write single element
    writer.write(update, (succ, e) -> {});
    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);
    // finally, last update to save watermark
    writer.write(update, (succ, e) -> {});
    latch.await();

    assertEquals(FixedWatermarkIdlePolicyFactory.FIXED_IDLE_WATERMARK, watermark.get());
  }

  private long testSequentialConsumption(long maxBytesPerSec) throws InterruptedException {

    final Accessor accessor =
        kafka.createAccessor(
            direct,
            entity,
            storageUri,
            cfg(
                Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, 1L),
                Pair.of(KafkaAccessor.MAX_BYTES_PER_SEC, maxBytesPerSec),
                Pair.of(KafkaAccessor.MAX_POLL_RECORDS, 1)));
    final LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing log reader"));
    final AtomicLong lastOnNext = new AtomicLong(Long.MIN_VALUE);
    final AtomicLong maxLatency = new AtomicLong(0);
    final int numElements = 2;
    final CountDownLatch latch = new CountDownLatch(numElements);

    LogObserver observer =
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            long now = System.nanoTime();
            long last = lastOnNext.getAndSet(now);
            if (last > 0) {
              long latency = now - last;
              maxLatency.getAndUpdate(old -> Math.max(old, latency));
            }
            latch.countDown();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };
    reader.observe("dummy", Position.OLDEST, observer);
    for (int i = 0; i < numElements; i++) {
      writer.write(
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "key1",
              attr.getName(),
              System.currentTimeMillis(),
              emptyValue()),
          (succ, exc) -> {
            assertTrue(succ);
            assertNull(exc);
          });
    }
    latch.await();
    return maxLatency.get();
  }

  private Context context() {
    return direct.getContext();
  }

  private static Map<String, Object> partitionsCfg(int partitions) {
    return partitionsCfg(partitions, null);
  }

  private static Map<String, Object> partitionsCfg(
      int partitions, @Nullable Class<? extends Partitioner> partitioner) {

    return cfg(
        Pair.of(LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS, String.valueOf(partitions)),
        partitioner != null
            ? Pair.of(KafkaAccessor.PARTITIONER_CLASS, partitioner.getName())
            : null);
  }

  @SafeVarargs
  private static Map<String, Object> cfg(Pair<String, Object>... pairs) {

    return Arrays.stream(pairs)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private static Map<String, Object> and(Map<String, Object> left, Map<String, Object> right) {

    return Stream.concat(left.entrySet().stream(), right.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static byte[] emptyValue() {
    return new byte[] {};
  }

  public static final class FirstBytePartitioner implements Partitioner {
    @Override
    public int getPartitionId(StreamElement element) {
      if (!element.isDelete()) {
        return (int) element.getValue()[0];
      }
      return 0;
    }
  }

  public static final class SingleAttrSerializer implements ElementSerializer<String, String> {

    private AttributeDescriptor<?> attrDesc;

    @Override
    public void setup(EntityDescriptor entityDescriptor) {
      attrDesc =
          entityDescriptor
              .findAttribute("strAttr")
              .orElseThrow(() -> new IllegalStateException("Missing attribute 'strAttr'"));
    }

    @Nullable
    @Override
    public StreamElement read(ConsumerRecord<String, String> record, EntityDescriptor entityDesc) {
      return StreamElement.upsert(
          entityDesc,
          attrDesc,
          attrDesc.getName(),
          UUID.randomUUID().toString(),
          record.key(),
          record.timestamp(),
          record.value().getBytes());
    }

    @Override
    public Pair<String, String> write(StreamElement element) {
      return Pair.of(element.getKey(), (String) element.getParsed().get());
    }

    @Override
    public Serde<String> keySerde() {
      return Serdes.String();
    }

    @Override
    public Serde<String> valueSerde() {
      return Serdes.String();
    }
  }

  public static final class FixedWatermarkEstimatorFactory implements WatermarkEstimatorFactory {
    public static final long FIXED_WATERMARK = 333L;

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {
      return new WatermarkEstimator() {
        @Override
        public long getWatermark() {
          return FIXED_WATERMARK;
        }

        @Override
        public void setMinWatermark(long minWatermark) {}
      };
    }
  }

  public static final class FixedWatermarkIdlePolicyFactory implements WatermarkIdlePolicyFactory {
    public static final long FIXED_IDLE_WATERMARK = 555L;

    @Override
    public WatermarkIdlePolicy create(Map<String, Object> cfg) {
      return (WatermarkIdlePolicy) () -> FIXED_IDLE_WATERMARK;
    }
  }
}
