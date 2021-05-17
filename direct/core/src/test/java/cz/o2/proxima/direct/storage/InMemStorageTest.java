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
package cz.o2.proxima.direct.storage;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserverUtils;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.InMemStorage.ConsumedOffset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.SerializationException;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.KeyAttributePartitioner;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.storage.commitlog.Partitioners;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Optionals;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

/** Test suite for {@link InMemStorage}. */
public class InMemStorageTest implements Serializable {

  final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  final EntityDescriptor entity = repo.getEntity("dummy");
  final AttributeDescriptor<?> data = entity.getAttribute("data");

  @Test(timeout = 10000)
  public void testObservePartitions() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    reader.observePartitions(
        reader.getPartitions(),
        new LogObserver() {

          @Override
          public void onRepartition(OnRepartitionContext context) {
            assertEquals(1, context.partitions().size());
            latch.set(new CountDownLatch(1));
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {

            assertEquals(0, context.getPartition().getId());
            assertEquals("key", ingest.getKey());
            context.confirm();
            latch.get().countDown();
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });

    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    latch.get().await();
  }

  @Test(timeout = 10000)
  public void testObservePartitionsWithSamePath() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test1")));
    DataAccessor accessor2 =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test2")));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing batch log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));
    CountDownLatch latch = new CountDownLatch(1);
    StreamElement element =
        StreamElement.upsert(
            entity,
            data,
            UUID.randomUUID().toString(),
            "key",
            data.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    writer.online().write(element, (succ, exc) -> {});
    accessor2
        .getWriter(direct.getContext())
        .orElseThrow(() -> new IllegalStateException("Missing writer2"))
        .online()
        .write(element, (succ, exc) -> {});
    AtomicInteger count = new AtomicInteger();
    reader.observePartitions(
        reader.getPartitions(),
        Position.OLDEST,
        true,
        new LogObserver() {

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals("key", ingest.getKey());
            context.confirm();
            count.incrementAndGet();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
    assertEquals(1, count.get());
  }

  @Test(timeout = 10000)
  public void testObserveBatch() throws InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    BatchLogReader reader =
        accessor
            .getBatchLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing batch log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));
    CountDownLatch latch = new CountDownLatch(1);
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(data),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals(0, context.getPartition().getId());
            assertEquals("key", ingest.getKey());
            latch.countDown();
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
  }

  @Test(timeout = 10000)
  public void testObserveBatchWithSamePath() throws InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test1")));
    DataAccessor accessor2 =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test2")));
    BatchLogReader reader =
        accessor
            .getBatchLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing batch log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));
    CountDownLatch latch = new CountDownLatch(1);
    StreamElement element =
        StreamElement.upsert(
            entity,
            data,
            UUID.randomUUID().toString(),
            "key",
            data.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    writer.online().write(element, (succ, exc) -> {});
    accessor2
        .getWriter(direct.getContext())
        .orElseThrow(() -> new IllegalStateException("Missing writer2"))
        .online()
        .write(element, (succ, exc) -> {});
    AtomicInteger count = new AtomicInteger();
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(data),
        new BatchLogObserver() {

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals(0, context.getPartition().getId());
            assertEquals("key", ingest.getKey());
            count.incrementAndGet();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
    assertEquals(1, count.get());
  }

  @Test(timeout = 10000)
  public void testObserveCancel() {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));
    List<Byte> received = new ArrayList<>();
    ObserveHandle handle =
        reader.observePartitions(
            reader.getPartitions(),
            new LogObserver() {

              @Override
              public void onRepartition(LogObserver.OnRepartitionContext context) {
                assertEquals(1, context.partitions().size());
              }

              @Override
              public boolean onNext(StreamElement ingest, LogObserver.OnNextContext context) {

                assertEquals(0, context.getPartition().getId());
                assertEquals("key", ingest.getKey());
                context.confirm();
                received.add(ingest.getValue()[0]);
                return false;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });

    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1}),
            (succ, exc) -> {});
    List<Offset> offsets = handle.getCurrentOffsets();
    assertEquals(1, offsets.size());
    assertEquals(Collections.singletonList((byte) 1), received);
    handle.close();
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {2}),
            (succ, exc) -> {});
    assertEquals(Collections.singletonList((byte) 1), received);
  }

  @Test(timeout = 10000)
  public void testObserveOffsets() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));
    List<Byte> received = new ArrayList<>();
    LogObserver observer =
        new LogObserver() {

          @Override
          public void onRepartition(LogObserver.OnRepartitionContext context) {
            assertEquals(1, context.partitions().size());
          }

          @Override
          public boolean onNext(StreamElement ingest, LogObserver.OnNextContext context) {
            assertEquals(0, context.getPartition().getId());
            received.add(ingest.getValue()[0]);
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };

    ObserveHandle handle = reader.observePartitions(reader.getPartitions(), observer);

    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1}),
            (succ, exc) -> {});

    List<Offset> offsets = handle.getCurrentOffsets();
    assertEquals(1, offsets.size());
    assertTrue(offsets.get(0).getWatermark() > 0);
    assertEquals(Collections.singletonList((byte) 1), received);
    handle.close();
    handle = reader.observeBulkOffsets(offsets, observer);
    handle.waitUntilReady();
    offsets = handle.getCurrentOffsets();
    assertEquals(1, offsets.size());
    assertTrue(
        "Expected positive watermark, got " + offsets.get(0).getWatermark(),
        offsets.get(0).getWatermark() > 0);
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {2}),
            (succ, exc) -> {});

    assertEquals(Arrays.asList((byte) 1, (byte) 1), received);
    assertEquals(
        0, ((ConsumedOffset) handle.getCurrentOffsets().get(0)).getConsumedKeyAttr().size());
    handle.close();
  }

  @Test
  public void testObserveWithEndOfTime() throws InterruptedException {
    URI uri = URI.create("inmem:///inmemstoragetest");
    InMemStorage storage = new InMemStorage();
    InMemStorage.setWatermarkEstimatorFactory(
        uri,
        (stamp, name, offset) ->
            new WatermarkEstimator() {

              {
                Preconditions.checkArgument(offset != null);
              }

              @Override
              public long getWatermark() {
                return Watermarks.MAX_WATERMARK - InMemStorage.getBoundedOutOfOrderness();
              }

              @Override
              public void update(StreamElement element) {}

              @Override
              public void setMinWatermark(long minWatermark) {}
            });
    DataAccessor accessor = storage.createAccessor(direct, createFamilyDescriptor(uri));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    CountDownLatch completed = new CountDownLatch(1);
    reader.observe(
        "observer",
        new LogObserver() {

          @Override
          public void onCompleted() {
            completed.countDown();
          }

          @Override
          public boolean onError(Throwable error) {
            return false;
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            return false;
          }
        });
    assertTrue(completed.await(1, TimeUnit.SECONDS));
  }

  @Test(timeout = 1000L)
  public void testObserveError() throws InterruptedException {
    final URI uri = URI.create("inmem:///inmemstoragetest");
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor = storage.createAccessor(direct, createFamilyDescriptor(uri));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));

    final AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    final CountDownLatch failingObserverErrorReceived = new CountDownLatch(1);
    final AtomicInteger failingObserverMessages = new AtomicInteger(0);
    reader.observe(
        "failing-observer",
        new LogObserver() {

          @Override
          public void onCompleted() {
            throw new UnsupportedOperationException("This should never happen.");
          }

          @Override
          public boolean onError(Throwable error) {
            failingObserverErrorReceived.countDown();
            return false;
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            failingObserverMessages.incrementAndGet();
            throw new RuntimeException("Test exception.");
          }
        });

    final int numElements = 100;
    final CountDownLatch successObserverAllReceived = new CountDownLatch(numElements);
    reader.observe(
        "success-observer",
        new LogObserver() {

          @Override
          public void onCompleted() {
            throw new UnsupportedOperationException("This should never happen.");
          }

          @Override
          public boolean onError(Throwable error) {
            return false;
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            successObserverAllReceived.countDown();
            return true;
          }
        });

    for (int i = 0; i < numElements; i++) {
      writer
          .online()
          .write(
              StreamElement.upsert(
                  entity,
                  data,
                  UUID.randomUUID().toString(),
                  "key_" + i,
                  data.getName(),
                  System.currentTimeMillis(),
                  new byte[] {2}),
              (success, error) -> {});
    }
    failingObserverErrorReceived.await();
    successObserverAllReceived.await();
    assertEquals(1, failingObserverMessages.get());
  }

  @Test
  public void testObserveMultiplePartitions() throws InterruptedException {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    final AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    final int numElements = 1_000;
    final ConcurrentMap<Partition, Long> partitionHistogram = new ConcurrentHashMap<>();
    final CountDownLatch elementsReceived = new CountDownLatch(numElements);
    // Start observer.
    final ObserveHandle observeHandle =
        reader.observePartitions(
            reader.getPartitions(),
            new LogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                assertEquals(numPartitions, context.partitions().size());
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                partitionHistogram.merge(context.getPartition(), 1L, Long::sum);
                context.confirm();
                elementsReceived.countDown();
                return elementsReceived.getCount() > 0;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });
    // Write data.
    for (int i = 0; i < numElements; i++) {
      writer
          .online()
          .write(
              StreamElement.upsert(
                  entity,
                  data,
                  UUID.randomUUID().toString(),
                  "key_" + i,
                  data.getName(),
                  System.currentTimeMillis(),
                  new byte[] {1, 2, 3}),
              CommitCallback.noop());
    }
    // Wait for all elements to be received.
    elementsReceived.await();

    assertEquals(3, partitionHistogram.size());
    assertEquals(3, observeHandle.getCurrentOffsets().size());
  }

  @Test
  public void testObserveSinglePartitionOutOfMultiplePartitions() throws InterruptedException {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    final AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    final int numElements = 999;
    final ConcurrentMap<Partition, Long> partitionHistogram = new ConcurrentHashMap<>();
    // Elements are uniformly distributed between partitions.
    final CountDownLatch elementsReceived = new CountDownLatch(numElements / numPartitions);
    // Start observer.
    final List<Partition> consumedPartitions = reader.getPartitions().subList(0, 1);
    final ObserveHandle observeHandle =
        reader.observePartitions(
            reader.getPartitions().subList(0, 1),
            new LogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                assertEquals(numPartitions, context.partitions().size());
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                partitionHistogram.merge(context.getPartition(), 1L, Long::sum);
                context.confirm();
                elementsReceived.countDown();
                return elementsReceived.getCount() > 0;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });
    // Write data.
    final Partitioner partitioner = new KeyAttributePartitioner();
    final Map<Partition, Long> expectedPartitionHistogram = new HashMap<>();
    for (int i = 0; i < numElements; i++) {
      final StreamElement element =
          StreamElement.upsert(
              entity,
              data,
              UUID.randomUUID().toString(),
              "key_" + i,
              data.getName(),
              System.currentTimeMillis(),
              new byte[] {1, 2, 3});
      expectedPartitionHistogram.merge(
          Partition.of(Partitioners.getTruncatedPartitionId(partitioner, element, numPartitions)),
          1L,
          Long::sum);
      writer.online().write(element, CommitCallback.noop());
    }
    assertEquals(3, expectedPartitionHistogram.size());

    // Wait for all elements to be received.
    elementsReceived.await();

    assertEquals(1, partitionHistogram.size());
    assertEquals(1, observeHandle.getCurrentOffsets().size());
    assertEquals(
        expectedPartitionHistogram.get(Iterables.getOnlyElement(consumedPartitions)),
        partitionHistogram.get(Iterables.getOnlyElement(consumedPartitions)));
  }

  @Test
  public void testRandomAccessReaderWithMultiplePartitions() {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    assertFalse(accessor.getRandomAccessReader(direct.getContext()).isPresent());
  }

  @Test
  public void testBatchLogReaderWithMultiplePartitions() {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    assertFalse(accessor.getBatchLogReader(direct.getContext()).isPresent());
  }

  @Test
  public void testCachedViewWithMultiplePartitions() {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    assertFalse(accessor.getCachedView(direct.getContext()).isPresent());
  }

  @Test(timeout = 10000)
  public void testReadWriteSequentialIds() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));

    List<StreamElement> result = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    LogObserver observer =
        LogObserverUtils.toList(
            result,
            Assert::assertTrue,
            elem -> {
              latch.countDown();
              return false;
            });
    reader.observe("test", observer);
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                1L,
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    latch.await();
    assertEquals(1, result.size());
    assertTrue(result.get(0).hasSequentialId());
    assertEquals(1L, result.get(0).getSequentialId());
  }

  @Test
  public void testConsumedOffsetExternalizerToJson() throws JsonProcessingException {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();

    String json =
        externalizer.toJson(
            new ConsumedOffset(Partition.of(1), new HashSet<>(Arrays.asList("a", "b")), 1000L));

    HashMap<String, Object> jsonObject =
        new ObjectMapper().readValue(json, new TypeReference<HashMap<String, Object>>() {});

    assertEquals(1, jsonObject.get("partition"));
    assertEquals(Arrays.asList("a", "b"), jsonObject.get("offset"));
    assertEquals(1000, jsonObject.get("watermark"));
  }

  @Test
  public void testConsumedOffsetExternalizerFromJson() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    ConsumedOffset consumedOffset =
        new ConsumedOffset(Partition.of(1), new HashSet<>(Arrays.asList("a", "b")), 1000L);

    assertEquals(consumedOffset, externalizer.fromJson(externalizer.toJson(consumedOffset)));
  }

  @Test
  public void testOffsetExternalizerFromBytesWhenInvalidJson() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    assertThrows(SerializationException.class, () -> externalizer.fromJson(""));
  }

  @Test
  public void testConsumedOffsetExternalizerFromBytes() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    ConsumedOffset consumedOffset =
        new ConsumedOffset(Partition.of(1), new HashSet<>(Arrays.asList("a", "b")), 1000L);

    assertEquals(consumedOffset, externalizer.fromBytes(externalizer.toBytes(consumedOffset)));
  }

  @Test
  public void testOffsetExternalizerFromBytesWhenInvalidBytes() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    assertThrows(SerializationException.class, () -> externalizer.fromBytes(new byte[] {0x0}));
  }

  private AttributeFamilyDescriptor createFamilyDescriptor(URI storageUri) {
    return createFamilyDescriptor(storageUri, 1);
  }

  private AttributeFamilyDescriptor createFamilyDescriptor(URI storageUri, int numPartitions) {
    final Map<String, Object> config = new HashMap<>();
    if (numPartitions > 1) {
      config.put(InMemStorage.NUM_PARTITIONS, 3);
    }
    return AttributeFamilyDescriptor.newBuilder()
        .setName("test")
        .setEntity(entity)
        .setType(StorageType.PRIMARY)
        .setAccess(AccessType.from("commit-log"))
        .setStorageUri(storageUri)
        .setCfg(config)
        .build();
  }
}
