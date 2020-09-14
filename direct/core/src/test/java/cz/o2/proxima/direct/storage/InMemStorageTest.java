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
package cz.o2.proxima.direct.storage;

import static org.junit.Assert.*;

import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.InMemStorage.ConsumedOffset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Optionals;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/** Test suite for {@link InMemStorage}. */
public class InMemStorageTest implements Serializable {

  final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  final EntityDescriptor entity =
      repo.findEntity("dummy").orElseThrow(() -> new IllegalStateException("Missing entity dummy"));
  final AttributeDescriptor<?> data =
      entity
          .findAttribute("data")
          .orElseThrow(() -> new IllegalStateException("Missing attribute data"));

  @Test(timeout = 10000)
  public void testObservePartitions() throws InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, entity, URI.create("inmem:///inmemstoragetest"), Collections.emptyMap());
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    AttributeWriterBase writer =
        accessor
            .getWriter(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing writer"));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    ObserveHandle handle =
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
  public void testObservePartionsWithSamePath() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(direct, entity, URI.create("inmem://test1"), Collections.emptyMap());
    DataAccessor accessor2 =
        storage.createAccessor(direct, entity, URI.create("inmem://test2"), Collections.emptyMap());
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
            direct, entity, URI.create("inmem:///inmemstoragetest"), Collections.emptyMap());
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
        Arrays.asList(data),
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
        storage.createAccessor(direct, entity, URI.create("inmem://test1"), Collections.emptyMap());
    DataAccessor accessor2 =
        storage.createAccessor(direct, entity, URI.create("inmem://test2"), Collections.emptyMap());
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
        Arrays.asList(data),
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
            direct, entity, URI.create("inmem:///inmemstoragetest"), Collections.emptyMap());
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
    assertEquals(Arrays.asList((byte) 1), received);
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
    assertEquals(Arrays.asList((byte) 1), received);
  }

  @Test(timeout = 10000)
  public void testObserveOffsets() {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, entity, URI.create("inmem:///inmemstoragetest"), Collections.emptyMap());
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
            assertEquals("key", ingest.getKey());
            context.confirm();
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
    assertEquals(Arrays.asList((byte) 1, (byte) 2), received);
    assertEquals(
        2, ((ConsumedOffset) handle.getCurrentOffsets().get(0)).getConsumedKeyAttr().size());
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
    DataAccessor accessor = storage.createAccessor(direct, entity, uri, Collections.emptyMap());
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
    final DataAccessor accessor =
        storage.createAccessor(direct, entity, uri, Collections.emptyMap());
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
}
