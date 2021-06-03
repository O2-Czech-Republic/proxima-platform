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
package cz.o2.proxima.server;

import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.CommitLogObserver.OnNextContext;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.storage.InMemStorage;
import cz.o2.proxima.functional.UnaryPredicate;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.util.Optionals;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link ReplicationController}. */
public class ReplicationControllerTest {

  final Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  final EntityDescriptor event = repo.getEntity("event");
  final AttributeDescriptor<byte[]> data = event.getAttribute("data");
  DirectDataOperator direct;
  ReplicationController controller;
  CompletableFuture<Void> future;
  CountDownLatch livenessLatch;
  long now;

  @Before
  public void setUp() {
    // We're not using gateway replicas in this test, so we need to set watermark close to
    // processing time, for the liveness check to pass.
    InMemStorage.setWatermarkEstimatorFactory(
        URI.create("inmem:///proxima_gateway"),
        (stamp, name, offset) ->
            new WatermarkEstimator() {
              @Override
              public long getWatermark() {
                return System.currentTimeMillis();
              }

              @Override
              public void setMinWatermark(long minWatermark) {
                // No-op.
              }
            });

    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    livenessLatch = new CountDownLatch(1);
    controller =
        new ReplicationController(repo) {
          @Override
          boolean checkLiveness() {
            if (super.checkLiveness()) {
              livenessLatch.countDown();
              return true;
            }
            return false;
          }
        };
    future = controller.runReplicationThreads();
    now = System.currentTimeMillis();
  }

  @After
  public void tearDown() {
    future.cancel(true);
    direct.close();
  }

  @Test(timeout = 5000)
  public void testSimpleEventReplication() throws InterruptedException {
    List<StreamElement> written = new ArrayList<>();
    final CommitLogReader reader = Optionals.get(direct.getCommitLogReader(data));
    final CommitLogObserver observer =
        controller.createOnlineObserver(
            "consumer",
            direct
                .getCommitLogReader(data)
                .orElseThrow(
                    () -> new IllegalArgumentException("Missing commit log reader for data")),
            Sets.newHashSet(data),
            new PassthroughFilter(),
            fakeOnlineWriter(written));
    try (ObserveHandle ignored = reader.observe("consumer", observer)) {
      writeEvent();
      assertEquals(1, written.size());
    }
    livenessLatch.await();
    assertEquals(1.0, Metrics.LIVENESS.getValue(), 0.0001);
  }

  @Test
  public void testSimpleEventReplicationWithFilter() {
    final List<StreamElement> written = new ArrayList<>();
    final CommitLogReader reader = Optionals.get(direct.getCommitLogReader(data));
    final CommitLogObserver observer =
        controller.createOnlineObserver(
            "consumer",
            direct
                .getCommitLogReader(data)
                .orElseThrow(
                    () -> new IllegalArgumentException("Missing commit log reader for data")),
            Sets.newHashSet(data),
            ingest -> false,
            fakeOnlineWriter(written));
    try (ObserveHandle ignored = reader.observe("consumer", observer)) {
      writeEvent();
      assertEquals(0, written.size());
    }
  }

  @Test
  public void testEventReplicationWithReadOfInvalidAttribute() {
    List<StreamElement> written = new ArrayList<>();
    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<byte[]> armed = gateway.getAttribute("armed");
    AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
    CommitLogObserver observer =
        controller.createOnlineObserver(
            "consumer",
            direct
                .getCommitLogReader(status)
                .orElseThrow(
                    () -> new IllegalArgumentException("Missing commit log reader for data")),
            Sets.newHashSet(status),
            new PassthroughFilter(),
            fakeOnlineWriter(written));
    AtomicInteger commits = new AtomicInteger();
    observer.onNext(
        getUpdate(gateway, armed, now),
        new OnNextContext() {
          @Override
          public OffsetCommitter committer() {
            return (success, error) -> commits.incrementAndGet();
          }

          @Override
          public Partition getPartition() {
            return null;
          }

          @Override
          public long getWatermark() {
            return 0;
          }

          @Override
          public Offset getOffset() {
            return null;
          }
        });
    assertEquals(1, commits.get());
    assertEquals(0, written.size());
  }

  @Test
  public void testEventReplicationWithReadOfInvalidAttributeBulk() {
    List<StreamElement> written = new ArrayList<>();
    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<byte[]> armed = gateway.getAttribute("armed");
    AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
    CommitLogObserver observer =
        controller.createBulkObserver(
            "consumer",
            direct
                .getCommitLogReader(status)
                .orElseThrow(
                    () -> new IllegalArgumentException("Missing commit log reader for data")),
            Sets.newHashSet(status),
            new PassthroughFilter(),
            fakeBulkWriter(written, stamp -> stamp == 100));
    AtomicInteger commits = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      long watermark = i * 20;
      observer.onNext(
          getUpdate(gateway, armed, now),
          new OnNextContext() {
            @Override
            public OffsetCommitter committer() {
              return (success, error) -> commits.incrementAndGet();
            }

            @Override
            public Partition getPartition() {
              return null;
            }

            @Override
            public long getWatermark() {
              return watermark;
            }

            @Override
            public Offset getOffset() {
              return null;
            }
          });
    }
    assertEquals(0, commits.get());
    assertEquals(0, written.size());
  }

  @Test
  public void testBulkReplication() throws InterruptedException {
    final List<StreamElement> written = new ArrayList<>();
    final CommitLogReader reader = Optionals.get(direct.getCommitLogReader(data));
    final CommitLogObserver observer =
        controller.createBulkObserver(
            "consumer",
            reader,
            Sets.newHashSet(data),
            new PassthroughFilter(),
            fakeBulkWriter(
                written, stamp -> stamp == now - InMemStorage.getBoundedOutOfOrderness() + 100));
    try (ObserveHandle ignored = reader.observeBulk("consumer", observer)) {
      for (int i = 0; i < 10; i++) {
        writeEvent(now + 20 * i);
      }
      assertEquals(6, written.size());
    }
    livenessLatch.await();
    assertEquals(1.0, Metrics.LIVENESS.getValue(), 0.0001);
  }

  @Test
  public void testOnErrorIsRetryable() {
    CommitLogObserver observer = controller.createBulkObserver(null, null, null, null, null);
    for (int i = 0; i < 3; i++) {
      assertTrue(observer.onError(new IllegalArgumentException()));
    }
  }

  @Test
  public void testTransformationObserverRetryable() {
    TransformationObserver observer = new TransformationObserver(direct, null, null, true, null);
    assertTrue(observer.onError(new IllegalArgumentException()));
  }

  private OnlineAttributeWriter fakeOnlineWriter(List<StreamElement> written) {
    return new OnlineAttributeWriter() {
      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        written.add(data);
        statusCallback.commit(true, null);
      }

      @Override
      public Factory<?> asFactory() {
        return repo -> fakeOnlineWriter(written);
      }

      @Override
      public URI getUri() {
        return URI.create("fake-online:///");
      }

      @Override
      public void close() {}
    };
  }

  private BulkAttributeWriter fakeBulkWriter(
      List<StreamElement> written, UnaryPredicate<Long> commitWatermarkPredicate) {

    return new BulkAttributeWriter() {

      private final List<StreamElement> buffered = new ArrayList<>();

      @Override
      public void write(StreamElement data, long watermark, CommitCallback statusCallback) {
        buffered.add(data);
        if (commitWatermarkPredicate.apply(watermark)) {
          written.addAll(buffered);
          buffered.clear();
          statusCallback.commit(true, null);
        }
      }

      @Override
      public Factory<?> asFactory() {
        return repo -> fakeBulkWriter(written, commitWatermarkPredicate);
      }

      @Override
      public URI getUri() {
        return URI.create("fake-bulk:///");
      }

      @Override
      public void rollback() {
        written.clear();
      }

      @Override
      public void close() {}
    };
  }

  private void writeEvent() {
    writeEvent(now);
  }

  private void writeEvent(long stamp) {
    direct
        .getWriter(data)
        .orElseThrow(() -> new IllegalArgumentException("Missing writer for data"))
        .write(getUpdate(event, data, stamp), (succ, exc) -> {});
  }

  private static StreamElement getUpdate(
      EntityDescriptor entity, AttributeDescriptor<?> attr, long stamp) {

    return StreamElement.upsert(
        entity,
        attr,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        attr.getName(),
        stamp,
        new byte[] {});
  }
}
