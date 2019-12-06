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
package cz.o2.proxima.direct.commitlog;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.Test;

/** {@link CommitLogReader} test suite. */
public class CommitLogReaderTest {

  private final transient Repository repo =
      Repository.of(
          ConfigFactory.load().withFallback(ConfigFactory.load("test-reference.conf")).resolve());

  private final transient EntityDescriptor entity = Optionals.get(repo.findEntity("event"));

  private final transient AttributeDescriptor<?> attr = Optionals.get(entity.findAttribute("data"));

  private transient CommitLogReader reader;
  private transient AttributeWriterBase writer;

  @Before
  public void setUp() {
    DirectAttributeFamilyDescriptor family =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("event-storage-stream"))
            .findAny()
            .map(repo.asDataOperator(DirectDataOperator.class)::resolveRequired)
            .get();

    reader = family.getCommitLogReader().get();
    writer = family.getWriter().get();
  }

  @Test(timeout = 10000)
  public void testObserveSimple() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        "test",
        new LogObserver() {

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            received.add(ingest);
            latch.countDown();
            context.confirm();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });

    writer
        .online()
        .write(
            StreamElement.update(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            (succ, exc) -> {});

    latch.await();

    assertEquals(1, received.size());
  }

  @Test(timeout = 10000)
  public void testObserveWithError() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    reader.observe(
        "test",
        new LogObserver() {

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            throw new RuntimeException("fail");
          }

          @Override
          public boolean onError(Throwable error) {
            caught.set(error);
            latch.countDown();
            return false;
          }
        });

    writer
        .online()
        .write(
            StreamElement.update(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            (succ, exc) -> {});

    latch.await();

    assertNotNull(caught.get());
  }

  @Test(timeout = 10000)
  public void testObserveWithRetry() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger count = new AtomicInteger();
    RetryableLogObserver observer =
        RetryableLogObserver.online(
            2,
            "test",
            reader,
            new LogObserver() {

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext confirm) {
                if (count.incrementAndGet() == 0) {
                  throw new RuntimeException("fail");
                }
                received.add(ingest);
                latch.countDown();
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                return false;
              }
            });

    observer.start();
    writer
        .online()
        .write(
            StreamElement.update(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            (succ, exc) -> {});

    latch.await();

    assertEquals(1, received.size());
  }

  @Test(timeout = 10000)
  public void testBulkObserve() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(2);
    reader.observeBulk(
        "test",
        new LogObserver() {

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            received.add(ingest);
            latch.countDown();
            if (received.size() == 2) {
              context.confirm();
            }
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });

    for (int i = 0; i < 2; i++) {
      writer
          .online()
          .write(
              StreamElement.update(
                  entity,
                  attr,
                  UUID.randomUUID().toString(),
                  "key",
                  attr.getName(),
                  System.currentTimeMillis(),
                  new byte[] {1, 2}),
              (succ, exc) -> {});
    }

    latch.await();

    assertEquals(2, received.size());
  }

  @Test(timeout = 10000)
  public void testObserveOrdered() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(100);
    reader.observe(
        "test",
        LogObservers.withSortBuffer(
            new LogObserver() {

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                received.add(ingest);
                latch.countDown();
                context.confirm();
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            },
            Duration.ofMillis(500)));

    long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      writer
          .online()
          .write(
              StreamElement.update(
                  entity,
                  attr,
                  UUID.randomUUID().toString(),
                  "key",
                  attr.getName(),
                  now + 99 - i,
                  new byte[] {1, 2}),
              (succ, exc) -> {});
    }

    // put one latecomer to test it is dropped
    writer
        .online()
        .write(
            StreamElement.update(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key",
                attr.getName(),
                now + 99 - 10000,
                new byte[] {1, 2}),
            (succ, exc) -> {});

    latch.await();

    assertEquals(100, received.size());
    assertEquals(
        LongStream.range(0, 100).mapToObj(Long::valueOf).collect(Collectors.toList()),
        received.stream().map(s -> s.getStamp() - now).collect(Collectors.toList()));
  }

  @Test
  public void testOrderedObserverLifycycle() {
    StreamElement update =
        StreamElement.update(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            0,
            new byte[] {1, 2});
    AtomicInteger mask = new AtomicInteger();
    LogObserver inner =
        new LogObserver() {

          @Override
          public void onCompleted() {
            mask.updateAndGet(m -> m | 0x1);
          }

          @Override
          public void onCancelled() {
            mask.updateAndGet(m -> m | 0x2);
          }

          @Override
          public boolean onError(Throwable error) {
            mask.updateAndGet(m -> m | 0x4);
            return true;
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            mask.updateAndGet(m -> m | 0x8);
            return true;
          }

          @Override
          public void onRepartition(OnRepartitionContext context) {
            mask.updateAndGet(m -> m | 0x10);
          }

          @Override
          public void onIdle(OnIdleContext context) {
            mask.updateAndGet(m -> m | 0x20);
          }
        };
    assertEquals(0x0, mask.get());
    inner.onCompleted();
    assertEquals(0x1, mask.get());
    inner.onCancelled();
    assertEquals(0x3, mask.get());
    inner.onError(null);
    assertEquals(0x7, mask.get());
    inner.onNext(update, asOnNextContext(0));
    assertEquals(0x0F, mask.get());
    inner.onRepartition(() -> Collections.emptyList());
    assertEquals(0x1F, mask.get());
    inner.onIdle(() -> 0);
    assertEquals(0x3F, mask.get());
  }

  private OnNextContext asOnNextContext(long watermark) {
    return new OnNextContext() {

      @Override
      public OffsetCommitter committer() {
        return null;
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
    };
  }
}
