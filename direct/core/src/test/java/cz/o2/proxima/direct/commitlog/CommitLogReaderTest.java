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
package cz.o2.proxima.direct.commitlog;

import static org.junit.Assert.*;

import com.google.common.collect.Iterables;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReaders.LimitedCommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.ThroughputLimiter;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.After;
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
            .map(repo.getOrCreateOperator(DirectDataOperator.class)::resolveRequired)
            .get();

    reader = family.getCommitLogReader().get();
    writer = family.getWriter().get();
  }

  @After
  public void tearDown() {
    repo.drop();
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
            StreamElement.upsert(
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
            StreamElement.upsert(
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
            StreamElement.upsert(
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
              StreamElement.upsert(
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
              StreamElement.upsert(
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
            StreamElement.upsert(
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
        LongStream.range(0, 100).boxed().collect(Collectors.toList()),
        received.stream().map(s -> s.getStamp() - now).collect(Collectors.toList()));
  }

  @Test(timeout = 10000)
  public void testObserveOrderedPerPartition() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(100);
    reader.observe(
        "test",
        LogObservers.withSortBufferWithinPartition(
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
              StreamElement.upsert(
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
            StreamElement.upsert(
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
        LongStream.range(0, 100).boxed().collect(Collectors.toList()),
        received.stream().map(s -> s.getStamp() - now).collect(Collectors.toList()));
  }

  @Test
  public void testOrderedObserverLifycycle() {
    StreamElement update =
        StreamElement.upsert(
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
    inner.onRepartition(Collections::emptyList);
    assertEquals(0x1F, mask.get());
    inner.onIdle(() -> 0);
    assertEquals(0x3F, mask.get());
  }

  @Test(timeout = 5000)
  public void testObserveThroughputLimitedCommitLog() throws InterruptedException {
    testThroughputLimitedCommitLogWithObserve(
        (reader, onNext) -> reader.observe("dummy", createLimitedObserver(onNext)));
  }

  @Test(timeout = 5000)
  public void testObserveBulkThroughputLimitedCommitLog() throws InterruptedException {
    testThroughputLimitedCommitLogWithObserve(
        (reader, onNext) ->
            reader.observeBulk("dummy", Position.NEWEST, createLimitedObserver(onNext)));
  }

  @Test(timeout = 5000)
  public void testObserveBulkPartitionsThroughputLimitedCommitLog() throws InterruptedException {
    testThroughputLimitedCommitLogWithObserve(
        (reader, onNext) ->
            reader.observeBulkPartitions(
                "dummy", reader.getPartitions(), Position.NEWEST, createLimitedObserver(onNext)));
  }

  @Test(timeout = 5000)
  public void testObservePartitionsThroughputLimitedCommitLog() throws InterruptedException {
    testThroughputLimitedCommitLogWithObserve(
        (reader, onNext) ->
            reader.observePartitions(
                "dummy",
                reader.getPartitions(),
                Position.NEWEST,
                false,
                createLimitedObserver(onNext)));
  }

  @Test
  public void testThroughputLimitedCommitLogReaderAsFactory() {
    ThroughputLimiter limiter = ThroughputLimiter.NoOpThroughputLimiter.INSTANCE;
    CommitLogReader limitedReader = CommitLogReaders.withThroughputLimit(reader, limiter);
    CommitLogReader cloned = limitedReader.asFactory().apply(repo);
    assertTrue(limitedReader.getClass().isAssignableFrom(cloned.getClass()));
  }

  @Test(timeout = 10000)
  public void testThroughputLimitedCommitLogIdles() throws InterruptedException {
    ThroughputLimiter limiter = ThroughputLimiter.NoOpThroughputLimiter.INSTANCE;
    CommitLogReader limitedReader = CommitLogReaders.withThroughputLimit(reader, limiter);
    UnaryFunction<CountDownLatch, LogObserver> observerFactory =
        myLatch ->
            new LogObserver() {

              @Override
              public boolean onError(Throwable error) {
                return false;
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                return false;
              }

              @Override
              public void onIdle(OnIdleContext context) {
                myLatch.countDown();
              }
            };
    CountDownLatch latch = new CountDownLatch(1);
    LogObserver observer = observerFactory.apply(latch);
    long nanoTime = System.nanoTime();
    ObserveHandle handle = limitedReader.observe("dummy", observer);
    latch.await();
    handle.close();
    latch = new CountDownLatch(1);
    observer = observerFactory.apply(latch);
    long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoTime);
    handle.close();
    limitedReader = CommitLogReaders.withThroughputLimit(reader, withNumRecordsPerSec(1));
    handle = limitedReader.observe("dummy", observer);
    // no idle called this time
    assertFalse(latch.await(2 * durationMillis, TimeUnit.MILLISECONDS));
  }

  @Test(timeout = 5000)
  public void testObserveOffsetsThroughputLimitedCommitLog() throws InterruptedException {
    testThroughputLimitedCommitLogWithObserve(
        (reader, onNext) -> {
          ObserveHandle handle =
              reader.observeBulk(
                  "offset-fetch",
                  Position.NEWEST,
                  false,
                  new LogObserver() {

                    @Override
                    public boolean onError(Throwable error) {
                      return false;
                    }

                    @Override
                    public boolean onNext(StreamElement ingest, OnNextContext context) {
                      return false;
                    }
                  });
          ExceptionUtils.unchecked(handle::waitUntilReady);
          List<Offset> offsets = handle.getCommittedOffsets();
          handle.close();
          return reader.observeBulkOffsets(offsets, createLimitedObserver(onNext));
        });
  }

  @Test(timeout = 10000)
  public void testThroughputLimitedCommitLogReader() {
    LimitedCommitLogReader limited =
        (LimitedCommitLogReader)
            CommitLogReaders.withThroughputLimit(reader, withNumRecordsPerSec(100));
    AtomicBoolean completed = new AtomicBoolean();
    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicBoolean throwOnNext = new AtomicBoolean();
    AtomicReference<Throwable> caught = new AtomicReference<>();

    for (int i = 0; i < 10; i++) {
      writer
          .online()
          .write(
              StreamElement.upsert(
                  entity,
                  attr,
                  UUID.randomUUID().toString(),
                  "key" + i,
                  attr.getName(),
                  System.currentTimeMillis(),
                  new byte[] {1, 2}),
              (succ, exc) -> {});
    }

    LogObserver observer =
        new LogObserver() {

          @Override
          public void onCompleted() {
            completed.set(true);
          }

          @Override
          public void onCancelled() {
            cancelled.set(true);
          }

          @Override
          public boolean onError(Throwable error) {
            caught.set(error);
            return false;
          }

          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            if (throwOnNext.get()) {
              throw new RuntimeException("Fail");
            }
            return true;
          }
        };
    limited.observeBulk("dummy", Position.OLDEST, true, observer);
    while (!completed.get()) {
      ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(50));
    }

    ObserveHandle handle = limited.observeBulk("dummy", Position.OLDEST, true, observer);
    handle.close();
    while (!cancelled.get()) {
      ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(50));
    }

    throwOnNext.set(true);
    limited.observeBulk("dummy", Position.OLDEST, true, observer);
    while (caught.get() == null) {
      ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(50));
    }
  }

  private LogObserver createLimitedObserver(Runnable onNext) {
    return new LogObserver() {

      @Override
      public boolean onError(Throwable error) {
        return false;
      }

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        onNext.run();
        return true;
      }
    };
  }

  private void testThroughputLimitedCommitLogWithObserve(
      BiFunction<CommitLogReader, Runnable, ObserveHandle> observe) throws InterruptedException {
    final int numElements = 50;
    ThroughputLimiter limiter = withNumRecordsPerSec(2 * numElements);
    CommitLogReader limitedReader = CommitLogReaders.withThroughputLimit(reader, limiter);
    assertEquals(limitedReader.getPartitions(), reader.getPartitions());
    assertEquals(limitedReader.getUri(), reader.getUri());
    CountDownLatch latch = new CountDownLatch(numElements);
    AtomicLong lastMillis = new AtomicLong(System.currentTimeMillis());
    List<Long> durations = new ArrayList<>();
    ObserveHandle handle =
        observe.apply(
            limitedReader,
            () -> {
              latch.countDown();
              long now = System.currentTimeMillis();
              long lastGet = lastMillis.getAndUpdate(current -> now);
              durations.add(now - lastGet);
            });

    for (int i = 0; i < 50; i++) {
      writer
          .online()
          .write(
              StreamElement.upsert(
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
    assertEquals(numElements, durations.size());
    assertEquals(
        durations.toString(),
        numElements,
        durations.stream().filter(s -> s >= 500 / numElements).count());
    handle.close();
  }

  public static ThroughputLimiter withNumRecordsPerSec(int recordsPerSec) {
    final Duration pauseDuration = Duration.ofMillis(1000L / recordsPerSec);
    return new ThroughputLimiter() {

      @Override
      public Duration getPauseTime(Context context) {
        assertEquals(1, context.getConsumedPartitions().size());
        assertEquals(0, Iterables.getOnlyElement(context.getConsumedPartitions()).getId());
        assertTrue(context.getMinWatermark() < Long.MAX_VALUE);
        return pauseDuration;
      }

      @Override
      public void close() {}
    };
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
