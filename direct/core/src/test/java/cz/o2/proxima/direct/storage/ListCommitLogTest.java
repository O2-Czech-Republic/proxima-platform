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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.ListCommitLog.ListObserveHandle;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryPredicate;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkEstimator;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

/** Test {@link ListCommitLog}. */
public class ListCommitLogTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor event = repo.getEntity("event");
  private final AttributeDescriptor<byte[]> data = event.getAttribute("data");

  @Test(timeout = 10000)
  public void testObserveExternalizableUnnamed() throws InterruptedException {
    CommitLogReader reader = ListCommitLog.of(data(10), direct.getContext());
    List<StreamElement> data = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle = reader.observe(null, toList(data, b -> latch.countDown()));
    latch.await();
    assertEquals(10, data.size());
    assertTrue(handle.getCommittedOffsets().isEmpty());
    assertFalse(handle.getCurrentOffsets().isEmpty());
  }

  @Test(timeout = 10000)
  public void testObserveNonExternalizableUnnamed() throws InterruptedException {
    CommitLogReader reader = ListCommitLog.ofNonExternalizable(data(10), direct.getContext());
    List<StreamElement> data = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle = reader.observe(null, toList(data, b -> latch.countDown()));
    latch.await();
    assertEquals(10, data.size());
    assertFalse(handle.getCommittedOffsets().isEmpty());
    assertFalse(handle.getCurrentOffsets().isEmpty());
    ListObserveHandle listObserveHandle = (ListObserveHandle) handle;
    ListCommitLog.Consumer consumer = listObserveHandle.getConsumer();
    assertTrue(consumer.getInflightOffsets().isEmpty());
  }

  @Test(timeout = 10000)
  public void testObserveBulkNonExternalizableUnnamed() throws InterruptedException {
    CommitLogReader reader = ListCommitLog.ofNonExternalizable(data(10), direct.getContext());
    List<StreamElement> data = new ArrayList<>();
    CountDownLatch first = new CountDownLatch(1);
    CountDownLatch second = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observeBulk(
            null,
            new LogObserver() {
              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                context.nack();
                return false;
              }

              @Override
              public void onCancelled() {
                first.countDown();
              }
            });
    first.await();
    List<Offset> offsets = handle.getCurrentOffsets();
    handle = reader.observeBulkOffsets(offsets, toList(data, b -> second.countDown()));
    second.await();
    assertEquals(10, data.size());
    assertFalse(handle.getCommittedOffsets().isEmpty());
    assertFalse(handle.getCurrentOffsets().isEmpty());
    ListObserveHandle listObserveHandle = (ListObserveHandle) handle;
    ListCommitLog.Consumer consumer = listObserveHandle.getConsumer();
    assertTrue(consumer.getInflightOffsets().isEmpty());
  }

  @Test
  public void testObserveNonExternalizableWatermark() throws InterruptedException {
    int numElements = 10;
    CommitLogReader reader =
        ListCommitLog.ofNonExternalizable(data(numElements), direct.getContext());
    CountDownLatch first = new CountDownLatch(1);
    List<Long> watermarks = new ArrayList<>();
    ObserveHandle handle =
        reader.observeBulk(
            null,
            new LogObserver() {

              int received = 0;

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                watermarks.add(context.getWatermark());
                if (++received == numElements) {
                  context.confirm();
                }
                return true;
              }

              @Override
              public void onCompleted() {
                first.countDown();
              }
            });
    first.await();
    assertEquals(numElements, watermarks.size());
    long min = watermarks.get(0);
    for (int i = 1; i < numElements; i++) {
      assertEquals(min, (long) watermarks.get(i));
    }
  }

  @Test(timeout = 10000)
  public void testObserveExternalizableUnnamedPauseContinue() throws InterruptedException {
    CommitLogReader reader = ListCommitLog.of(data(10), direct.getContext());
    List<StreamElement> data = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observe(null, toList(data, b -> latch.countDown(), v -> v.getValue()[0] < 5));
    latch.await();
    assertEquals(6, data.size());
    assertTrue(handle.getCommittedOffsets().isEmpty());
    assertFalse(handle.getCurrentOffsets().isEmpty());
    CountDownLatch nextLatch = new CountDownLatch(1);
    reader.observeBulkOffsets(handle.getCurrentOffsets(), toList(data, b -> nextLatch.countDown()));
    nextLatch.await();
    assertEquals(11, data.size());
  }

  @Test(timeout = 10000)
  public void testObserveNonExternalizableUnnamedPauseContinue() throws InterruptedException {
    CommitLogReader reader = ListCommitLog.ofNonExternalizable(data(10), direct.getContext());
    List<StreamElement> data = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observe(null, toList(data, b -> latch.countDown(), v -> v.getValue()[0] < 5));
    latch.await();
    assertEquals(6, data.size());
    assertFalse(handle.getCommittedOffsets().isEmpty());
    assertFalse(handle.getCurrentOffsets().isEmpty());
    CountDownLatch nextLatch = new CountDownLatch(1);
    reader.observeBulkOffsets(handle.getCurrentOffsets(), toList(data, b -> nextLatch.countDown()));
    nextLatch.await();
    assertEquals(10, data.size());
  }

  @Test(timeout = 10000)
  public void testObserveNonExternalizableUnnamedPauseContinueNoCommit()
      throws InterruptedException {

    CommitLogReader reader = ListCommitLog.ofNonExternalizable(data(10), direct.getContext());
    List<StreamElement> data = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observe(null, toList(data, b -> latch.countDown(), v -> v.getValue()[0] < 5));
    latch.await();
    assertEquals(6, data.size());
    assertFalse(handle.getCommittedOffsets().isEmpty());
    assertFalse(handle.getCurrentOffsets().isEmpty());
    CountDownLatch nextLatch = new CountDownLatch(1);
    reader.observeBulkOffsets(handle.getCurrentOffsets(), toList(data, b -> nextLatch.countDown()));
    nextLatch.await();
    assertEquals(10, data.size());
  }

  @Test(timeout = 1000)
  public void testObserveWithCustomWatemarkEstimator() throws InterruptedException {
    long now = System.currentTimeMillis() - 1000;
    int numElements = 10;
    CommitLogReader reader =
        ListCommitLog.of(data(numElements), new TestWatermarkEstimator(now), direct.getContext());

    CountDownLatch latch = new CountDownLatch(1);
    List<Long> watermarks = new ArrayList<>();
    ObserveHandle handle =
        reader.observe(
            null,
            new LogObserver() {
              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                context.confirm();
                watermarks.add(context.getWatermark());
                return true;
              }

              @Override
              public void onCompleted() {
                latch.countDown();
              }
            });
    latch.await();
    assertEquals(numElements, watermarks.size());
    List<Long> expected =
        IntStream.range(0, numElements).mapToObj(i -> now + i).collect(Collectors.toList());
    assertEquals(expected, watermarks);
  }

  private static LogObserver toList(List<StreamElement> list, Consumer<Boolean> onFinished) {
    return toList(list, onFinished, ign -> true);
  }

  private static LogObserver toList(
      List<StreamElement> list,
      Consumer<Boolean> onFinished,
      UnaryPredicate<StreamElement> shouldContinue) {

    return new LogObserver() {
      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        list.add(ingest);
        context.confirm();
        return shouldContinue.apply(ingest);
      }

      @Override
      public void onCompleted() {
        onFinished.accept(true);
      }

      @Override
      public void onCancelled() {
        onFinished.accept(false);
      }
    };
  }

  private List<StreamElement> data(int count) {
    long now = System.currentTimeMillis();
    return IntStream.range(0, count)
        .mapToObj(
            i ->
                StreamElement.upsert(
                    event,
                    data,
                    UUID.randomUUID().toString(),
                    "key" + i,
                    data.getName(),
                    now + i,
                    new byte[] {(byte) i}))
        .collect(Collectors.toList());
  }

  private static class TestWatermarkEstimator implements WatermarkEstimator {

    long watermark;

    public TestWatermarkEstimator(long now) {
      watermark = now;
    }

    @Override
    public long getWatermark() {
      return watermark;
    }

    @Override
    public void setMinWatermark(long minWatermark) {
      // nop
    }

    @Override
    public void update(StreamElement element) {
      watermark++;
    }
  }
}
