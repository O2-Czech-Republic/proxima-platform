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
package cz.o2.proxima.repository;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.RetryableLogObserver;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link CommitLogReader} test suite.
 */
public class CommitLogReaderTest {

  private final transient Repository repo = Repository.Builder.of(
      ConfigFactory.load().resolve()).build();

  private transient LocalExecutor executor;
  private final transient EntityDescriptor entity = repo.findEntity("event").get();
  private final transient AttributeDescriptor<?> attr = entity.findAttribute("data").get();

  private transient CommitLogReader reader;
  private transient AttributeWriterBase writer;

  @Before
  public void setUp() {
    executor = new LocalExecutor();
    AttributeFamilyDescriptor family = repo.getAllFamilies()
        .filter(af -> af.getName().equals("event-storage-stream"))
        .findAny()
        .get();

    reader = family.getCommitLogReader().get();
    writer = family.getWriter().get();
  }

  @After
  public void tearDown() {
    executor.abort();
  }

  @Test(timeout = 2000)
  public void testObserveSimple() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe("test", new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, LogObserver.OffsetContext context) {
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

    writer.online().write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 }),
        (succ, exc) -> { });

    latch.await();

    assertEquals(1, received.size());
  }

  @Test(timeout = 2000)
  public void testObserveWithError() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    reader.observe("test", new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, LogObserver.OffsetContext context) {
        throw new RuntimeException("fail");
      }

      @Override
      public boolean onError(Throwable error) {
        caught.set(error);
        latch.countDown();
        return false;
      }

    });

    writer.online().write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 }),
        (succ, exc) -> { });

    latch.await();

    assertNotNull(caught.get());
  }

  @Test(timeout = 2000)
  public void testObserveWithRetry() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger count = new AtomicInteger();
    RetryableLogObserver observer = new RetryableLogObserver(2, "test", reader) {

      @Override
      protected boolean onNextInternal(
          StreamElement ingest, LogObserver.OffsetContext confirm) {

        if (count.incrementAndGet() == 0) {
          throw new RuntimeException("fail");
        }
        received.add(ingest);
        latch.countDown();
        return true;
      }

      @Override
      protected void failure() {

      }

    };

    observer.start();
    writer.online().write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 }),
        (succ, exc) -> { });

    latch.await();

    assertEquals(1, received.size());
  }

  @Test(timeout = 2000)
  public void testBulkObserve() throws InterruptedException {
    List<StreamElement> received = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(2);
    reader.observeBulk("test", new BulkLogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, BulkLogObserver.OffsetContext context) {
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
      writer.online().write(StreamElement.update(
          entity, attr, UUID.randomUUID().toString(),
          "key", attr.getName(), System.currentTimeMillis(), new byte[] { 1, 2 }),
          (succ, exc) -> { });
    }

    latch.await();

    assertEquals(2, received.size());
  }

}
