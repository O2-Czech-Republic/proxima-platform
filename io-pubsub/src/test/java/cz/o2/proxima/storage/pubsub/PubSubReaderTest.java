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
package cz.o2.proxima.storage.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorImpl;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import static cz.o2.proxima.storage.pubsub.Util.delete;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static cz.o2.proxima.storage.pubsub.Util.deleteWildcard;
import static cz.o2.proxima.storage.pubsub.Util.update;

/**
 * Test suite for {@link PubSubReader}.
 */
public class PubSubReaderTest {

  private final Repository repo = Repository.of(ConfigFactory.load().resolve());
  private final AttributeDescriptorImpl<?> attr;
  private final AttributeDescriptorImpl<?> wildcard;
  private final EntityDescriptor entity;
  private final PubSubAccessor accessor;
  private TestPubSubReader reader;

  private class TestPubSubReader extends PubSubReader {

    private final Context context;
    private Supplier<PubsubMessage> supplier;

    public TestPubSubReader(Context context) {
      super(accessor, context);
      this.context = context;
    }

    void setSupplier(Supplier<PubsubMessage> supplier) {
      this.supplier = supplier;
    }

    @Override
    Subscriber newSubscriber(
        SubscriptionName subscription,
        MessageReceiver receiver) {

      return MockSubscriber.create(
          subscription, receiver, supplier, context.getExecutorService());
    }

  }

  public PubSubReaderTest() throws URISyntaxException {
    this.attr = AttributeDescriptor.newBuilder(repo)
        .setEntity("entity")
        .setName("attr")
        .setSchemeURI(new URI("bytes:///"))
        .build();
    this.wildcard = AttributeDescriptor.newBuilder(repo)
        .setEntity("entity")
        .setName("wildcard.*")
        .setSchemeURI(new URI("bytes:///"))
        .build();
    this.entity = EntityDescriptor.newBuilder()
        .setName("entity")
        .addAttribute(attr)
        .addAttribute(wildcard)
        .build();
    assertTrue(entity.findAttribute("attr").isPresent());
    this.accessor = new PubSubAccessor(
        entity, new URI("gps://my-project/topic"), Collections.emptyMap());
  }

  @Before
  public void setUp() {
    Context context = new Context(() -> Executors.newCachedThreadPool()) { };
    reader = new TestPubSubReader(context);
  }

  @Test(timeout = 2000)
  public void testObserve() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs = new LinkedList<>(
        Arrays.asList(
            update("key1", "attr", new byte[] { 1, 2 }, now),
            delete("key2", "attr", now + 1000),
            deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(() -> {
      if (inputs.isEmpty()) {
        LockSupport.park();
      }
      return inputs.pop();
    });
    List<StreamElement> elems = new ArrayList<>();
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(3);
    ObserveHandle handle = reader.observe("dummy", new LogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        elems.add(ingest);
        committer.confirm();
        latch.countDown();
        return true;
      }

      @Override
      public void onCancelled() {
        cancelled.set(true);
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    });
    latch.await();
    handle.cancel();
    assertEquals(3, elems.size());
    StreamElement elem = elems.get(0);
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] { 1, 2 }, elem.getValue());
    assertEquals(now, elem.getStamp());
    elem = elems.get(1);
    assertEquals("key2", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertEquals(now + 1000L, elem.getStamp());
    elem = elems.get(2);
    assertEquals("key3", elem.getKey());
    assertEquals(wildcard.toAttributePrefix() + "*", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertTrue(elem.isDeleteWildcard());
    assertEquals(now, elem.getStamp());

    assertTrue(cancelled.get());
  }

  @Test(timeout = 2000)
  public void testObserveError() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs = new LinkedList<>(
        Arrays.asList(
            update("key1", "attr", new byte[] { 1, 2 }, now),
            delete("key2", "attr", now + 1000),
            deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(() -> {
      if (inputs.isEmpty()) {
        LockSupport.park();
      }
      return inputs.pop();
    });
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(3);
    ObserveHandle handle = reader.observe("dummy", new LogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        throw new RuntimeException("Fail");
      }

      @Override
      public void onCancelled() {
        cancelled.set(true);
      }

      @Override
      public boolean onError(Throwable error) {
        assertEquals("Fail", error.getMessage());
        latch.countDown();
        return true;
      }
    });
    latch.await();
    handle.cancel();
  }

  @Test(timeout = 2000)
  public void testObserveBulk() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs = new LinkedList<>(
        Arrays.asList(
            update("key1", "attr", new byte[] { 1, 2 }, now),
            delete("key2", "attr", now + 1000),
            deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(() -> {
      if (inputs.isEmpty()) {
        LockSupport.park();
      }
      return inputs.pop();
    });
    List<StreamElement> elems = new ArrayList<>();
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(3);
    ObserveHandle handle = reader.observeBulk("dummy", new BulkLogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        elems.add(ingest);
        committer.confirm();
        latch.countDown();
        return true;
      }

      @Override
      public void onCancelled() {
        cancelled.set(true);
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    });
    latch.await();
    handle.cancel();
    assertEquals(3, elems.size());
    StreamElement elem = elems.get(0);
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] { 1, 2 }, elem.getValue());
    assertEquals(now, elem.getStamp());
    elem = elems.get(1);
    assertEquals("key2", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertEquals(now + 1000L, elem.getStamp());
    elem = elems.get(2);
    assertEquals("key3", elem.getKey());
    assertEquals(wildcard.toAttributePrefix() + "*", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertTrue(elem.isDeleteWildcard());
    assertEquals(now, elem.getStamp());

    assertTrue(cancelled.get());
  }

}
