/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.pubsub;

import static cz.o2.proxima.direct.io.pubsub.Util.delete;
import static cz.o2.proxima.direct.io.pubsub.Util.deleteWildcard;
import static cz.o2.proxima.direct.io.pubsub.Util.update;
import static org.junit.Assert.*;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.WatermarkEstimatorFactory;
import cz.o2.proxima.core.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OffsetCommitter;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.time.UnboundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.direct.io.pubsub.PubSubReader.PubSubOffset;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import io.grpc.internal.GrpcUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link PubSubReader}. */
public class PubSubReaderTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load().resolve());
  private final DirectDataOperator direct =
      repo.getOrCreateOperator(
          DirectDataOperator.class, op -> op.withExecutorFactory(Executors::newCachedThreadPool));
  private final Context context = direct.getContext();
  private final AttributeDescriptor<?> attr;
  private final AttributeDescriptor<?> wildcard;
  private final EntityDescriptor entity;
  private final PubSubStorage storage = new PubSubStorage();
  private final PubSubAccessor accessor;
  private static final AtomicLong timestampSupplier = new AtomicLong();
  private TestPubSubReader reader;

  public class TestPubSubReader extends PubSubReader {

    private final Context context;
    private final Set<Integer> acked = new HashSet<>();
    private final Set<Integer> nacked = new HashSet<>();

    private Supplier<PubsubMessage> supplier;

    public TestPubSubReader(Context context) {
      super(accessor, context);
      this.context = context;
    }

    void setSupplier(Supplier<PubsubMessage> supplier) {
      this.supplier = supplier;
    }

    @Override
    Subscriber newSubscriber(ProjectSubscriptionName subscription, MessageReceiver receiver) {

      return MockSubscriber.create(
          subscription, receiver, supplier, acked, nacked, context.getExecutorService());
    }
  }

  public static class TestWatermarkEstimatorFactory implements WatermarkEstimatorFactory {
    @Override
    public void setup(Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {}

    @Override
    public WatermarkEstimator create() {
      return UnboundedOutOfOrdernessWatermarkEstimator.newBuilder()
          .withDurationMs(1)
          .withStepMs(1)
          .withTimestampSupplier(timestampSupplier::get)
          .build();
    }
  }

  public PubSubReaderTest() throws URISyntaxException {
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("attr")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    this.wildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("wildcard.*")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("entity")
            .addAttribute(attr)
            .addAttribute(wildcard)
            .build();
    assertTrue(entity.findAttribute("attr").isPresent());

    Map<String, Object> cfg =
        ImmutableMap.<String, Object>builder()
            .put(
                "watermark.estimator-factory",
                PubSubReaderTest.TestWatermarkEstimatorFactory.class.getName())
            .build();
    this.accessor = new PubSubAccessor(storage, entity, new URI("gps://my-project/topic"), cfg);
  }

  @Before
  public void setUp() {
    reader = new TestPubSubReader(context);
    timestampSupplier.set(System.currentTimeMillis());
  }

  @Test(timeout = 10000)
  public void testObserve() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs =
        new LinkedList<>(
            Arrays.asList(
                update("key1", "attr", new byte[] {1, 2}, now),
                delete("key2", "attr", now + 1000),
                deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(
        () -> {
          if (inputs.isEmpty()) {
            LockSupport.park();
          }
          return inputs.pop();
        });
    List<StreamElement> elems = new ArrayList<>();
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(3);
    CommitLogObserver observer =
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            elems.add(element);
            context.confirm();
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
        };
    try (ObserveHandle handle = reader.observe("dummy", observer)) {
      latch.await();
    }
    assertEquals(3, elems.size());
    StreamElement elem = elems.get(0);
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] {1, 2}, elem.getValue());
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
    assertEquals(Sets.newHashSet(0, 1, 2), reader.acked);
  }

  @Test
  public void testObserveWatermark() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs =
        new LinkedList<>(
            Arrays.asList(
                update("key1", "attr", new byte[] {1, 2}, now),
                delete("key2", "attr", now + 1000),
                deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(
        () -> {
          if (inputs.isEmpty()) {
            LockSupport.park();
          }
          return inputs.pop();
        });
    CountDownLatch latch = new CountDownLatch(3);
    AtomicLong watermark = new AtomicLong();
    reader.observe(
        "dummy",
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            timestampSupplier.addAndGet(1000);
            context.confirm();
            watermark.set(context.getWatermark());
            latch.countDown();
            return false;
          }

          @Override
          public void onCancelled() {}

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
    assertTrue(watermark.get() > 0);
  }

  @Test
  public void testObserveCommittedOffset() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs =
        new LinkedList<>(
            Arrays.asList(
                update("key1", "attr", new byte[] {1, 2}, now),
                delete("key2", "attr", now + 1000),
                deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(
        () -> {
          if (inputs.isEmpty()) {
            LockSupport.park();
          }
          return inputs.pop();
        });
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observe(
            "dummy",
            new CommitLogObserver() {
              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                timestampSupplier.addAndGet(1000);
                context.confirm();
                latch.countDown();
                return false;
              }

              @Override
              public void onCancelled() {}

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });
    latch.await();
    assertEquals(1, handle.getCommittedOffsets().size());
    assertTrue(handle.getCommittedOffsets().get(0).getWatermark() > 0);
  }

  @Test
  public void testObserveBulkOffsetsWithWatermark() throws InterruptedException {
    long now = System.currentTimeMillis();
    Offset off = new PubSubOffset("dummy", now);
    reader.setSupplier(
        () -> {
          LockSupport.park();
          return null;
        });
    List<Offset> offsets = Collections.singletonList(off);
    CommitLogObserver observer =
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            context.confirm();
            return false;
          }

          @Override
          public void onCancelled() {}

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };
    try (ObserveHandle handle = reader.observeBulkOffsets(offsets, observer)) {
      handle.waitUntilReady();
      assertEquals(1, handle.getCommittedOffsets().size());
      assertEquals(now, handle.getCommittedOffsets().get(0).getWatermark());
    }
  }

  @Test
  public void testPartitionsSplit() throws InterruptedException {
    List<Partition> partitions = reader.getPartitions();
    assertEquals(1, partitions.size());
    partitions = new ArrayList<>(partitions.get(0).split(3));
    assertEquals(3, partitions.size());
    reader.setSupplier(
        () -> {
          LockSupport.park();
          return null;
        });
    CommitLogObserver observer =
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            context.confirm();
            return false;
          }

          @Override
          public void onCancelled() {}

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };
    try (ObserveHandle handle =
        reader.observeBulkPartitions(partitions, Position.NEWEST, observer)) {
      handle.waitUntilReady();
    }
  }

  @Test(timeout = 10000)
  public void testObserveError() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs =
        new LinkedList<>(
            Arrays.asList(
                update("key1", "attr", new byte[] {1, 2}, now),
                delete("key2", "attr", now + 1000),
                deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(
        () -> {
          if (inputs.isEmpty()) {
            LockSupport.park();
          }
          return inputs.pop();
        });
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(3);
    CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            throw new RuntimeException("Fail");
          }

          @Override
          public void onCancelled() {
            cancelled.set(true);
          }

          @Override
          public boolean onError(Throwable error) {
            assertEquals("Fail", error.getCause().getMessage());
            latch.countDown();
            return true;
          }
        };
    try (final ObserveHandle handle = reader.observe("dummy", observer)) {
      latch.await();
      assertFalse(cancelled.get());
      assertTrue(reader.acked.isEmpty());
      assertFalse(reader.nacked.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void testObserveBulk() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs =
        new LinkedList<>(
            Arrays.asList(
                update("key1", "attr", new byte[] {1, 2}, now),
                delete("key2", "attr", now + 1000),
                deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(
        () -> {
          if (inputs.isEmpty()) {
            LockSupport.park();
          }
          return inputs.pop();
        });
    List<StreamElement> elems = new ArrayList<>();
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(3);
    AtomicReference<OffsetCommitter> commit = new AtomicReference<>();
    CommitLogObserver observer =
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            elems.add(element);
            commit.set(context);
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
        };
    try (ObserveHandle handle = reader.observeBulk("dummy", observer)) {
      latch.await();
      commit.get().confirm();
    }
    assertEquals(3, elems.size());
    StreamElement elem = elems.get(0);
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] {1, 2}, elem.getValue());
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
    assertEquals(Sets.newHashSet(0, 1, 2), reader.acked);
  }

  @Test
  public void testInstantiationHttp2Error() {
    GrpcUtil.Http2Error error = GrpcUtil.Http2Error.NO_ERROR;
    assertNotNull(error);
  }

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    CommitLogReader.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(reader.getUri(), factory.apply(repo).getUri());
  }
}
