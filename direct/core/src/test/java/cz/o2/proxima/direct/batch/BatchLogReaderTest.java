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
package cz.o2.proxima.direct.batch;

import static cz.o2.proxima.direct.commitlog.CommitLogReaderTest.withNumRecordsPerSec;
import static org.junit.Assert.*;

import com.google.common.collect.Streams;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.ListBatchReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.ReplicationRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link BatchLogReader BatchLogReaders}. */
public class BatchLogReaderTest {

  private Repository repo;
  private DirectDataOperator direct;
  private EntityDescriptor entity;
  private AttributeDescriptor<byte[]> attr;

  private long now;

  @Before
  public void setUp() {
    repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    ReplicationRunner.runAttributeReplicas(direct);
    entity = repo.getEntity("gateway");
    attr = entity.getAttribute("armed");
    now = System.currentTimeMillis();
  }

  @Test
  public void testSimpleObserve() throws InterruptedException {
    write("gw", new byte[] {1});
    BatchLogReader reader = getBatchReader();
    BlockingQueue<StreamElement> read = new SynchronousQueue<>();
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(attr),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            ExceptionUtils.unchecked(() -> read.put(element));
            return false;
          }
        });
    StreamElement element = read.take();
    assertEquals("gw", element.getKey());
  }

  @After
  public void tearDown() {
    repo.drop();
  }

  @Test(timeout = 5000)
  public void testObserveWithContext() throws InterruptedException {
    write("gw", new byte[] {1});
    BatchLogReader reader = getBatchReader();
    BlockingQueue<StreamElement> read = new SynchronousQueue<>();
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(attr),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            assertEquals(0, context.getPartition().getId());
            assertEquals(Long.MIN_VALUE, context.getWatermark());
            ExceptionUtils.unchecked(() -> read.put(element));
            return false;
          }
        });
    StreamElement element = read.take();
    assertEquals("gw", element.getKey());
  }

  @Test(timeout = 5000)
  public void testObserveWithoutThroughputLimit() throws InterruptedException {
    int numElements = 100;
    for (int i = 0; i < numElements; i++) {
      write("gw" + i, new byte[] {(byte) i});
    }
    BatchLogReader reader = getBatchReader();
    BlockingQueue<StreamElement> read = new SynchronousQueue<>();
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(attr),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            ExceptionUtils.unchecked(() -> read.put(element));
            return true;
          }
        });

    List<Integer> values = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      StreamElement element = read.take();
      assertNotNull(element.getValue());
      assertEquals(1, element.getValue().length);
      values.add((int) element.getValue()[0]);
    }
    assertEquals(
        IntStream.range(0, numElements).boxed().collect(Collectors.toList()),
        values.stream().sorted().collect(Collectors.toList()));
    assertTrue(System.currentTimeMillis() - now < 1000);
  }

  @Test(timeout = 5000)
  public void testObserveWithThroughputLimit() throws InterruptedException {
    int numElements = 100;
    for (int i = 0; i < numElements; i++) {
      write("gw" + i, new byte[] {(byte) i});
    }
    BatchLogReader reader =
        BatchLogReaders.withLimitedThroughput(
            getBatchReader(), withNumRecordsPerSec(numElements / 2));
    BlockingQueue<StreamElement> read = new SynchronousQueue<>();
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(attr),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            ExceptionUtils.unchecked(() -> read.put(element));
            return true;
          }
        });

    List<Integer> values = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      StreamElement element = read.take();
      assertNotNull(element.getValue());
      assertEquals(1, element.getValue().length);
      values.add((int) element.getValue()[0]);
    }
    assertEquals(
        IntStream.range(0, numElements).boxed().collect(Collectors.toList()),
        values.stream().sorted().collect(Collectors.toList()));
    assertTrue(System.currentTimeMillis() - now > 1000);
  }

  @Test
  public void testObserveReadOffset() throws InterruptedException {
    final List<StreamElement> firstPartition = newPartition("first_", 10);
    final List<StreamElement> secondPartition = newPartition("second_", 20);
    final List<StreamElement> thirdPartition = newPartition("third_", 30);
    final ListBatchReader reader =
        ListBatchReader.ofPartitioned(
            direct.getContext(), Arrays.asList(firstPartition, secondPartition, thirdPartition));
    final ConcurrentMap<Partition, BatchLogObserver.Offset> lastOffsets = new ConcurrentHashMap<>();
    final CountDownLatch doneConsuming = new CountDownLatch(1);
    reader.observe(
        Arrays.asList(Partition.of(0), Partition.of(1), Partition.of(2)),
        Collections.singletonList(attr),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            lastOffsets.merge(
                context.getPartition(),
                context.getOffset(),
                (oldValue, newValue) -> {
                  assertTrue(oldValue.getElementIndex() < newValue.getElementIndex());
                  return newValue;
                });
            return true;
          }

          @Override
          public void onCompleted() {
            doneConsuming.countDown();
          }
        });
    doneConsuming.await();
    assertEquals(
        new BatchLogObserver.SimpleOffset(Partition.of(0), 9, true),
        lastOffsets.get(Partition.of(0)));
    assertEquals(
        new BatchLogObserver.SimpleOffset(Partition.of(1), 19, true),
        lastOffsets.get(Partition.of(1)));
    assertEquals(
        new BatchLogObserver.SimpleOffset(Partition.of(2), 29, true),
        lastOffsets.get(Partition.of(2)));
  }

  @Test
  public void testObserveOffsets() throws InterruptedException {
    final List<StreamElement> firstPartition = newPartition("first_", 100);
    final List<StreamElement> secondPartition = newPartition("second_", 80);
    final List<StreamElement> thirdPartition = newPartition("third_", 60);
    final ListBatchReader reader =
        ListBatchReader.ofPartitioned(
            direct.getContext(), Arrays.asList(firstPartition, secondPartition, thirdPartition));
    final BlockingQueue<String> consumed = new LinkedBlockingQueue<>();
    final CountDownLatch doneConsuming = new CountDownLatch(1);
    reader.observeOffsets(
        Arrays.asList(
            new BatchLogObserver.SimpleOffset(Partition.of(0), 50, false),
            new BatchLogObserver.SimpleOffset(Partition.of(2), 40, false)),
        Collections.singletonList(attr),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element) {
            assertTrue(consumed.add(element.getKey()));
            return true;
          }

          @Override
          public void onCompleted() {
            doneConsuming.countDown();
          }
        });
    doneConsuming.await();
    final Set<String> expected =
        Streams.concat(
                firstPartition.subList(50, firstPartition.size()).stream(),
                thirdPartition.subList(40, thirdPartition.size()).stream())
            .map(StreamElement::getKey)
            .collect(Collectors.toSet());
    assertEquals(expected, new HashSet<>(consumed));
  }

  private BatchLogReader getBatchReader() {
    return Optionals.get(direct.getBatchLogReader(attr));
  }

  private List<StreamElement> newPartition(String keyPrefix, int size) {
    final List<StreamElement> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      result.add(newData(keyPrefix + i, new byte[] {(byte) i}));
    }
    return Collections.unmodifiableList(result);
  }

  private StreamElement newData(String key, byte[] value) {
    return StreamElement.upsert(
        entity, attr, UUID.randomUUID().toString(), key, attr.getName(), now, value);
  }

  private void write(String key, byte[] value) {
    Optionals.get(direct.getWriter(attr)).write(newData(key, value), CommitCallback.noop());
  }
}
