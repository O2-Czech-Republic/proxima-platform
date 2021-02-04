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
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

/** Test {@link ListBatchReader}. */
public class ListBatchReaderTest {

  private final long now = System.currentTimeMillis();
  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final Context context = repo.getOrCreateOperator(DirectDataOperator.class).getContext();
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<?> armed = gateway.getAttribute("armed");

  @Test(timeout = 10000)
  public void testReadingFromSinglePartition() throws InterruptedException {
    int count = 100;
    List<StreamElement> data = getData(count);
    ListBatchReader reader = ListBatchReader.of(context, data);
    assertEquals(1, reader.getPartitions().size());
    List<StreamElement> read = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    BatchLogObserver observer = toList(read, latch::countDown);
    reader.observe(reader.getPartitions(), Collections.singletonList(status), observer);
    latch.await();
    long keys = read.stream().map(StreamElement::getKey).distinct().count();
    assertEquals(count, keys);
    read.clear();
    latch = new CountDownLatch(1);
    observer = toList(read, latch::countDown);
    reader.observe(reader.getPartitions(), Collections.singletonList(armed), observer);
    latch.await();
    assertTrue(read.isEmpty());
  }

  @Test(timeout = 10000)
  public void testReadingFromTwoPartitions() throws InterruptedException {
    int count = 100;
    List<StreamElement> dataFirst = getData(count);
    List<StreamElement> dataSecond = getData(2 * count);
    ListBatchReader reader = ListBatchReader.ofPartitioned(context, dataFirst, dataSecond);
    assertEquals(2, reader.getPartitions().size());
    List<StreamElement> read = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    BatchLogObserver observer = toList(read, latch::countDown);
    reader.observe(
        reader.getPartitions().subList(0, 1), Collections.singletonList(status), observer);
    latch.await();
    long keys = read.stream().map(StreamElement::getKey).distinct().count();
    assertEquals(count, keys);
    read.clear();
    latch = new CountDownLatch(1);
    observer = toList(read, latch::countDown);
    reader.observe(
        reader.getPartitions().subList(1, 2), Collections.singletonList(status), observer);
    latch.await();
    keys = read.stream().map(StreamElement::getKey).distinct().count();
    assertEquals(2 * count, keys);
    read.clear();
    latch = new CountDownLatch(1);
    observer = toList(read, latch::countDown);
    reader.observe(reader.getPartitions(), Collections.singletonList(status), observer);
    latch.await();
    keys = read.stream().map(StreamElement::getUuid).distinct().count();
    assertEquals(3 * count, keys);
  }

  private BatchLogObserver toList(List<StreamElement> list, Runnable onCompleted) {
    return new BatchLogObserver() {
      @Override
      public boolean onNext(StreamElement element, OnNextContext context) {
        assertNotNull(context);
        list.add(element);
        return true;
      }

      @Override
      public void onCompleted() {
        onCompleted.run();
      }
    };
  }

  private List<StreamElement> getData(int count) {
    return IntStream.range(0, count)
        .mapToObj(
            i ->
                StreamElement.upsert(
                    gateway,
                    status,
                    UUID.randomUUID().toString(),
                    "key" + i,
                    status.getName(),
                    now,
                    new byte[] {}))
        .collect(Collectors.toList());
  }
}
