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
package cz.o2.proxima.direct.hbase;

import static cz.o2.proxima.direct.hbase.HbaseTestUtil.bytes;
import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader.Factory;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test suite for {@link HBaseLogReader}. */
public class HBaseLogReaderTest {

  private static final TableName tableName = TableName.valueOf("test");
  private static final HBaseTestingUtility util = HBaseTestingUtility.createLocalHTU();

  private static MiniHBaseCluster cluster;

  private final Repository repo = ConfigRepository.Builder.ofTest(ConfigFactory.load()).build();
  private final EntityDescriptor entity = repo.findEntity("test").get();
  private final AttributeDescriptor<?> attr = entity.findAttribute("dummy").get();
  private final AttributeDescriptor<?> wildcard = entity.findAttribute("wildcard.*").get();

  private HBaseLogReader reader;
  private Connection conn;
  private Table client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = util.startMiniCluster();
    cluster.waitForActiveAndReadyMaster(10_0000);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    cluster.shutdown();
    cluster.waitUntilShutDown();
  }

  @Before
  public void setUp() throws Exception {
    util.deleteTableIfAny(tableName);
    util.createTable(tableName, bytes("u"), new byte[][] {bytes("first"), bytes("second")});
    conn = ConnectionFactory.createConnection(util.getConfiguration());
    client = conn.getTable(tableName);
    reader =
        new HBaseLogReader(
            new URI("hbase://localhost:2181/test?family=u"),
            cluster.getConfiguration(),
            entity,
            () -> Executors.newCachedThreadPool());
  }

  @After
  public void tearDown() throws IOException {
    client.close();
    conn.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetPartitions() {
    List<HBasePartition> partitions = (List) reader.getPartitions(-1, 1);
    assertEquals(partitions.toString(), 3, partitions.size());
    partitions.forEach(
        p -> {
          assertEquals(0, p.getStartStamp());
          assertEquals(1, p.getEndStamp());
        });

    assertEquals("", new String(partitions.get(0).getStartKey()));
    assertEquals("first", new String(partitions.get(0).getEndKey()));
    assertEquals("first", new String(partitions.get(1).getStartKey()));
    assertEquals("second", new String(partitions.get(1).getEndKey()));
    assertEquals("second", new String(partitions.get(2).getStartKey()));
    assertEquals("", new String(partitions.get(2).getEndKey()));
  }

  @Test(timeout = 30000)
  public void testObserve() throws InterruptedException, IOException {

    long now = 1500000000000L;
    write("a", "dummy", "a", now);
    write("firs", "wildcard.1", "firs", now);
    write("fir", "dummy", "fir", now);
    write("first", "dummy", "first", now);

    List<Partition> partitions = reader.getPartitions();
    List<String> keys = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        partitions.subList(0, 1),
        Lists.newArrayList(attr),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element) {
            assertEquals(now, element.getStamp());
            keys.add(element.getKey());
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });
    latch.await();

    assertEquals(Arrays.asList("a", "fir"), keys);
  }

  @Test(timeout = 5000)
  public void testObserveCancel() throws InterruptedException, IOException {

    long now = 1500000000000L;
    write("a", "dummy", "a", now);
    write("firs", "wildcard.1", "firs", now);
    write("fir", "dummy", "fir", now);
    write("first", "dummy", "first", now);

    List<Partition> partitions = reader.getPartitions();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<ObserveHandle> handle = new AtomicReference<>();
    handle.set(
        reader.observe(
            partitions.subList(0, 1),
            Collections.singletonList(attr),
            new BatchLogObserver() {

              @Override
              public boolean onNext(StreamElement element) {
                handle.get().close();
                return true;
              }

              @Override
              public void onCancelled() {
                latch.countDown();
              }

              @Override
              public void onCompleted() {
                fail("onCompleted should not heve been called");
              }
            }));
    latch.await();
  }

  @Test(timeout = 30000)
  public void testObserveLast() throws InterruptedException, IOException {

    long now = 1500000000000L;
    write("secon", "dummy", "secon", now);
    write("second", "dummy", "second", now);
    write("third", "dummy", "third", now);

    List<Partition> partitions = reader.getPartitions();
    List<String> keys = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        partitions.subList(2, 3),
        Lists.newArrayList(attr),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element) {
            assertEquals(now, element.getStamp());
            keys.add(element.getKey());
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });
    latch.await();

    assertEquals(Lists.newArrayList("second", "third"), keys);
  }

  @Test(timeout = 30000)
  public void testObserveMultiple() throws IOException, InterruptedException {

    long now = 1500000000000L;
    write("a", "dummy", "a", now);
    write("firs", "wildcard.1", "firs", now);
    write("fir", "dummy", "fir", now);
    write("first", "dummy", "first", now);

    List<Partition> partitions = reader.getPartitions();
    List<String> keys = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    assertEquals(3, partitions.size());

    reader.observe(
        partitions.subList(0, 1),
        Lists.newArrayList(attr, wildcard),
        new BatchLogObserver() {

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onNext(StreamElement element) {
            assertEquals(now, element.getStamp());
            keys.add(element.getKey());
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });

    latch.await();

    assertEquals(Lists.newArrayList("a", "fir", "firs"), keys);
  }

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(reader.getUri(), ((HBaseLogReader) factory.apply(repo)).getUri());
  }

  private void write(String key, String attribute, String value, long stamp) throws IOException {
    HbaseTestUtil.write(key, attribute, value, stamp, client);
  }
}
