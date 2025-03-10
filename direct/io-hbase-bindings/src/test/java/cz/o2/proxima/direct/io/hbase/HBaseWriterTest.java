/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.hbase;

import static cz.o2.proxima.direct.io.hbase.HbaseTestUtil.bytes;
import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.OnlineAttributeWriter.Factory;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test {@code HBaseWriter} via a local instance of HBase cluster. */
public class HBaseWriterTest {

  private final Repository repo = ConfigRepository.Builder.ofTest(ConfigFactory.load()).build();
  private final EntityDescriptor entity = repo.getEntity("test");
  private final AttributeDescriptor<?> attr = entity.getAttribute("dummy");
  private final AttributeDescriptor<?> wildcard = entity.getAttribute("wildcard.*");

  private static MiniHBaseCluster cluster;
  private static HBaseTestingUtility util;
  private HBaseWriter writer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    util = new HBaseTestingUtility();
    cluster = util.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    cluster.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    util.createTable(TableName.valueOf("users"), bytes("u"));
    writer =
        new HBaseWriter(
            new URI("hbase://localhost:2181/users?family=u"),
            cluster.getConfiguration(),
            Collections.emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    util.deleteTable(TableName.valueOf("users"));
  }

  @Test
  public void testWriteIntoNotExistsTable() throws URISyntaxException {
    long now = 1500000000000L;
    HBaseWriter failWriter =
        new HBaseWriter(
            new URI("hbase://localhost:2181/not-exists?family=u"),
            cluster.getConfiguration(),
            Collections.emptyMap());
    failWriter.write(
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "entity", "dummy", now, new byte[] {1, 2}),
        ((success, error) -> {
          assertFalse(success);
        }));
  }

  @Test(timeout = 10000)
  public void testWrite() throws InterruptedException, IOException {
    CountDownLatch latch = new CountDownLatch(1);
    long now = 1500000000000L;
    writer.write(
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "entity", "dummy", now, new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue("Error on write: " + exc, succ);
          latch.countDown();
        });
    latch.await();
    Connection conn = ConnectionFactory.createConnection(cluster.getConfiguration());
    Table table = conn.getTable(TableName.valueOf("users"));
    Get get = new Get(bytes("entity"));
    Result res = table.get(get);
    NavigableMap<byte[], byte[]> familyMap = res.getFamilyMap(bytes("u"));
    assertEquals(1, familyMap.size());
    assertArrayEquals(new byte[] {1, 2}, familyMap.get(bytes("dummy")));
    assertEquals(
        now, (long) res.getMap().get(bytes("u")).get(bytes("dummy")).firstEntry().getKey());
  }

  @Test(timeout = 10000)
  public void testWriteDelete() throws InterruptedException, IOException {
    CountDownLatch latch = new CountDownLatch(2);
    long now = 1500000000000L;
    writer.write(
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "entity", "dummy", now, new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue("Error on write: " + exc, succ);
          latch.countDown();
        });
    writer.write(
        StreamElement.delete(
            entity, attr, UUID.randomUUID().toString(), "entity", "dummy", now + 1),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
          ;
        });
    latch.await();
    Connection conn = ConnectionFactory.createConnection(cluster.getConfiguration());
    Table table = conn.getTable(TableName.valueOf("users"));
    Get get = new Get(bytes("entity"));
    Result res = table.get(get);
    assertTrue(res.isEmpty());
  }

  @Test(timeout = 10000)
  public void testWriteDeleteWithLessTs() throws InterruptedException, IOException {
    CountDownLatch latch = new CountDownLatch(2);
    long now = 1500000000000L;
    writer.write(
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "entity", "dummy", now, new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue("Error on write: " + exc, succ);
          latch.countDown();
        });
    writer.write(
        StreamElement.delete(
            entity, attr, UUID.randomUUID().toString(), "entity", "dummy", now - 1),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
          ;
        });
    latch.await();
    Connection conn = ConnectionFactory.createConnection(cluster.getConfiguration());
    Table table = conn.getTable(TableName.valueOf("users"));
    Get get = new Get(bytes("entity"));
    Result res = table.get(get);
    assertFalse(res.isEmpty());
  }

  @Test(timeout = 10000)
  public void testWriteDeleteWildcard() throws InterruptedException, IOException {
    CountDownLatch latch = new CountDownLatch(4);
    long now = 1500000000000L;
    writer.write(
        StreamElement.upsert(
            entity,
            wildcard,
            UUID.randomUUID().toString(),
            "entity",
            wildcard.toAttributePrefix() + 1,
            now,
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue("Error on write: " + exc, succ);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            wildcard,
            UUID.randomUUID().toString(),
            "entity",
            wildcard.toAttributePrefix() + 2,
            now + 1,
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue("Error on write: " + exc, succ);
          latch.countDown();
        });
    writer.write(
        StreamElement.deleteWildcard(
            entity, wildcard, UUID.randomUUID().toString(), "entity", now + 2),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
          ;
        });
    writer.write(
        StreamElement.upsert(
            entity,
            wildcard,
            UUID.randomUUID().toString(),
            "entity",
            wildcard.toAttributePrefix() + 3,
            now + 3,
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue("Error on write: " + exc, succ);
          latch.countDown();
        });
    latch.await();
    Connection conn = ConnectionFactory.createConnection(cluster.getConfiguration());
    Table table = conn.getTable(TableName.valueOf("users"));
    Get get = new Get(bytes("entity"));
    Result res = table.get(get);
    NavigableMap<byte[], byte[]> familyMap = res.getFamilyMap(bytes("u"));
    assertEquals(
        "Expected single key in "
            + familyMap.keySet().stream()
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .collect(Collectors.toList()),
        1,
        familyMap.size());
    assertArrayEquals(new byte[] {1, 2}, familyMap.get(bytes(wildcard.toAttributePrefix() + 3)));
    assertEquals(
        now + 3,
        (long)
            res.getMap()
                .get(bytes("u"))
                .get(bytes(wildcard.toAttributePrefix() + 3))
                .firstEntry()
                .getKey());
  }

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(writer.getUri(), ((HBaseWriter) factory.apply(repo)).getUri());
  }

  @Test
  public void testWriteGetObserve() throws InterruptedException {
    URI uri = URI.create("hbase://localhost:2181/users?family=u");
    HBaseWriter writer = new HBaseWriter(uri, cluster.getConfiguration(), Collections.emptyMap());
    HBaseLogReader reader =
        new HBaseLogReader(uri, cluster.getConfiguration(), entity, Executors::newCachedThreadPool);
    RandomHBaseReader randomReader =
        new RandomHBaseReader(uri, cluster.getConfiguration(), Collections.emptyMap(), entity);

    StreamElement write =
        StreamElement.upsert(
            entity,
            wildcard,
            UUID.randomUUID().toString(),
            "key",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    CountDownLatch writeLatch = new CountDownLatch(1);
    writer.write(write, (succ, exc) -> writeLatch.countDown());
    writeLatch.await();

    List<StreamElement> el = new ArrayList<>();
    CountDownLatch observeLatch = new CountDownLatch(1);
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(wildcard),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            el.add(element);
            return true;
          }

          @Override
          public void onCompleted() {
            observeLatch.countDown();
          }
        });
    observeLatch.await();

    assertEquals(1, el.size());
    assertEquals(write.getKey(), el.get(0).getKey());
    assertEquals(write.getAttribute(), el.get(0).getAttribute());
    assertArrayEquals(write.getValue(), el.get(0).getValue());

    Optional<? extends KeyValue<?>> kv =
        randomReader.get("key", wildcard.toAttributePrefix() + "1", wildcard, Long.MAX_VALUE);
    assertTrue(kv.isPresent());
    assertEquals(write.getKey(), el.get(0).getKey());
    assertEquals(write.getAttribute(), el.get(0).getAttribute());
    assertArrayEquals(write.getValue(), el.get(0).getValue());
  }

  @Test
  public void testWriteGetObserveV2() throws InterruptedException {
    URI uri = URI.create("hbase://localhost:2181/users?family=u&v=2");
    HBaseWriter writer = new HBaseWriter(uri, cluster.getConfiguration(), Collections.emptyMap());
    HBaseLogReader reader =
        new HBaseLogReader(uri, cluster.getConfiguration(), entity, Executors::newCachedThreadPool);
    RandomHBaseReader randomReader =
        new RandomHBaseReader(uri, cluster.getConfiguration(), Collections.emptyMap(), entity);

    StreamElement write =
        StreamElement.upsert(
            entity,
            wildcard,
            1L,
            "key",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    CountDownLatch writeLatch = new CountDownLatch(1);
    writer.write(write, (succ, exc) -> writeLatch.countDown());
    writeLatch.await();

    List<StreamElement> el = new ArrayList<>();
    CountDownLatch observeLatch = new CountDownLatch(1);
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(wildcard),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            el.add(element);
            return true;
          }

          @Override
          public void onCompleted() {
            observeLatch.countDown();
          }
        });
    observeLatch.await();

    assertEquals(1, el.size());
    assertEquals(write.getKey(), el.get(0).getKey());
    assertEquals(write.getAttribute(), el.get(0).getAttribute());
    assertArrayEquals(write.getValue(), el.get(0).getValue());
    assertEquals(1L, el.get(0).getSequentialId());

    Optional<? extends KeyValue<?>> kv =
        randomReader.get("key", wildcard.toAttributePrefix() + "1", wildcard, Long.MAX_VALUE);
    assertTrue(kv.isPresent());
    assertEquals(write.getKey(), kv.get().getKey());
    assertEquals(write.getAttribute(), kv.get().getAttribute());
    assertArrayEquals(write.getValue(), kv.get().getValue());
    assertEquals(1L, kv.get().getSequentialId());
  }
}
