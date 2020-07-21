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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader.Factory;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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

/** Testing suite for {@link RandomHBaseReader}. */
public class RandomHBaseReaderTest {

  private final Repository repo = ConfigRepository.Builder.ofTest(ConfigFactory.load()).build();
  private final EntityDescriptor entity = repo.findEntity("test").get();

  @SuppressWarnings("unchecked")
  private final AttributeDescriptor<byte[]> attr = entity.<byte[]>findAttribute("dummy").get();

  @SuppressWarnings("unchecked")
  private final AttributeDescriptor<byte[]> wildcard =
      entity.<byte[]>findAttribute("wildcard.*").get();

  private final TableName tableName = TableName.valueOf("test");

  private static HBaseTestingUtility util;
  private static MiniHBaseCluster cluster;
  private RandomHBaseReader reader;
  private Connection conn;
  private Table client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    try {
      util = new HBaseTestingUtility();
      cluster = util.startMiniCluster();
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }
  }

  @AfterClass
  public static void afterClass() throws IOException {
    cluster.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    util.createTable(tableName, bytes("u"));
    conn = ConnectionFactory.createConnection(util.getConfiguration());
    client = conn.getTable(tableName);

    reader =
        new RandomHBaseReader(
            new URI("hbase://localhost:2181/test?family=u"),
            cluster.getConfiguration(),
            Collections.emptyMap(),
            entity);
  }

  @After
  public void tearDown() throws IOException {
    util.deleteTable(tableName);
    client.close();
    conn.close();
  }

  @Test
  public void testRandomGet() throws IOException {
    long now = 1500000000000L;
    write("key", "dummy", "value", now);
    Optional<KeyValue<byte[]>> res = reader.get("key", attr);
    assertTrue(res.isPresent());
    assertEquals("key", res.get().getKey());
    assertArrayEquals(bytes("value"), res.get().getValue());
    assertEquals(attr, res.get().getAttributeDescriptor());
    assertEquals(now, res.get().getStamp());
  }

  @Test
  public void testRandomGetWildcard() throws IOException {
    long now = 1500000000000L;
    write("key", "wildcard.12345", "value", now);
    Optional<KeyValue<byte[]>> res = reader.get("key", "wildcard.12345", wildcard);
    assertTrue(res.isPresent());
    assertEquals("key", res.get().getKey());
    assertArrayEquals(bytes("value"), res.get().getValue());
    assertEquals("wildcard.12345", res.get().getAttribute());
    assertEquals(wildcard, res.get().getAttributeDescriptor());
    assertEquals(now, res.get().getStamp());
  }

  @Test
  public void testScanWildcard() throws IOException {
    long now = 1500000000000L;
    // write several values, delibetarely written in descending order
    write("key", "wildcard.12345", "value1", now);
    write("key", "wildcard.1234", "value2", now + 1);
    write("key", "wildcard.123", "value3", now + 2);
    write("key", "wildcard.12", "value4", now + 3);
    write("key", "wildcard.1", "value5", now + 4);
    write("key", "wildcard.0", "value6", now + 5);

    // include some "garbage"
    write("key", "dummy", "blah", now + 6);
    write("key2", "wildcard.1", "blah", now + 7);

    List<KeyValue<?>> res = new ArrayList<>();
    reader.scanWildcard("key", wildcard, res::add);
    assertEquals(6, res.size());

    // all results are from the same key
    assertEquals(
        Collections.singleton("key"),
        res.stream().map(KeyValue::getKey).collect(Collectors.toSet()));

    // all results have the same attribute descriptor
    assertEquals(
        Collections.singleton(wildcard),
        res.stream().map(KeyValue::getAttributeDescriptor).collect(Collectors.toSet()));

    // check the attributes, including ordering
    assertEquals(
        Arrays.asList(
            "wildcard.0",
            "wildcard.1",
            "wildcard.12",
            "wildcard.123",
            "wildcard.1234",
            "wildcard.12345"),
        res.stream().map(KeyValue::getAttribute).collect(Collectors.toList()));

    // check values
    assertEquals(
        Arrays.asList("value6", "value5", "value4", "value3", "value2", "value1"),
        res.stream().map(k -> new String(k.getValue())).collect(Collectors.toList()));

    res.stream().map(KeyValue::getStamp).forEach(ts -> assertTrue(ts >= now));
    res.stream().map(KeyValue::getStamp).forEach(ts -> assertTrue(ts < now + 10));
  }

  @Test
  public void testListWildcardWithOffset() throws IOException {
    long now = 1500000000000L;
    // write several values, delibetarely written in descending order
    write("key", "wildcard.12345", "value1", now);
    write("key", "wildcard.1234", "value2", now + 1);
    write("key", "wildcard.123", "value3", now + 2);
    write("key", "wildcard.12", "value4", now + 3);
    write("key", "wildcard.1", "value5", now + 4);
    write("key", "wildcard.0", "value6", now + 5);

    // include some "garbage"
    write("key", "dummy", "blah", now + 6);
    write("key2", "wildcard.1", "blah", now + 7);

    List<KeyValue<?>> res = new ArrayList<>();
    reader.scanWildcard("key", wildcard, null, 1, res::add);
    assertEquals(1, res.size());
    assertEquals("value6", new String(res.get(0).getValue()));

    RandomOffset offset = res.get(0).getOffset();
    res.clear();
    reader.scanWildcard("key", wildcard, offset, 3, res::add);
    assertEquals(3, res.size());

    // all results are from the same key
    assertEquals(
        Collections.singleton("key"),
        res.stream().map(KeyValue::getKey).collect(Collectors.toSet()));

    // all results have the same attribute descriptor
    assertEquals(
        Collections.singleton(wildcard),
        res.stream().map(KeyValue::getAttributeDescriptor).collect(Collectors.toSet()));

    // check the attributes, including ordering
    assertEquals(
        Arrays.asList("value5", "value4", "value3"),
        res.stream().map(k -> new String(k.getValue())).collect(Collectors.toList()));

    // check values
    assertEquals(
        Arrays.asList("wildcard.1", "wildcard.12", "wildcard.123"),
        res.stream().map(KeyValue::getAttribute).collect(Collectors.toList()));

    res.stream().map(KeyValue::getStamp).forEach(ts -> assertTrue(ts >= now));
    res.stream().map(KeyValue::getStamp).forEach(ts -> assertTrue(ts < now + 10));
  }

  @Test
  public void testListKeys() throws IOException {
    long now = 1500000000000L;
    write("key1", "dummy", "a", now);
    write("key2", "wildcard.1", "b", now);
    write("key0", "dummy", "c", now);

    List<String> keys = new ArrayList<>();
    reader.listEntities(p -> keys.add(p.getSecond()));
    assertEquals(Arrays.asList("key0", "key1", "key2"), keys);
  }

  @Test
  public void testListKeysOffset() throws IOException {
    long now = 1500000000000L;
    write("key1", "dummy", "a", now);
    write("key2", "wildcard.1", "b", now);
    write("key0", "dummy", "c", now);

    List<Pair<RandomOffset, String>> keys = new ArrayList<>();
    reader.listEntities(null, 1, keys::add);
    assertEquals(1, keys.size());
    RandomOffset off = keys.get(0).getFirst();
    keys.clear();
    reader.listEntities(off, 1, keys::add);
    assertEquals("key1", keys.get(0).getSecond());
  }

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(reader.getUri(), ((RandomHBaseReader) factory.apply(repo)).getUri());
  }

  void write(String key, String attribute, String value, long stamp) throws IOException {

    HbaseTestUtil.write(key, attribute, value, stamp, client);
  }
}
