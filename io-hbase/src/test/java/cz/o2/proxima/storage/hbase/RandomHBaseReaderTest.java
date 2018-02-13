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
package cz.o2.proxima.storage.hbase;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import static cz.o2.proxima.storage.hbase.TestUtil.b;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomOffset;
import cz.o2.proxima.util.Pair;
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
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing suite for {@link RandomHBaseReader}.
 */
public class RandomHBaseReaderTest {

  private final Repository repo = Repository.Builder.ofTest(ConfigFactory.load()).build();
  private final EntityDescriptor entity = repo.findEntity("test").get();
  @SuppressWarnings("unchecked")
  private final AttributeDescriptor<byte[]> attr = (AttributeDescriptor) entity.findAttribute("dummy").get();
  @SuppressWarnings("unchecked")
  private final AttributeDescriptor<byte[]> wildcard = (AttributeDescriptor) entity.findAttribute("wildcard.*").get();
  private final TableName tableName = TableName.valueOf("test");

  private HBaseTestingUtility util;
  private MiniHBaseCluster cluster;
  private RandomHBaseReader reader;
  private Connection conn;
  private Table client;

  @Before
  public void setUp() throws Exception {
    util = HBaseTestingUtility.createLocalHTU();
    cluster = util.startMiniCluster();
    util.createTable(tableName, b("u"));
    conn = ConnectionFactory.createConnection(util.getConfiguration());
    client = conn.getTable(tableName);

    reader = new RandomHBaseReader(
        new URI("hbase://localhost:2181/test?family=u"),
        cluster.getConfiguration(),
        Collections.emptyMap(),
        entity);
  }

  @After
  public void tearDown() throws IOException {
    client.close();
    conn.close();
  }

  @Test
  public void testRandomGet() throws IOException {
    write("key", "dummy", "value");
    Optional<KeyValue<byte[]>> res = reader.get("key", attr);
    assertTrue(res.isPresent());
    assertEquals("key", res.get().getKey());
    assertArrayEquals(b("value"), res.get().getValueBytes());
    assertEquals(attr, res.get().getAttrDescriptor());
  }

  @Test
  public void testRandomGetWildcard() throws IOException {
    write("key", "wildcard.12345", "value");
    Optional<KeyValue<byte[]>> res = reader.get("key", "wildcard.12345", wildcard);
    assertTrue(res.isPresent());
    assertEquals("key", res.get().getKey());
    assertArrayEquals(b("value"), res.get().getValueBytes());
    assertEquals("wildcard.12345", res.get().getAttribute());
    assertEquals(wildcard, res.get().getAttrDescriptor());
  }

  @Test
  public void testListWildcard() throws IOException {
    // write several values, delibetarely written in descending order
    write("key", "wildcard.12345", "value1");
    write("key", "wildcard.1234", "value2");
    write("key", "wildcard.123", "value3");
    write("key", "wildcard.12", "value4");
    write("key", "wildcard.1", "value5");
    write("key", "wildcard.0", "value6");

    // include some "garbage"
    write("key", "dummy", "blah");
    write("key2", "wildcard.1", "blah");

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
        res.stream().map(KeyValue::getAttrDescriptor).collect(Collectors.toSet()));

    // check the attributes, including ordering
    assertEquals(
        Arrays.asList(
            "wildcard.0", "wildcard.1", "wildcard.12",
            "wildcard.123", "wildcard.1234", "wildcard.12345"),
        res.stream().map(KeyValue::getAttribute).collect(Collectors.toList()));

    // check values
    assertEquals(
        Arrays.asList(
            "value6", "value5", "value4",
            "value3", "value2", "value1"),
        res.stream()
            .map(k -> new String(k.getValueBytes()))
            .collect(Collectors.toList()));
  }

  @Test
  public void testListWildcardWithOffset() throws IOException {
    // write several values, delibetarely written in descending order
    write("key", "wildcard.12345", "value1");
    write("key", "wildcard.1234", "value2");
    write("key", "wildcard.123", "value3");
    write("key", "wildcard.12", "value4");
    write("key", "wildcard.1", "value5");
    write("key", "wildcard.0", "value6");

    // include some "garbage"
    write("key", "dummy", "blah");
    write("key2", "wildcard.1", "blah");

    List<KeyValue<?>> res = new ArrayList<>();
    reader.scanWildcard("key", wildcard, null, 1, res::add);
    assertEquals(1, res.size());

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
        res.stream().map(KeyValue::getAttrDescriptor).collect(Collectors.toSet()));

    // check the attributes, including ordering
    assertEquals(
        Arrays.asList(
            "value5", "value4", "value3"),
        res.stream()
            .map(k -> new String(k.getValueBytes()))
            .collect(Collectors.toList()));

    // check values
    assertEquals(
        Arrays.asList(
            "wildcard.1", "wildcard.12", "wildcard.123"),
        res.stream().map(KeyValue::getAttribute).collect(Collectors.toList()));
  }

  @Test
  public void testListKeys() throws IOException {
    write("key1", "dummy", "a");
    write("key2", "wildcard.1", "b");
    write("key0", "dummy", "c");

    List<String> keys = new ArrayList<>();
    reader.listEntities(p -> keys.add(p.getSecond()));
    assertEquals(Arrays.asList("key0", "key1", "key2"), keys);
  }

  @Test
  public void testListKeysOffset() throws IOException {
    write("key1", "dummy", "a");
    write("key2", "wildcard.1", "b");
    write("key0", "dummy", "c");

    List<Pair<RandomOffset, String>> keys = new ArrayList<>();
    reader.listEntities(null, 1, keys::add);
    assertEquals(1, keys.size());
    RandomOffset off = keys.get(0).getFirst();
    keys.clear();
    reader.listEntities(off, 1, keys::add);
    assertEquals("key1", keys.get(0).getSecond());
  }

  void write(String key, String attribute, String value) throws IOException {
    TestUtil.write(key, attribute, value, client);
  }

}
