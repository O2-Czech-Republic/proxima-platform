/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@code HBaseWriter} via a local instance of HBase cluster.
 */
public class HBaseWriterTest {

  final Repository repo = Repository.Builder.ofTest(ConfigFactory.load()).build();
  final EntityDescriptor entity = repo.findEntity("test").get();
  final AttributeDescriptor<?> attr = entity.findAttribute("dummy").get();

  HBaseTestingUtility util;
  MiniHBaseCluster cluster;
  HBaseWriter writer;

  @Before
  public void setUp() throws Exception {
    util = HBaseTestingUtility.createLocalHTU();
    cluster = util.startMiniCluster();
    util.createTable(TableName.valueOf("users"), "u".getBytes("UTF-8"));
    writer = new HBaseWriter(
        new URI("hbase://localhost:2181/users?family=u"),
        cluster.getConfiguration(),
        Collections.emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test(timeout = 10000)
  public void testWrite() throws InterruptedException, IOException {
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "entity", "dummy", System.currentTimeMillis(), new byte[] { 1, 2 }),
        (succ, exc) -> {
          assertTrue("Error on write: " + exc, succ);
          latch.countDown();
        });
    latch.await();
    Connection conn = ConnectionFactory.createConnection(cluster.getConfiguration());
    Table table = conn.getTable(TableName.valueOf("users"));
    Get get = new Get("entity".getBytes("UTF-8"));
    Result res = table.get(get);
    NavigableMap<byte[], byte[]> familyMap = res.getFamilyMap("u".getBytes("UTF-8"));
    assertEquals(1, familyMap.size());
    assertArrayEquals(new byte[] { 1, 2 }, familyMap.get("dummy".getBytes("UTF-8")));
  }

}
