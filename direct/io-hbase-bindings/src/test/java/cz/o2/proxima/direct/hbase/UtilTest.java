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
package cz.o2.proxima.direct.hbase;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

/** Tests for {@link Util}. */
public class UtilTest {
  @Test
  public void testParseValidUri() throws URISyntaxException {
    URI uri = new URI("hbase://localhost:2181/table?family=f");
    assertEquals("table", Util.getTable(uri));
    assertEquals("f", new String(Util.getFamily(uri)));
    Configuration conf = Util.getConf(uri);
    assertEquals("localhost:2181", conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT, conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithMissingFamily() throws URISyntaxException {
    URI uri = new URI("hbase://localhost:2181/table");
    Util.getFamily(uri);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithMissingTable() throws URISyntaxException {
    URI uri = new URI("hbase://localhost:2181/");
    Util.getTable(uri);
  }

  @Test
  public void testWithCustomZookeeperParentNode() throws URISyntaxException {
    URI uri = new URI("hbase://localhost:2181/hbase-secure/table?family=f");
    assertEquals("table", Util.getTable(uri));
    assertEquals("f", new String(Util.getFamily(uri)));
    Configuration conf = Util.getConf(uri);
    assertEquals("localhost:2181", conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals("hbase-secure", conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    uri = new URI("hbase://localhost:2181/hbase-secure/my/zookeeper/table?family=f");
    assertEquals("table", Util.getTable(uri));
    assertEquals("f", new String(Util.getFamily(uri)));
    conf = Util.getConf(uri);
    assertEquals("localhost:2181", conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals("hbase-secure/my/zookeeper", conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }

  @Test
  public void testCloneArray() {
    byte[] value = "my-value".getBytes();
    assertEquals("my", new String(Util.cloneArray(value, 0, 2)));
    assertEquals("value", new String(Util.cloneArray(value, 3, 5)));
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testCloneArrayOutOfBound() {
    Util.cloneArray(new byte[] {1, 2, 3}, 0, 10);
  }
}
