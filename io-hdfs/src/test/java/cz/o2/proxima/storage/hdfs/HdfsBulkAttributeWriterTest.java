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
package cz.o2.proxima.storage.hdfs;

import com.google.common.collect.Maps;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple tests for {@code HdfsBulkAttributeWriter}.
 */
public class HdfsBulkAttributeWriterTest {

  private HdfsBulkAttributeWriter writer;

  @Before
  public void setUp() throws URISyntaxException {
    writer = new HdfsBulkAttributeWriter(
        EntityDescriptor.newBuilder().setName("dummy").build(),
        new URI("file://dummy/dir"), Maps.newHashMap(),
    HdfsDataAccessor.HDFS_MIN_ELEMENTS_TO_FLUSH_DEFAULT, HdfsDataAccessor.HDFS_ROLL_INTERVAL_DEFAULT);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testTempPathGeneration() throws UnknownHostException, URISyntaxException {
    Path tmp = writer.toTmpLocation(1500000000000L);
    assertEquals(
        "file://dummy/dir/.tmp/part-1500000000000-"
            + InetAddress.getLocalHost().getCanonicalHostName(),
        tmp.toUri().toString());
  }

  @Test
  public void testFinalPathGeneration() throws UnknownHostException, URISyntaxException {
    Path tmp = writer.toFinalLocation(1500000000000L, 1499999999000L, 1500000001000L);
    assertEquals(
        "file://dummy/dir/2017/07/part-1499999999000_1500000001000-"
            + InetAddress.getLocalHost().getCanonicalHostName(),
        tmp.toUri().toString());
  }

  @Test
  public void testFinalPathGeneration2017_12_31() throws UnknownHostException, URISyntaxException {
    Path tmp = writer.toFinalLocation(1514761200000L, 1514761200000L, 1514761200000L + 1000L);
    assertEquals(
        "file://dummy/dir/2017/12/part-1514761200000_1514761201000-"
            + InetAddress.getLocalHost().getCanonicalHostName(),
        tmp.toUri().toString());
  }

  @Test
  public void testPartParsing() {
    String part = "part-part-1499999999000_1500000001000-localhost.local";
    assertEquals(
        Maps.immutableEntry(1499999999000L, 1500000001000L),
        HdfsBatchLogObservable.getMinMaxStamp(part));
  }

}
