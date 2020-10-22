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
package cz.o2.proxima.direct.hadoop;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HadoopPathTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private final Repository repository =
      ConfigRepository.Builder.ofTest(ConfigFactory.load("test-reference.conf").resolve()).build();
  private final DirectDataOperator direct =
      repository.getOrCreateOperator(DirectDataOperator.class);;
  private final EntityDescriptor entity = repository.getEntity("gateway");
  private URI uri;
  private HadoopDataAccessor accessor;

  @Before
  public void setUp() throws IOException {
    uri = URI.create("file://" + folder.newFolder().getAbsolutePath());
    accessor = new HadoopDataAccessor(entity, uri, Collections.emptyMap());
  }

  @Test
  public void testPathRenameRemotely() throws IOException {
    HadoopFileSystem fs = new HadoopFileSystem(accessor);
    HadoopPath path1 = (HadoopPath) fs.newPath(System.currentTimeMillis());
    HadoopPath path2 = (HadoopPath) fs.newPath(System.currentTimeMillis());
    try (OutputStream os = path1.writer()) {
      os.write(new byte[] {1});
    }
    path1.renameOnFs(path2);
    assertEquals(1, path2.getFileStatus().getLen());
    try {
      path1.getFileStatus();
      fail("Should have thrown exception!");
    } catch (FileNotFoundException ex) {
      // pass
    }
  }

  @Test
  public void testPathCopyToRemote() throws IOException {
    HadoopFileSystem fs = new HadoopFileSystem(accessor);
    HadoopPath path1 = (HadoopPath) fs.newPath(System.currentTimeMillis());
    HadoopPath path2 = (HadoopPath) fs.newPath(System.currentTimeMillis());
    try (OutputStream os = path1.writer()) {
      os.write(new byte[] {1});
    }
    path1.moveToRemote(path2);
    assertEquals(1, path2.getFileStatus().getLen());
    try {
      path1.getFileStatus();
      fail("Should have thrown exception!");
    } catch (FileNotFoundException ex) {
      // pass
    }
  }

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    HadoopFileSystem fs = new HadoopFileSystem(accessor);
    HadoopPath path1 = (HadoopPath) fs.newPath(System.currentTimeMillis());
    HadoopPath path2 = TestUtils.assertSerializable(path1);
    TestUtils.assertHashCodeAndEquals(path1, path2);
  }

  @Test
  public void testPathCompare() {
    HadoopFileSystem fs = new HadoopFileSystem(accessor);
    long now = System.currentTimeMillis();
    HadoopPath path1 = (HadoopPath) fs.newPath(now);
    HadoopPath path2 = (HadoopPath) fs.newPath(now + 2 * accessor.getRollInterval());
    assertTrue(path1.compareTo(path2) < 0);
    assertTrue(path2.compareTo(path1) > 0);
  }
}
