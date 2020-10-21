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
package cz.o2.proxima.direct.bulk;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test {@link LocalPath}. */
public class LocalPathTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testWriteAndRead() throws IOException {
    NamingConvention convention =
        NamingConvention.defaultConvention(Duration.ofHours(1), "prefix", "suffix");
    File root = folder.newFolder();
    LocalPath path = Path.local(FileSystem.local(root, convention), new File("./test"));
    assertEquals("file", path.getFileSystem().getUri().getScheme());
    path.write().write(ByteBuffer.wrap(new byte[] {2}));
    ByteBuffer buf = ByteBuffer.allocate(1);
    assertEquals(1, path.read().read(buf));
    assertEquals(2, buf.get(0));
    path.delete();
    assertFalse(new File(root, "./test").exists());
    // check not throws
    path.delete();
  }

  @Test
  public void testPathCompare() throws IOException {
    NamingConvention convention =
        NamingConvention.defaultConvention(Duration.ofHours(1), "prefix", "suffix");
    File root = folder.newFolder();
    LocalPath path1 = Path.local(FileSystem.local(root, convention), new File("./test"));
    LocalPath path2 = Path.local(FileSystem.local(root, convention), new File("./test2"));
    assertTrue(path1.compareTo(path2) < 0);
  }
}
