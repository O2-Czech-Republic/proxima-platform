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
package cz.o2.proxima.direct.bulk;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test {@link FileSystem#local} */
public class FileSystemTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final FileFormat format = FileFormat.blob(true);
  FileSystem fs;

  @Before
  public void setUp() throws IOException {
    folder.create();
    fs =
        FileSystem.local(
            folder.newFolder(),
            NamingConvention.defaultConvention(Duration.ofHours(1), "prefix", format.fileSuffix()));
  }

  @After
  public void tearDown() {
    folder.delete();
  }

  @Test
  public void testCreateAndListLocalFileSystem() throws IOException {
    long now = 1500000000000L;
    // align start to hour boundary
    now = now - now % 3600000;
    Path p = fs.newPath(now);
    try (OutputStream stream = p.writer()) {
      stream.write(new byte[] {'b'});
    }
    p = fs.newPath(now + 7200000);
    try (OutputStream stream = p.writer()) {
      stream.write(new byte[] {'c'});
    }
    assertEquals(2L, fs.list().count());
    assertEquals(1L, fs.list(now, now + 1).count());
    assertEquals(0L, fs.list(now + 3600000, now + 7200000).count());
    assertEquals(1L, fs.list(now + 7200000, now + 7200001).count());

    try (InputStream reader = p.reader()) {
      int read = reader.read();
      assertTrue(read > 0);
    }
  }
}
