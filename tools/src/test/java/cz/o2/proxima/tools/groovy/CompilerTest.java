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
package cz.o2.proxima.tools.groovy;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test suite for {@link Compiler}. */
public class CompilerTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test(expected = IllegalArgumentException.class)
  public void testArgumentParsingNoConfig() throws Exception {
    File output = folder.newFile();
    new Compiler(new String[] {"-o", output.getAbsolutePath(), "-p", "cz.o2.proxima.tools"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArgumentParsingWithOutputDir() throws Exception {
    File output = folder.newFolder();
    File config = cloneConfig();
    Compiler compiler =
        new Compiler(
            new String[] {
              "-o", output.getAbsolutePath(), "-p", "cz.o2.proxima.tools", config.getAbsolutePath()
            });
    compiler.run();
  }

  @Test
  public void testArgumentParsing() throws Exception {
    File output = folder.newFile();
    File config = cloneConfig();
    Compiler compiler =
        new Compiler(
            new String[] {
              "-o", output.getAbsolutePath(), "-p", "cz.o2.proxima.tools", config.getAbsolutePath()
            });
    compiler.run();
    assertFalse(output.isDirectory());
    try (FileInputStream fin = new FileInputStream(output)) {
      String contents = IOUtils.toString(fin, StandardCharsets.UTF_8);
      assertFalse(contents.isEmpty());
    }
  }

  private File cloneConfig() throws IOException {
    File ret = folder.newFile();
    try (FileOutputStream fos = new FileOutputStream(ret)) {
      IOUtils.copy(
          Thread.currentThread().getContextClassLoader().getResourceAsStream("test-reference.conf"),
          fos);
    }
    return ret;
  }
}
