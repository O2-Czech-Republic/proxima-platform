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
package cz.o2.proxima.tools.groovy;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
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
  public void testArgumentParsingNoOutput() throws Exception {
    File config = cloneConfig();
    new Compiler(new String[] {"-p", "cz.o2.proxima.tools", config.getAbsolutePath()});
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

  @Test(expected = IllegalArgumentException.class)
  public void testArgumentParsingWithOutpuFileAsDir() throws Exception {
    File output = folder.newFile();
    output.mkdirs();
    File config = cloneConfig();
    Compiler compiler =
        new Compiler(
            new String[] {
              "-o",
              new File(output.getAbsolutePath(), File.pathSeparator).getAbsolutePath(),
              "-p",
              "cz.o2.proxima.tools",
              config.getAbsolutePath()
            });
    compiler.run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArgumentParsingNonExistingConfig() throws Exception {
    File output = folder.newFile();
    File config = new File(folder.newFolder(), UUID.randomUUID().toString());
    assertFalse(config.exists());
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

  @Test
  public void testMain() throws Exception {
    File output = folder.newFile();
    File config = cloneConfig();
    Compiler.main(new String[] {"-o", output.getAbsolutePath(), config.getAbsolutePath()});
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
