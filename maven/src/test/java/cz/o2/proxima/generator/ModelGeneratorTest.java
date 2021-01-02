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
package cz.o2.proxima.generator;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.StringWriter;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Briefly test {@link ModelGenerator}. */
public class ModelGeneratorTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder(new File("./target/"));

  @Test
  public void testModelGeneration() throws Exception {
    ModelGenerator generator =
        new ModelGenerator("test", "Test", new File("ignored"), new File("ignored"), false);
    StringWriter writer = new StringWriter();
    generator.generate(ConfigFactory.load("test-reference.conf"), writer);
    assertFalse(writer.toString().isEmpty());
    // validate we have some imports from direct submodule
    assertTrue(writer.toString().contains(".direct."));
    // and that we have some code related to CommitLogReader
    assertTrue(writer.toString().contains("CommitLogReader"));
  }

  @Test
  public void testModelGenerationWithFilenames() throws Exception {
    File config = folder.newFile().getAbsoluteFile();
    File outputFolder = folder.newFolder().getAbsoluteFile();
    IOUtils.copy(
        Thread.currentThread().getContextClassLoader().getResourceAsStream("test-reference.conf"),
        new FileOutputStream(config));
    ModelGenerator generator = new ModelGenerator("test", "Test", config, outputFolder, true);
    generator.generate();
    assertTrue(config.exists());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testModelGenerationMissingConfig() throws Exception {
    File config = new File("/tmp/" + UUID.randomUUID().toString());
    File outputFolder = folder.newFolder().getAbsoluteFile();
    ModelGenerator generator = new ModelGenerator("test", "Test", config, outputFolder, true);
    generator.generate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testModelGenerationWithFilenamesNotAbsolute() throws Exception {
    File config = folder.newFile().getAbsoluteFile();
    File outputFolder = folder.newFolder();
    IOUtils.copy(
        Thread.currentThread().getContextClassLoader().getResourceAsStream("test-reference.conf"),
        new FileOutputStream(config));
    new ModelGenerator("test", "Test", config, outputFolder, true);
  }

  @Test(expected = IllegalStateException.class)
  public void testModelGenerationWithFilenamesNotFolder() throws Exception {
    File config = folder.newFile().getAbsoluteFile();
    File outputFolder = folder.newFile().getAbsoluteFile();
    IOUtils.copy(
        Thread.currentThread().getContextClassLoader().getResourceAsStream("test-reference.conf"),
        new FileOutputStream(config));
    ModelGenerator generator = new ModelGenerator("test", "Test", config, outputFolder, true);
    generator.generate();
  }
}
