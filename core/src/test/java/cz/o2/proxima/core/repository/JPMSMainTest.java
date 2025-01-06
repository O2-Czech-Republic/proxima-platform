/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.repository;

import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.util.internal.ClassLoaders;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JPMSMainTest {

  static class Main {

    private static AtomicReference<String[]> passedArgs;

    public static void main(String[] args) {
      passedArgs = new AtomicReference<>(args);
    }
  }

  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testRun() throws Exception {
    File folder = tmpFolder.newFolder();
    createEmptyJar(folder);
    JPMSMain.main(new String[] {folder.getAbsolutePath(), Main.class.getName()});
    assertTrue(
        Thread.currentThread().getContextClassLoader()
            instanceof ClassLoaders.ChildLayerFirstClassLoader);
  }

  private void createEmptyJar(File folder) {
    File jar = new File(folder, "empty.jar");
    try (var file = new FileOutputStream(jar);
        var stream = new JarOutputStream(file)) {
      ZipEntry entry = new JarEntry("empty");
      stream.putNextEntry(entry);
      stream.closeEntry();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
