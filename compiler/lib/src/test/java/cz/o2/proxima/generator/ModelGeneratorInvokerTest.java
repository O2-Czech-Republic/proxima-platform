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
package cz.o2.proxima.generator;

import static org.junit.Assert.assertFalse;

import freemarker.template.TemplateException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ModelGeneratorInvokerTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testRequiredArgs() throws TemplateException, IOException {
    StringWriter writer = new StringWriter();
    InputStream input = getClass().getClassLoader().getResourceAsStream("test-reference.conf");
    File cfgFile = folder.newFile();
    try (OutputStream os = new FileOutputStream(cfgFile)) {
      IOUtils.copy(input, os);
    }
    ModelGeneratorInvoker.of(
            new String[] {
              "-p",
              "cz.o2.proxima.model",
              "-t",
              folder.newFolder().getAbsolutePath(),
              "-f",
              cfgFile.getAbsolutePath()
            })
        .generate(writer);
    assertFalse(writer.toString().isEmpty());
  }
}
