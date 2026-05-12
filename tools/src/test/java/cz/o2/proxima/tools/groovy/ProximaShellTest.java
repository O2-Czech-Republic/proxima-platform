/*
 * Copyright 2017-2026 O2 Czech Republic, a.s.
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

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ProximaShellTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testScriptRun() throws Exception {
    String script = "return env";
    File scriptFile = folder.newFile();
    Files.writeString(scriptFile.toPath(), script);
    Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    ProximaShell shell = new ProximaShell(repo, new String[] {scriptFile.getAbsolutePath()});
    Object env = shell.run(scriptFile);
    assertEquals("Environment", env.getClass().getSimpleName());
  }
}
