/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
import java.io.StringWriter;
import org.junit.Test;

/** Briefly test {@link ModelGenerator}. */
public class ModelGeneratorTest {

  @Test
  public void testModelGeneration() throws Exception {
    ModelGenerator generator = new ModelGenerator("test", "Test", "ignored", "ignored", false);
    StringWriter writer = new StringWriter();
    generator.generate(ConfigFactory.load("test-reference.conf"), writer);
    assertFalse(writer.toString().isEmpty());
    // validate we have some imports from direct submodule
    assertTrue(writer.toString().contains(".direct."));
    // and that we have some code related to CommitLogReader
    assertTrue(writer.toString().contains("CommitLogReader"));
  }
}
