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
package cz.o2.proxima.direct.compiler;

import static org.junit.Assert.assertNotNull;

import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.testing.model.CoreModel;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import org.junit.Test;

public class CompilerTest {

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    CoreModel model = CoreModel.of(ConfigFactory.empty());
    assertNotNull(TestUtils.deserializeObject(TestUtils.serializeObject(model)));
  }
}
