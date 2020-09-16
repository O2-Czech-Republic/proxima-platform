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
package cz.o2.proxima.direct.core;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.Repository;
import org.junit.Test;

public class StdoutStorageTest {

  private static final String CONFIG =
      "entities {\n"
          + "  entity {\n"
          + "    attributes {\n"
          + "      foo: { scheme: bytes }\n"
          + "    }\n"
          + "  }\n"
          + "}\n"
          + "attributeFamilies {\n"
          + "  scalar-primary {\n"
          + "    entity: entity\n"
          + "    attributes: [\"foo\"]\n"
          + "    storage: \"stdout:///\"\n"
          + "    type: primary\n"
          + "    access: commit-log\n"
          + "  }\n"
          + "}\n";

  @Test
  public void testLoadStorage() {
    Repository.ofTest(ConfigFactory.parseString(CONFIG))
        .getOrCreateOperator(DirectDataOperator.class);
  }
}
