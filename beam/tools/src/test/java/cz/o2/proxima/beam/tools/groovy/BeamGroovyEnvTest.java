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
package cz.o2.proxima.beam.tools.groovy;

import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.tools.groovy.GroovyEnvTest;

/**
 * Test {@link GroovyEnv} using beam.
 */
public class BeamGroovyEnvTest extends GroovyEnvTest {

  private final DirectDataOperator direct = getRepo().getOrCreateOperator(
      DirectDataOperator.class);

  @Override
  protected void write(StreamElement element) {
    direct
        .getWriter(element.getAttributeDescriptor())
        .orElseThrow(() -> new IllegalStateException(
            "Missing writer for " + element.getAttributeDescriptor()))
        .write(element, (succ, exc) -> { });
  }

}
