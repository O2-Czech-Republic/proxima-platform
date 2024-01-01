/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.groovy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.tools.groovy.ToolsClassLoader;
import groovy.lang.Closure;
import groovy.lang.Script;
import org.junit.Before;
import org.junit.Test;

/** Test {@link Types}. */
public class TypesTest {

  private ToolsClassLoader loader;

  @Before
  public void setUp() {
    loader = new ToolsClassLoader();
  }

  @Test
  public void testSimpleClosure() {
    Script s = compile("def a = { 1L }");
    Closure<?> closure = (Closure<?>) s.run();
    assertNotNull(Types.returnClass(closure));
    assertEquals(1L, closure.call());
  }

  @SuppressWarnings("unchecked")
  Script compile(String script) {
    Class<Script> cls = loader.parseClass(script);
    return Classpath.newInstance(cls);
  }
}
