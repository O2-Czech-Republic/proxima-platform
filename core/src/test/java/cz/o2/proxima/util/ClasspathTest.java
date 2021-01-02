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
package cz.o2.proxima.util;

import static org.junit.Assert.*;

import cz.o2.proxima.storage.PassthroughFilter;
import org.junit.Test;

/** Simple test for {@link Classpath} Tests for inner class are located in scheme module. */
public class ClasspathTest {

  @Test
  public void testFindTopLevelClassWithValidSuperClass() {
    assertEquals(
        DummyFilter.class,
        Classpath.findClass("cz.o2.proxima.util.DummyFilter", PassthroughFilter.class));
  }

  @Test(expected = RuntimeException.class)
  public void testFindTopLevelClassWithInvalidSuperClass() {
    Classpath.findClass("cz.o2.proxima.util.DummyFilter", Classpath.class);
  }

  @Test
  public void testCreateNewInstanceForTopLevelClassWithValidSuper() {
    assertEquals(
        DummyFilter.class,
        Classpath.newInstance("cz.o2.proxima.util.DummyFilter", PassthroughFilter.class)
            .getClass());
  }

  @Test(expected = RuntimeException.class)
  public void testCreateNewInstanceForTopLevelClassWithInvalidSuper() {
    Classpath.newInstance("cz.o2.proxima.util.DummyFilter", Classpath.class);
  }
}
