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
package cz.o2.proxima.scheme;

import static junit.framework.TestCase.assertEquals;

import cz.o2.proxima.scheme.avro.test.Event;
import cz.o2.proxima.util.Classpath;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

/** Test for {@link Classpath} with inner classes. */
public class ClasspathTest {
  private final String eventClassName = "cz.o2.proxima.scheme.avro.test.Event";

  @Test
  public void testFindInnerClassWithValidSuperClass() {
    assertEquals(Event.class, Classpath.findClass(eventClassName, SpecificRecord.class));
  }

  @Test(expected = RuntimeException.class)
  public void testFindInnerClassWithInvalidSuperClass() {
    Classpath.findClass(eventClassName, Classpath.class);
  }

  @Test
  public void testCreateNewInstanceForInnerClassWithValidSuper() {
    assertEquals(
        Event.class, Classpath.newInstance(eventClassName, SpecificRecord.class).getClass());
  }

  @Test(expected = RuntimeException.class)
  public void testCreateNewInstanceForInnerClassWithInvalidSuper() {
    Classpath.newInstance(eventClassName, Classpath.class);
  }
}
