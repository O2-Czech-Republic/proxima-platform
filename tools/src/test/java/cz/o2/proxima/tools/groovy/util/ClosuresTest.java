/**
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;

import groovy.lang.Closure;
import org.junit.Test;

/** Test suite for {@link Closures}. */
public class ClosuresTest {

  @Test
  public void testFromFactory() {
    Closure<Long> closure = Closures.from(this, () -> 1L);
    assertEquals(Long.valueOf(1L), closure.call());
  }

  @Test
  public void testFromFunction() {
    Closure<Long> closure = Closures.from(this, a -> (long) a);
    assertEquals(Long.valueOf(1L), closure.call(1L));
  }

  @Test
  public void testFromBiFunction() {
    Closure<Long> closure = Closures.from(this, (a, b) -> (long) a + (long) b);
    assertEquals(Long.valueOf(3L), closure.call(1L, 2L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromBiFunctionWithSrongArgumentCount() {
    Closure<Long> closure = Closures.from(this, (a, b) -> (long) a + (long) b);
    closure.call(1L, 2L, 3L);
  }

  @Test
  public void testFromArray() {
    Closure<Integer> closure = Closures.fromArray(this, arr -> arr.length);
    assertEquals(Integer.valueOf(0), closure.call());
    assertEquals(Integer.valueOf(1), closure.call("1"));
    assertEquals(Integer.valueOf(2), closure.call("1", 2));
  }
}
