/**
 * Copyright 2017-${Year} O2 Czech Republic, a.s.
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
package cz.o2.proxima.time;

import static org.junit.Assert.*;

import org.junit.Test;

/** Test suite for {@link VectorClock}. */
public class VectorClockTest {

  @Test
  public void testSingleDimension() {
    VectorClock clock = VectorClock.of(1);
    assertEquals(Long.MIN_VALUE, clock.getStamp());
    clock.update(0, 1);
    assertEquals(1, clock.getStamp());
  }

  @Test
  public void testThreeDimensions() {
    VectorClock clock = VectorClock.of(3);
    assertEquals(Long.MIN_VALUE, clock.getStamp());
    clock.update(0, 1);
    assertEquals(Long.MIN_VALUE, clock.getStamp());
    clock.update(1, 2);
    assertEquals(Long.MIN_VALUE, clock.getStamp());
    clock.update(2, 3);
    assertEquals(1, clock.getStamp());
    clock.update(0, 3);
    assertEquals(2, clock.getStamp());
  }
}
