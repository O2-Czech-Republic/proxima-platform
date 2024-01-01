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
package cz.o2.proxima.core.util;

import static org.junit.Assert.*;

import org.junit.Test;

/** Test pattern matching. */
public class NamePatternTest {

  @Test
  public void testSingleWildcard() {
    String pattern = "device.*";
    NamePattern test = new NamePattern(pattern);
    assertTrue(test.matches("device.abc-xyz"));
    assertTrue(test.matches("device."));
    assertFalse(test.matches("device"));
  }
}
