/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

import cz.o2.proxima.util.NamePattern;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test pattern matching.
 */
public class NamePatternTest {
  
  @Test
  public void testSingleWildcard() {
    String pattern = "device.*";
    NamePattern test = new NamePattern(pattern);
    assertTrue(test.matches("device.abc-xyz"));
    assertFalse(test.matches("device."));
    assertFalse(test.matches("device"));
  }
  
  @Test
  public void testSingleWildcardInMiddle() {
    String pattern = "device.*-abc";
    NamePattern test = new NamePattern(pattern);
    assertTrue(test.matches("device.xyz-abc"));
    assertFalse(test.matches("device.xyz-abc-123"));
    assertFalse(test.matches("device.-abc"));
  }
  
  @Test
  public void testWildcardAtBegginingAndEnd() {
    String pattern = "*device*";
    NamePattern test = new NamePattern(pattern);
    assertTrue(test.matches("this is my device number 1"));
    assertFalse(test.matches("device 1"));
    assertFalse(test.matches("first device"));
  }

}
