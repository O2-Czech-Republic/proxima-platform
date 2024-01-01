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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Test suite for {@link cz.o2.proxima.core.util.CamelCase}. */
public class CamelCaseTest {

  @Test
  public void testCamelCaseWithFirstLower() {
    String input = "gateway-replication_test-123";
    assertEquals("gatewayReplication_test123", CamelCase.apply(input, false));
    assertEquals(
        "gatewayReplicationTest123",
        CamelCase.apply(input, false, CamelCase.Characters.SPACE_DASH_AND_UNDERSCORE));
  }

  @Test
  public void testCamelCaseWithFirstUpper() {
    String input = "gateway-replication_test-123";
    assertEquals("GatewayReplication_test123", CamelCase.apply(input));
    assertEquals(
        "GatewayReplicationTest123",
        CamelCase.apply(input, true, CamelCase.Characters.SPACE_DASH_AND_UNDERSCORE));
  }
}
