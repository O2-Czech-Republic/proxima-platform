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
package cz.o2.proxima.storage;

import static org.junit.Assert.*;

import java.util.Collections;
import org.junit.Test;

/** Test contract of {@link Partition} default methods. */
public class PartitionTest {

  @Test
  public void testDefaultMethods() {
    Partition partition = Partition.of(1);
    assertEquals(1, partition.getId());
    assertEquals(Long.MIN_VALUE, partition.getMinTimestamp());
    assertEquals(Long.MAX_VALUE, partition.getMaxTimestamp());
    assertFalse(partition.isBounded());
    assertEquals(-1L, partition.size());
    assertEquals(Collections.singletonList(partition), partition.split(10));
  }

  @Test
  public void testDefaultCompareTo() {
    assertEquals(0, Partition.of(1).compareTo(Partition.of(1)));
    assertTrue(Partition.of(0).compareTo(Partition.of(1)) < 0);
    assertTrue(Partition.of(1).compareTo(Partition.of(0)) > 0);
    assertTrue(
        Partition.withMinimalTimestamp(1, 0).compareTo(Partition.withMinimalTimestamp(0, 1)) < 0);
  }
}
