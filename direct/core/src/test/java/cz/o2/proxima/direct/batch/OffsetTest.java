/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.batch;

import cz.o2.proxima.core.storage.Partition;
import org.junit.Assert;
import org.junit.Test;

public class OffsetTest {

  @Test
  public void testSimpleOffset() {
    final Partition partition = Partition.of(2);
    final Offset.SimpleOffset firstOffset = Offset.of(partition, 5, false);
    final Offset.SimpleOffset secondOffset = Offset.of(partition, 10, true);
    Assert.assertEquals(partition, firstOffset.getPartition());
    Assert.assertEquals(5, firstOffset.getElementIndex());
    Assert.assertFalse(firstOffset.isLast());
    Assert.assertEquals(partition, secondOffset.getPartition());
    Assert.assertEquals(10, secondOffset.getElementIndex());
    Assert.assertTrue(secondOffset.isLast());
  }
}
