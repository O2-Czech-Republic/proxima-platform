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
package cz.o2.proxima.storage.commitlog;

import cz.o2.proxima.storage.StreamElement;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PartitionersTest {

  @Test
  public void testUniformDistribution() {
    final Partitioner partitioner =
        new Partitioner() {

          /** Start from negative number, to ensure correctness... */
          int lastPartitionId = -33;

          @Override
          public int getPartitionId(StreamElement element) {
            return lastPartitionId++;
          }
        };

    final int numElements = 99;
    final StreamElement element = Mockito.mock(StreamElement.class);
    final Map<Integer, Integer> counts = new HashMap<>();
    for (int i = 0; i < numElements; i++) {
      counts.merge(Partitioners.getTruncatedPartitionId(partitioner, element, 3), 1, Integer::sum);
    }
    Assert.assertEquals(3, counts.size());
    Assert.assertEquals(Integer.valueOf(33), counts.get(0));
    Assert.assertEquals(Integer.valueOf(33), counts.get(1));
    Assert.assertEquals(Integer.valueOf(33), counts.get(2));
  }
}
