/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.direct.io;

import cz.o2.proxima.direct.core.Partition;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import static org.junit.Assert.*;
import org.junit.Test;

public class DirectBatchUnboundedSourceTest {

  @Test
  public void testCheckpointCoder()
      throws Coder.NonDeterministicException, CoderException {

    DirectBatchUnboundedSource.CheckpointCoder coder;
    coder = new DirectBatchUnboundedSource.CheckpointCoder();
    coder.verifyDeterministic();
    DirectBatchUnboundedSource.Checkpoint checkpoint;
    checkpoint = new DirectBatchUnboundedSource.Checkpoint(Arrays.asList(() -> 0), 1);
    DirectBatchUnboundedSource.Checkpoint cloned = CoderUtils.clone(coder, checkpoint);
    assertEquals(1, cloned.getPartitions().size());
    assertEquals(1L, cloned.getSkipFromFirst());
  }

  @Test
  public void testPartitionsSorted() {
    List<Partition> partitions = Arrays.asList(
        partition(0, 4, 5), partition(1, 3, 4), partition(2, 1, 2));
    partitions.sort(DirectBatchUnboundedSource.partitionsComparator());
    assertEquals(
        Arrays.asList(partition(2, 1, 2), partition(1, 3, 4), partition(0, 4, 5)),
        partitions);
  }


  static Partition partition(int id, long minStamp, long maxStamp) {
    return new Partition() {

      @Override
      public int getId() {
        return id;
      }

      @Override
      public long getMinTimestamp() {
        return minStamp;
      }

      @Override
      public long getMaxTimestamp() {
        return maxStamp;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof Partition)) {
          return false;
        }
        return ((Partition) obj).getId() == getId();
      }

      @Override
      public int hashCode() {
        return id;
      }

    };
  }

}
