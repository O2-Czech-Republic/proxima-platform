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

import java.util.Arrays;
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

}
