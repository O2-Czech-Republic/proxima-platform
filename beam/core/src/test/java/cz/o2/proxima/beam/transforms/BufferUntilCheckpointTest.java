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
package cz.o2.proxima.beam.transforms;

import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class BufferUntilCheckpointTest {

  @Test
  public void testBufferUntilCheckpointIsIdentity() {
    Pipeline p = Pipeline.create();
    PCollection<String> input = p.apply(Create.of("a", "b", "c")).setCoder(StringUtf8Coder.of());
    PCollection<String> output = input.apply(ParDo.of(new BufferUntilCheckpoint<>()));

    PAssert.that(output).containsInAnyOrder("a", "b", "c");

    assertNotNull(p.run());
  }
}
