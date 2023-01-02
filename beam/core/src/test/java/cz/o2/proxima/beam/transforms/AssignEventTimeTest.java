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
package cz.o2.proxima.beam.transforms;

import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.junit.Test;

public class AssignEventTimeTest {

  @Test
  public void testTransform() {
    Pipeline p = Pipeline.create();
    Instant now = Instant.now();
    PCollection<KV<Instant, Instant>> res =
        p.apply(Create.of(now, now.plus(1), now.plus(2)))
            .apply(AssignEventTime.forTimestampFn(e -> e))
            .apply(Reify.timestamps())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(Instant.class), TypeDescriptor.of(Instant.class)))
                    .via(e -> KV.of(e.getValue(), e.getTimestamp())));
    PAssert.that(res)
        .containsInAnyOrder(
            KV.of(now, now), KV.of(now.plus(1), now.plus(1)), KV.of(now.plus(2), now.plus(2)));
    assertNotNull(p.run());
  }
}
