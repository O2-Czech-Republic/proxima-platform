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
package cz.o2.proxima.beam.util;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Test;

public class FilterLatecomersTest {

  @Test
  public void testFiltering() {
    Instant now = new Instant(0);
    Pipeline p = Pipeline.create();
    PCollection<Integer> input =
        p.apply(
            TestStream.create(VarIntCoder.of())
                .advanceWatermarkTo(now)
                .addElements(
                    TimestampedValue.of(1, now),
                    TimestampedValue.of(2, now.plus(1)),
                    TimestampedValue.of(3, now.plus(2)))
                .advanceWatermarkTo(now.plus(5))
                .addElements(TimestampedValue.of(0, now.minus(1)))
                .advanceWatermarkToInfinity());
    PCollectionTuple result = input.apply(FilterLatecomers.of());
    PAssert.that(FilterLatecomers.getOnTime(result)).containsInAnyOrder(1, 2, 3);
    PAssert.that(FilterLatecomers.getLate(result)).containsInAnyOrder(0);
    p.run();
  }
}
