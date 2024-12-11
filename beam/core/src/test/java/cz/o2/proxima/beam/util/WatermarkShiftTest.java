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
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class WatermarkShiftTest {

  @Test
  public void testWatermarkShift() {
    Pipeline p = Pipeline.create();
    Instant now = new Instant(0);
    PCollection<Integer> input =
        p.apply(
            TestStream.create(VarIntCoder.of())
                .advanceWatermarkTo(now)
                .addElements(
                    TimestampedValue.of(1, now.plus(1)), TimestampedValue.of(2, now.plus(2)))
                .advanceWatermarkTo(now.plus(5))
                .addElements(TimestampedValue.of(0, now.minus(1)))
                .advanceWatermarkToInfinity());
    PCollectionTuple filtered = input.apply(FilterLatecomers.of());
    PCollection<Integer> onTimeBeforeShift = FilterLatecomers.getOnTime(filtered);
    filtered = input.apply(WatermarkShift.of(Duration.millis(6))).apply(FilterLatecomers.of());
    PCollection<Integer> onTimeAfterShift = FilterLatecomers.getOnTime(filtered);
    PCollection<KV<Integer, Long>> counts =
        PCollectionList.of(onTimeAfterShift)
            .and(onTimeBeforeShift)
            .apply(Flatten.pCollections())
            .apply(
                Window.<Integer>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes())
            .apply(Count.perElement());
    // assert that 1, 2 are always on time, but 0 only after the shift
    PAssert.that(counts).containsInAnyOrder(KV.of(1, 2L), KV.of(2, 2L), KV.of(0, 1L));
    p.run();
  }
}
