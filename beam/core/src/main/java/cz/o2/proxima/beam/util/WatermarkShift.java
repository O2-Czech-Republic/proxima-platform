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

import cz.o2.proxima.beam.util.state.ExcludeExternal;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Shift watermark of input {@link PCollection} by given duration back in time.
 *
 * @param <T> type parameter
 */
public class WatermarkShift<T> extends PTransform<PCollection<T>, PCollection<T>> {

  public static <T> WatermarkShift<T> of(Duration duration) {
    return new WatermarkShift<>(duration);
  }

  private final Duration shiftDuration;

  public WatermarkShift(Duration shiftDuration) {
    this.shiftDuration = shiftDuration;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    PCollection<byte[]> impulse = input.getPipeline().apply(Impulse.create());
    // filter elements out, just take watermark
    PCollection<byte[]> originalWatermark =
        input
            .apply(Filter.by(e -> false))
            .apply(MapElements.into(impulse.getTypeDescriptor()).via(e -> new byte[0]));
    PCollection<T> holdWatermark =
        PCollectionList.of(impulse)
            .and(originalWatermark)
            .apply(Flatten.pCollections())
            .apply(WithKeys.of(""))
            .apply("hold", ParDo.of(new HoldFn()))
            .apply(MapElements.into(input.getTypeDescriptor()).via(e -> null))
            .setCoder(input.getCoder());
    return PCollectionList.of(input)
        .and(holdWatermark)
        .apply(Flatten.pCollections())
        .setCoder(input.getCoder())
        .setTypeDescriptor(input.getTypeDescriptor());
  }

  @ExcludeExternal
  private class HoldFn extends DoFn<KV<String, byte[]>, byte[]> {

    private final Instant minInstant = BoundedWindow.TIMESTAMP_MIN_VALUE.plus(shiftDuration);

    @TimerId("holder")
    private final TimerSpec holderTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processImpulse(@TimerId("holder") Timer holder) {
      // start the timer
      holder.withOutputTimestamp(BoundedWindow.TIMESTAMP_MIN_VALUE).set(minInstant);
    }

    @OnTimer("holder")
    public void onTimer(@TimerId("holder") Timer holder) {
      Instant current = holder.getCurrentRelativeTime();
      if (current.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        Instant shifted = current.minus(shiftDuration);
        holder.withOutputTimestamp(shifted).offset(Duration.ZERO).setRelative();
      }
    }
  }
}
