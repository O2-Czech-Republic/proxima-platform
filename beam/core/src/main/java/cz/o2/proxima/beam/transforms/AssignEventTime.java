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

import cz.o2.proxima.storage.StreamElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class AssignEventTime<T> extends PTransform<PCollection<T>, PCollection<T>> {

  public static AssignEventTime<StreamElement> forStreamElement() {
    return new AssignEventTime<>(el -> Instant.ofEpochMilli(el.getStamp()));
  }

  public static <T> AssignEventTime<T> forTimestampFn(
      SerializableFunction<T, Instant> timestampFn) {

    return new AssignEventTime<>(timestampFn);
  }

  static class AssignDoFn<T> extends DoFn<T, T> {

    private final SerializableFunction<T, Instant> timestampFn;

    private AssignDoFn(SerializableFunction<T, Instant> timestampFn) {
      this.timestampFn = timestampFn;
    }

    @ProcessElement
    public void process(@Element T in, OutputReceiver<T> out) {
      out.outputWithTimestamp(in, timestampFn.apply(in));
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(Long.MAX_VALUE);
    }
  }

  private final SerializableFunction<T, Instant> timestampFn;

  private AssignEventTime(SerializableFunction<T, Instant> timestampFn) {
    this.timestampFn = timestampFn;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply(ParDo.of(new AssignDoFn<>(timestampFn)))
        .setTypeDescriptor(input.getTypeDescriptor())
        .setCoder(input.getCoder());
  }
}
