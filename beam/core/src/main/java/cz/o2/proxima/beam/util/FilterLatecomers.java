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
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

/**
 * Split input {@link PCollection} on late and on-time elements.
 *
 * @param <T> type parameter
 */
public class FilterLatecomers<T> extends PTransform<PCollection<T>, PCollectionTuple> {

  private static final TupleTag<?> ON_TIME_TAG = new TupleTag<>();
  private static final TupleTag<?> LATE_TAG = new TupleTag<>();

  public static <T> FilterLatecomers<T> of() {
    return new FilterLatecomers<>();
  }

  @SuppressWarnings("unchecked")
  public static <T> PCollection<T> getOnTime(PCollectionTuple tuple, TypeDescriptor<T> type) {
    Coder<T> coder = getCoder(tuple, type);
    return (PCollection<T>)
        tuple.get(ON_TIME_TAG).setTypeDescriptor((TypeDescriptor) type).setCoder(coder);
  }

  @SuppressWarnings("unchecked")
  public static <T> PCollection<T> getLate(PCollectionTuple tuple, TypeDescriptor<T> type) {
    final Coder<T> coder = getCoder(tuple, type);
    return (PCollection<T>)
        tuple.get(LATE_TAG).setTypeDescriptor((TypeDescriptor) type).setCoder(coder);
  }

  private static <T> Coder<T> getCoder(PCollectionTuple tuple, TypeDescriptor<T> type) {
    try {
      return tuple.getPipeline().getCoderRegistry().getCoder(type);
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException(e);
    }
  }

  @ExcludeExternal
  @SuppressWarnings("unchecked")
  private static class FilterFn<T> extends DoFn<KV<Integer, T>, T> {

    private final TypeDescriptor<T> inputDescriptor;

    @TimerId("watermark")
    private final TimerSpec watermarkSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private FilterFn(TypeDescriptor<T> inputDescriptor) {
      this.inputDescriptor = inputDescriptor;
    }

    @ProcessElement
    public void process(
        @Element KV<Integer, T> elem,
        @Timestamp Instant ts,
        @TimerId("watermark") Timer watermark,
        MultiOutputReceiver output) {

      if (ts.isBefore(watermark.getCurrentRelativeTime())) {
        output.get((TupleTag<T>) LATE_TAG).output(elem.getValue());
      } else {
        output.get((TupleTag<T>) ON_TIME_TAG).output(elem.getValue());
      }
    }

    @Override
    public TypeDescriptor<T> getOutputTypeDescriptor() {
      return inputDescriptor;
    }

    @OnTimer("watermark")
    public void timer() {}
  }

  @SuppressWarnings("unchecked")
  @Override
  public PCollectionTuple expand(PCollection<T> input) {
    PCollection<KV<Integer, T>> withKeys =
        input.apply(
            WithKeys.<Integer, T>of(Object::hashCode).withKeyType(TypeDescriptors.integers()));
    TupleTag<T> mainTag = (TupleTag<T>) ON_TIME_TAG;
    return withKeys.apply(
        "filter",
        ParDo.of(new FilterFn<>(input.getTypeDescriptor()))
            .withOutputTags(mainTag, TupleTagList.of(LATE_TAG)));
  }
}
