/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core.transforms.retract;

import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import java.util.Map;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

public class RetractPCollection<T> extends PValueBase implements PValue {

  public static <T> RetractPCollection<T> wrap(
      PCollection<T> parent, UnaryFunction<T, Long> seqId, UnaryFunction<T, Boolean> isAddition) {

    TypeDescriptor<RetractElement<T>> target =
        new TypeDescriptor<RetractElement<T>>() {}.where(
            new TypeParameter<>() {}, parent.getTypeDescriptor());
    Coder<T> valueCoder = parent.getCoder();
    Coder<RetractElement<T>> wrappedCoder = RetractElement.Coder.of(valueCoder);
    TupleTag<RetractElement<T>> tupleTag =
        new TupleTag<>() {
          @Override
          public TypeDescriptor<RetractElement<T>> getTypeDescriptor() {
            return new TypeDescriptor<RetractElement<T>>() {}.where(
                new TypeParameter<T>() {}, parent.getTypeDescriptor());
          }
        };
    PCollection<RetractElement<T>> wrapped =
        parent
            .apply(
                MapElements.into(target)
                    .via(
                        v ->
                            isAddition.apply(v)
                                ? RetractElement.ofAddition(v, seqId.apply(v))
                                : RetractElement.ofDeletion(v, seqId.apply(v))))
            .setCoder(wrappedCoder)
            .setTypeDescriptor(tupleTag.getTypeDescriptor());

    return new RetractPCollection<>(
        wrapped, parent.getPipeline(), parent.getTypeDescriptor(), tupleTag);
  }

  public static <T> RetractPCollection<T> fromRetractedElements(
      PCollection<RetractElement<T>> parent, TypeDescriptor<T> typeDescriptor) {

    RetractElement.Coder<T> coder = (RetractElement.Coder<T>) parent.getCoder();
    return fromRetractedElements(parent, e -> e, typeDescriptor);
  }

  public static <T, V> RetractPCollection<T> fromRetractedElements(
      PCollection<RetractElement<V>> parent,
      UnaryFunction<V, T> mapper,
      TypeDescriptor<T> typeDescriptor) {

    Coder<T> outputCoder =
        ExceptionUtils.uncheckedFactory(
            () -> parent.getPipeline().getCoderRegistry().getCoder(typeDescriptor));
    TypeDescriptor<RetractElement<T>> out =
        new TypeDescriptor<RetractElement<T>>() {}.where(new TypeParameter<>() {}, typeDescriptor);
    PCollection<RetractElement<T>> wrapped =
        parent
            .apply(MapElements.into(out).via(e -> e.mapped(mapper)))
            .setCoder(RetractElement.Coder.of(outputCoder));
    return new RetractPCollection<>(
        wrapped, parent.getPipeline(), typeDescriptor, new TupleTag<>() {});
  }

  private final PCollection<RetractElement<T>> unwrapped;
  private final TupleTag<RetractElement<T>> tag;
  @Getter private final TypeDescriptor<T> descriptor;

  RetractPCollection(
      PCollection<RetractElement<T>> parent,
      Pipeline pipeline,
      TypeDescriptor<T> type,
      TupleTag<RetractElement<T>> tag) {

    super(pipeline);
    this.unwrapped = parent;
    this.tag = tag;
    this.descriptor = type;
  }

  public <OUT extends PValue> OUT apply(PTransform<RetractPCollection<T>, OUT> transform) {
    return Pipeline.applyTransform(this, transform);
  }

  public <OUT extends PValue> OUT apply(
      String name, PTransform<RetractPCollection<T>, OUT> transform) {
    return Pipeline.applyTransform(name, this, transform);
  }

  public <K> KeyedRetractPCollection<K, T> keyed(
      UnaryFunction<T, K> keyExtractor, TypeDescriptor<K> keyDescriptor) {

    return new KeyedRetractPCollection<>(
        unwrapped, getPipeline(), descriptor, keyDescriptor, keyExtractor, new TupleTag<>() {});
  }

  public PCollection<RetractElement<T>> unwrapped() {
    return unwrapped;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return Map.of(tag, unwrapped);
  }
}
