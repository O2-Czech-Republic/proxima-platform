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
package cz.o2.proxima.beam.core.transform.retract;

import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

public class KeyedRetractPCollection<K, V> extends PValueBase implements PValue {

  private final PCollection<RetractElement<V>> unwrapped;
  private final TupleTag<RetractElement<V>> tag;
  @Getter private final TypeDescriptor<K> keyDescriptor;
  @Getter private final TypeDescriptor<V> valueDescriptor;

  @Getter(AccessLevel.PACKAGE)
  private final UnaryFunction<V, K> keyExtractor;

  KeyedRetractPCollection(
      PCollection<RetractElement<V>> parent,
      Pipeline pipeline,
      TypeDescriptor<V> valueType,
      TypeDescriptor<K> keyType,
      UnaryFunction<V, K> keyExtractor,
      TupleTag<RetractElement<V>> tag) {

    super(pipeline);
    this.unwrapped = parent;
    this.keyDescriptor = keyType;
    this.valueDescriptor = valueType;
    this.tag = tag;
    this.keyExtractor = keyExtractor;
  }

  public <OUT extends PValue> OUT apply(PTransform<KeyedRetractPCollection<K, V>, OUT> transform) {
    return Pipeline.applyTransform(this, transform);
  }

  public PCollection<RetractElement<KV<K, V>>> unwrapped() {
    TypeDescriptor<RetractElement<KV<K, V>>> type =
        getRetractElementTypeDescriptor(keyDescriptor, valueDescriptor);

    Coder<K> keyCoder =
        ExceptionUtils.uncheckedFactory(
            () -> unwrapped.getPipeline().getCoderRegistry().getCoder(keyDescriptor));
    Coder<V> valueCoder =
        ExceptionUtils.uncheckedFactory(
            () -> unwrapped.getPipeline().getCoderRegistry().getCoder(valueDescriptor));
    KvCoder<K, V> kvCoder = KvCoder.of(keyCoder, valueCoder);
    Coder<RetractElement<KV<K, V>>> outputCoder = RetractElement.Coder.of(kvCoder);
    return unwrapped.apply(mapToUnwrapped(keyExtractor, type)).setCoder(outputCoder);
  }

  public PCollection<RetractElement<V>> unwrappedValues() {
    return unwrapped;
  }

  private static <K, V> MapElements<RetractElement<V>, RetractElement<KV<K, V>>> mapToUnwrapped(
      UnaryFunction<V, K> keyExtractor, TypeDescriptor<RetractElement<KV<K, V>>> type) {

    return MapElements.into(type)
        .via(
            e ->
                e.isAddition()
                    ? RetractElement.ofAddition(
                        KV.of(keyExtractor.apply(e.getValue()), e.getValue()), e.getSeqId())
                    : RetractElement.ofDeletion(
                        KV.of(keyExtractor.apply(e.getValue()), e.getValue()), e.getSeqId()));
  }

  private static <K, V>
      MapElements<RetractElement<V>, RetractElement<KV<K, V>>> mapToUnwrappedValue(
          UnaryFunction<V, K> keyExtractor, TypeDescriptor<RetractElement<KV<K, V>>> type) {

    return MapElements.into(type)
        .via(
            e ->
                e.isAddition()
                    ? RetractElement.ofAddition(
                        KV.of(keyExtractor.apply(e.getValue()), e.getValue()), e.getSeqId())
                    : RetractElement.ofDeletion(
                        KV.of(keyExtractor.apply(e.getValue()), e.getValue()), e.getSeqId()));
  }

  private static <K, V> TypeDescriptor<RetractElement<KV<K, V>>> getRetractElementTypeDescriptor(
      TypeDescriptor<K> keyDesc, TypeDescriptor<V> valueDesc) {

    return new TypeDescriptor<RetractElement<KV<K, V>>>() {}.where(
            new TypeParameter<>() {}, keyDesc)
        .where(new TypeParameter<>() {}, valueDesc);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return Map.of(tag, unwrapped);
  }
}
