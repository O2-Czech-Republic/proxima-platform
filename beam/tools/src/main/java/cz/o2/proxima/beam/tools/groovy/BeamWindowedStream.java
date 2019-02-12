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
package cz.o2.proxima.beam.tools.groovy;

import cz.o2.proxima.beam.core.PCollectionTools;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

/**
 * A {@link WindowedStream} backed by beam.
 */
class BeamWindowedStream<T> extends BeamStream<T> implements WindowedStream<T> {

  @Getter
  private final WindowFn<Object, ?> windowing;
  @Getter
  private final WindowingStrategy.AccumulationMode mode;
  private long earlyEmitting = -1L;
  private long allowedLateness = 0;

  @SuppressWarnings("unchecked")
  BeamWindowedStream(
      boolean bounded, PCollectionProvider<T> input,
      WindowFn<? super T, ?> windowing,
      WindowingStrategy.AccumulationMode mode,
      StreamProvider.TerminatePredicate terminateCheck) {

    super(bounded, input, terminateCheck);
    this.windowing = (WindowFn) windowing;
    this.mode = mode;
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor, Closure<V> valueExtractor,
      V initialValue, Closure<V> reducer) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> valueDehydrated = valueExtractor.dehydrate();
    Closure<V> reducerDehydrated = reducer.dehydrate();
    TypeDescriptor<K> keyDesc = typeOf(keyDehydrated);
    TypeDescriptor<V> valueDesc = typeOf(valueDehydrated);

    return descendant(pipeline -> {
      PCollection<KV<K, V>> kvs = ReduceByKey
          .of(collection.materialize(pipeline))
          .keyBy(keyDehydrated::call, keyDesc)
          .valueBy(valueDehydrated::call, valueDesc)
          .reduceBy((java.util.stream.Stream<V> in) -> {
            V current = initialValue;
            return in.reduce(current, reducerDehydrated::call);
          }, valueDesc)
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .output();

      return asPairs(kvs, keyDesc, valueDesc);
    });
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor, V initialValue, Closure<V> reducer) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> reducerDehydrated = reducer.dehydrate();
    TypeDescriptor<K> keyDesc = typeOf(keyDehydrated);
    TypeDescriptor<V> valueDesc = typeOf(reducerDehydrated);

    return descendant(pipeline -> {
      PCollection<T> c = collection.materialize(pipeline);
      return asPairs(
          ReduceByKey.of(c)
              .keyBy(keyDehydrated::call, keyDesc)
              .valueBy(e -> e, c.getTypeDescriptor())
              .reduceBy((java.util.stream.Stream<T> in) -> {
                V current = initialValue;
                Iterable<T> iter = in::iterator;
                for (T v : iter) {
                  current = reducerDehydrated.call(current, v);
                }
                return current;
              }, valueDesc)
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .output()
              .setTypeDescriptor(TypeDescriptors.kvs(keyDesc, valueDesc)),
          keyDesc, valueDesc);
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<StreamElement> reduceToLatest() {
    return descendant(pipeline -> PCollectionTools.reduceAsSnapshot(
            (PCollection) collection.materialize(pipeline)));
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> flatReduce(
      Closure<K> keyExtractor, Closure<V> listReduce) {


    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> reducerDehydrated = listReduce.dehydrate();
    TypeDescriptor<K> keyDesc = typeOf(keyExtractor);
    TypeDescriptor<V> valueDesc = typeOf(listReduce);

    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      PCollection<KV<K, V>> kvs = ReduceByKey
          .of(in)
          .keyBy(keyDehydrated::call, keyDesc)
          .valueBy(e -> e, in.getTypeDescriptor())
          .reduceBy((java.util.stream.Stream<T> values) -> {
            List<T> list = values.collect(Collectors.toList());
            // FIXME: we need access to window here
            return reducerDehydrated.call(null, list);
          }, valueDesc)
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .output();

      return asPairs(kvs, keyDesc, valueDesc);
    });

  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> combine(
      Closure<K> keyExtractor, Closure<V> valueExtractor,
      V initial, Closure<V> combine) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> valueDehydrated = valueExtractor.dehydrate();
    Closure<V> combineDehydrated = combine.dehydrate();

    TypeDescriptor<K> keyDesc = typeOf(keyDehydrated);
    TypeDescriptor<V> valueDesc = typeOf(valueDehydrated);

    return descendant(pipeline ->
        asPairs(
            ReduceByKey.of(collection.materialize(pipeline))
                .keyBy(keyDehydrated::call, keyDesc)
                .valueBy(valueDehydrated::call, valueDesc)
                .combineBy((java.util.stream.Stream<V> in) ->
                    in.reduce(initial, combineDehydrated::call), valueDesc)
                .windowBy(windowing)
                .triggeredBy(createTrigger())
                .accumulationMode(mode)
                .withAllowedLateness(Duration.millis(allowedLateness))
                .output(),
            keyDesc, valueDesc));
  }

  @Override
  public <K> WindowedStream<Pair<K, T>> combine(
      Closure<K> keyExtractor, T initial, Closure<T> combine) {


    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<T> combineDehydrated = combine.dehydrate();

    TypeDescriptor<K> keyDesc = typeOf(keyDehydrated);
    TypeDescriptor<T> valueDesc = typeOf(combineDehydrated);

    return descendant(pipeline ->
        asPairs(
            ReduceByKey.of(collection.materialize(pipeline))
                .keyBy(keyDehydrated::call, keyDesc)
                .valueBy(e -> e, valueDesc)
                .combineBy((java.util.stream.Stream<T> in) ->
                    in.reduce(initial, combineDehydrated::call), valueDesc)
                .windowBy(windowing)
                .triggeredBy(createTrigger())
                .accumulationMode(mode)
                .withAllowedLateness(Duration.millis(allowedLateness))
                .output()
                .setTypeDescriptor(TypeDescriptors.kvs(keyDesc, valueDesc)),
            keyDesc, valueDesc));
  }

  @Override
  public <K> WindowedStream<Pair<K, Long>> countByKey(
      Closure<K> keyExtractor) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    TypeDescriptor<K> keyDesc = typeOf(keyDehydrated);
    return descendant(pipeline ->
        asPairs(
            ReduceByKey.of(collection.materialize(pipeline))
                .keyBy(keyDehydrated::call, keyDesc)
                .valueBy(e -> 1L, TypeDescriptors.longs())
                .combineBy(Sums.ofLongs(), TypeDescriptors.longs())
                .windowBy(windowing)
                .triggeredBy(createTrigger())
                .accumulationMode(mode)
                .withAllowedLateness(Duration.millis(allowedLateness))
                .output(),
            keyDesc, TypeDescriptors.longs()));
  }

  @Override
  public WindowedStream<Double> average(Closure<Double> valueExtractor) {

    Closure<Double> valueDehydrated = valueExtractor.dehydrate();
    return descendant(pipeline -> {
      PCollection<KV<Double, Long>> intermediate = ReduceByKey
          .of(collection.materialize(pipeline))
          .keyBy(e -> "", TypeDescriptors.strings())
          .valueBy(
              e -> KV.of(valueDehydrated.call(e), 1L),
              TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()))
          .combineBy(Fold.of(
              (a, b) -> KV.of(a.getKey() + b.getKey(), a.getValue() + b.getValue())),
              TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()))
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .outputValues();
      intermediate.setTypeDescriptor(
          TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()));

      return MapElements.of(intermediate)
          .using(p -> p.getKey() / p.getValue(), TypeDescriptors.doubles())
          .output();
    });
  }

  @Override
  public <K> WindowedStream<Pair<K, Double>> averageByKey(
      Closure<K> keyExtractor, Closure<Double> valueExtractor) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<Double> valueDehydrated = valueExtractor.dehydrate();
    TypeDescriptor<K> keyDesc = typeOf(keyDehydrated);

    return descendant(pipeline -> {
      PCollection<KV<K, KV<Double, Long>>> intermediate = ReduceByKey
          .of(collection.materialize(pipeline))
          .keyBy(keyDehydrated::call, keyDesc)
          .valueBy(
              e -> KV.of(valueDehydrated.call(e), 1L),
              TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()))
          .combineBy(Fold.of(
              (a, b) -> KV.of(a.getKey() + b.getKey(), a.getValue() + b.getValue())))
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .output();
      return MapElements.of(intermediate)
          .using(
              p -> Pair.of(
                  p.getKey(), p.getValue().getKey() / p.getValue().getValue()),
              PairCoder.descriptor(keyDesc, TypeDescriptors.doubles()))
          .output();
    });
  }

  @Override
  public <K, OTHER> WindowedStream<Pair<T, OTHER>> join(
      WindowedStream<OTHER> right, Closure<K> leftKey,
      Closure<K> rightKey) {

    Closure<K> leftKeyDehydrated = leftKey.dehydrate();
    Closure<K> rightKeyDehydrated = rightKey.dehydrate();
    TypeDescriptor<K> keyType = typeOf(leftKey);
    return descendant(
        pipeline -> {
          PCollection<T> lc = collection.materialize(pipeline);
          PCollection<OTHER> rc = ((BeamWindowedStream<OTHER>) right)
              .collection.materialize(pipeline);
          return Join.of(lc, rc)
              .by(leftKeyDehydrated::call, rightKeyDehydrated::call, keyType)
              .using((l, r, ctx) -> ctx.collect(Pair.of(l, r)),
                  PairCoder.descriptor(lc.getTypeDescriptor(), rc.getTypeDescriptor()))
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .outputValues();
        });
  }

  @Override
  public <K, RIGHT> WindowedStream<Pair<T, RIGHT>> leftJoin(
      WindowedStream<RIGHT> right, Closure<K> leftKey,
      Closure<K> rightKey) {

    Closure<K> leftKeyDehydrated = leftKey.dehydrate();
    Closure<K> rightKeyDehydrated = rightKey.dehydrate();
    TypeDescriptor<K> keyType = typeOf(leftKey);

    return descendant(
        pipeline -> {
          PCollection<T> lc = collection.materialize(pipeline);
          PCollection<RIGHT> rc = ((BeamWindowedStream<RIGHT>) right)
              .collection.materialize(pipeline);
          return LeftJoin.of(lc, rc)
              .by(leftKeyDehydrated::call, rightKeyDehydrated::call, keyType)
              .using((l, r, ctx) ->
                  ctx.collect(Pair.of(l, r.orElse(null))),
                  PairCoder.descriptor(lc.getTypeDescriptor(), rc.getTypeDescriptor()))
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .outputValues();
        });
  }

  @Override
  public WindowedStream<T> sorted(Closure<Integer> compareFn) {
    Closure<Integer> dehydrated = compareFn.dehydrate();
    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      return ReduceByKey
          .of(in)
          .keyBy(e -> null, TypeDescriptors.nulls())
          .reduceBy((values, ctx) ->
              values.forEach(ctx::collect), in.getTypeDescriptor())
          .withSortedValues(dehydrated::call)
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .outputValues();
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<Comparable<T>> sorted() {
    return (WindowedStream) descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      return ReduceByKey
          .of((PCollection<Comparable<T>>) in)
          .keyBy(e -> null, TypeDescriptors.nulls())
          .reduceBy((values, ctx) ->
              values.forEach(e -> ctx.collect((T) e)), in.getTypeDescriptor())
          .withSortedValues((a, b) -> a.compareTo((T) b))
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .outputValues();
    });
  }

  @Override
  public WindowedStream<Long> count() {
    return descendant(pipeline ->
        ReduceByKey
            .of(collection.materialize(pipeline))
            .keyBy(e -> null, TypeDescriptors.nulls())
            .valueBy(e -> 1L, TypeDescriptors.longs())
            .combineBy(Sums.ofLongs(), TypeDescriptors.longs())
            .windowBy(windowing)
            .triggeredBy(createTrigger())
            .accumulationMode(mode)
            .withAllowedLateness(Duration.millis(allowedLateness))
            .outputValues());
  }

  @Override
  public WindowedStream<Double> sum(Closure<Double> valueExtractor) {
    Closure<Double> valueDehydrated = valueExtractor.dehydrate();
    return descendant(pipeline ->
        ReduceByKey
            .of(collection.materialize(pipeline))
            .keyBy(e -> null, TypeDescriptors.nulls())
            .valueBy(valueDehydrated::call, TypeDescriptors.doubles())
            .combineBy(Fold.of(0.0, (a, b) -> a + b), TypeDescriptors.doubles())
            .windowBy(windowing)
            .triggeredBy(createTrigger())
            .accumulationMode(mode)
            .withAllowedLateness(Duration.millis(allowedLateness))
            .outputValues());
  }

  @Override
  public <K> WindowedStream<Pair<K, Double>> sumByKey(
      Closure<K> keyExtractor, Closure<Double> valueExtractor) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<Double> valueDehydrated = valueExtractor.dehydrate();
    TypeDescriptor<K> keyType = typeOf(keyExtractor);
    return descendant(pipeline ->
        asPairs(ReduceByKey.of(collection.materialize(pipeline))
            .keyBy(keyDehydrated::call, keyType)
            .valueBy(valueDehydrated::call, TypeDescriptors.doubles())
            .combineBy(Fold.of(0.0, (a, b) -> a + b), TypeDescriptors.doubles())
            .windowBy(windowing)
            .triggeredBy(createTrigger())
            .accumulationMode(mode)
            .withAllowedLateness(Duration.millis(allowedLateness))
            .output(), keyType, TypeDescriptors.doubles()));
  }

  @Override
  public WindowedStream<T> distinct() {
    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      PCollection<KV<T, Void>> distinct = ReduceByKey
          .of(in)
          .keyBy(e -> e, in.getTypeDescriptor())
          .valueBy(e -> null, TypeDescriptors.nulls())
          .combineBy(e -> null, TypeDescriptors.nulls())
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .output();

      return MapElements.of(distinct)
          .using(KV::getKey, in.getTypeDescriptor())
          .output();

        /* Beam 2.11.0: */
        /*
        Distinct
            .of(collection.materialize(pipeline))
            .windowBy(windowing)
            .triggeredBy(createTrigger())
            .accumulationMode(mode)
            .withAllowedLateness(Duration.millis(allowedLateness))
            .output());
        */
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<T> distinct(Closure<?> mapper) {
    Closure<Object> dehydrated = (Closure) mapper.dehydrate();
    TypeDescriptor<Object> keyType = (TypeDescriptor) typeOf(mapper);
    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      return ReduceByKey
        .of(in)
        .keyBy(dehydrated::call, keyType)
        .valueBy(e -> e, in.getTypeDescriptor())
        .combineBy(e -> e.findAny().orElseThrow(
            () -> new IllegalStateException("Processing empty key?")),
            in.getTypeDescriptor())
        .windowBy(windowing)
        .triggeredBy(createTrigger())
        .accumulationMode(mode)
        .withAllowedLateness(Duration.millis(allowedLateness))
        .outputValues();
    });

    /* Beam 2.11.0: */
    /*
    Distinct
        .of(collection.materialize(pipeline))
        .projected(...)
        .windowBy(windowing)
        .triggeredBy(createTrigger())
        .accumulationMode(mode)
        .withAllowedLateness(Duration.millis(allowedLateness))
        .output());
    */

  }

  @Override
  public WindowedStream<T> withEarlyEmitting(long duration) {
    this.earlyEmitting = duration;
    return this;
  }

  @Override
  public WindowedStream<T> withAllowedLateness(long lateness) {
    this.allowedLateness = lateness;
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  <X> BeamWindowedStream<X> descendant(PCollectionProvider<X> provider) {
    return new BeamWindowedStream<>(
        bounded, provider, (WindowFn) windowing, mode, terminateCheck);
  }

  private static <K, V> PCollection<Pair<K, V>> asPairs(
      PCollection<KV<K, V>> kvs,
      TypeDescriptor<K> keyDesc,
      TypeDescriptor<V> valueDesc) {

    return MapElements.of(kvs)
        .using(kv -> Pair.of(kv.getKey(), kv.getValue()),
            PairCoder.descriptor(keyDesc, valueDesc))
        .output();
  }

  private Trigger createTrigger() {
    if (earlyEmitting > 0) {
      return AfterWatermark.pastEndOfWindow()
          .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
              .plusDelayOf(Duration.millis(earlyEmitting)));
    }
    return AfterWatermark.pastEndOfWindow();
  }

}
