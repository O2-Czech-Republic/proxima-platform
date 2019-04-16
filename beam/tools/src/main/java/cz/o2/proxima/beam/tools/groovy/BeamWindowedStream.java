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
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
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
      StreamConfig config, boolean bounded, PCollectionProvider<T> input,
      WindowFn<? super T, ?> windowing,
      WindowingStrategy.AccumulationMode mode,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory) {

    super(config, bounded, input, terminateCheck, pipelineFactory);
    this.windowing = (WindowFn) windowing;
    this.mode = mode;
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initialValue,
      Closure<V> reducer) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<V> reducerDehydrated = dehydrate(reducer);
    return descendant(pipeline -> {
      Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
      Coder<V> valueCoder = coderOf(pipeline, valueDehydrated);
      PCollection<KV<K, V>> kvs = ReduceByKey
          .of(collection.materialize(pipeline))
          .keyBy(keyDehydrated::call)
          .valueBy(valueDehydrated::call)
          .reduceBy((java.util.stream.Stream<V> in) -> {
            V current = initialValue;
            return in.reduce(current, reducerDehydrated::call);
          })
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .output()
          .setCoder(KvCoder.of(keyCoder, valueCoder));

      return asPairs(kvs, keyCoder, valueCoder);
    });
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor, V initialValue, Closure<V> reducer) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> reducerDehydrated = dehydrate(reducer);
    return descendant(pipeline -> {
      Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
      Coder<V> valueCoder = coderOf(pipeline, reducerDehydrated);
      PCollection<T> c = collection.materialize(pipeline);
      return asPairs(
          ReduceByKey.of(c)
              .keyBy(keyDehydrated::call)
              .valueBy(e -> e)
              .reduceBy((java.util.stream.Stream<T> in) -> {
                V current = initialValue;
                Iterable<T> iter = in::iterator;
                for (T v : iter) {
                  current = reducerDehydrated.call(current, v);
                }
                return current;
              })
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .output()
              .setCoder(KvCoder.of(keyCoder, valueCoder)),
          keyCoder, valueCoder);
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<StreamElement> reduceToLatest() {
    return descendant(pipeline -> PCollectionTools.reduceAsSnapshot(
            (PCollection) collection.materialize(pipeline)));
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> groupReduce(
      Closure<K> keyExtractor, Closure<Iterable<V>> listReduce) {


    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<Iterable<V>> reducerDehydrated = dehydrate(listReduce);

    return descendant(pipeline -> {
      final Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
      // FIXME: need a way to retrieve inner type of the list
      @SuppressWarnings("unchecked")
      final Coder<V> valueCoder = (Coder) getCoder(
          pipeline, TypeDescriptor.of(Object.class));
      PCollection<T> in = collection.materialize(pipeline);
      // use native beam, beamphoria doesn't allow access
      // to window label as of 2.12
      in = in.apply(createWindowFn());
      PCollection<KV<K, T>> keyed = MapElements.of(in)
          .using(el -> KV.of(keyDehydrated.call(el), el))
          .output()
          .setCoder(KvCoder.of(keyCoder, in.getCoder()));
      PCollection<KV<K, Iterable<T>>> groupped = keyed.apply(GroupByKey.create());
      return applyGroupReduce(groupped, reducerDehydrated)
          .setCoder(PairCoder.of(keyCoder, valueCoder));
    });

  }

  private static class GroupReduce<K, T, O> extends DoFn<KV<K, Iterable<T>>, Pair<K, O>> {

    private final Closure<Iterable<O>> reducer;

    GroupReduce(Closure<Iterable<O>> reducer) {
      this.reducer = reducer;
    }

    @ProcessElement
    public void process(
        @Element KV<K, Iterable<T>> elem,
        BoundedWindow window,
        OutputReceiver<Pair<K, O>> output) {

      Iterable<O> res = reducer.call(window, elem.getValue());
      res.forEach(o -> output.output(Pair.of(elem.getKey(), o)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeDescriptor<Pair<K, O>> getOutputTypeDescriptor() {
      return (TypeDescriptor) TypeDescriptor.of(Object.class);
    }

  }


  private static <K, V, T> PCollection<Pair<K, V>> applyGroupReduce(
      PCollection<KV<K, Iterable<T>>> in, Closure<Iterable<V>> reducer) {

    return in.apply(ParDo.of(new GroupReduce<>(reducer)));
  }

  @SuppressWarnings("unchecked")
  private Window<T> createWindowFn() {
    Window ret = (Window) Window.into(windowing);
    switch (mode) {
      case ACCUMULATING_FIRED_PANES:
        ret = ret.accumulatingFiredPanes();
        break;
      case DISCARDING_FIRED_PANES:
        ret = ret.discardingFiredPanes();
        break;
      default:
        throw new IllegalArgumentException("Unknown mode " + mode);
    }
    return ret
        .triggering(createTrigger())
        .withAllowedLateness(Duration.millis(allowedLateness));
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> combine(
      Closure<K> keyExtractor, Closure<V> valueExtractor,
      V initial, Closure<V> combine) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<V> combineDehydrated = dehydrate(combine);

    return descendant(pipeline -> {
      Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
      Coder<V> valueCoder = coderOf(pipeline, valueExtractor);
      return asPairs(
          ReduceByKey.of(collection.materialize(pipeline))
              .keyBy(keyDehydrated::call)
              .valueBy(valueDehydrated::call)
              .combineBy((java.util.stream.Stream<V> in) ->
                  in.reduce(initial, combineDehydrated::call))
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .output()
              .setCoder(KvCoder.of(keyCoder, valueCoder)),
          keyCoder, valueCoder);
    });
  }

  @Override
  public <K> WindowedStream<Pair<K, T>> combine(
      Closure<K> keyExtractor, T initial, Closure<T> combine) {


    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<T> combineDehydrated = dehydrate(combine);

    return descendant(pipeline -> {
      Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
      Coder<T> valueCoder = coderOf(pipeline, combine);
      return asPairs(
          ReduceByKey.of(collection.materialize(pipeline))
              .keyBy(keyDehydrated::call)
              .valueBy(e -> e)
              .combineBy(in -> in.reduce(initial, combineDehydrated::call))
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .output()
              .setCoder(KvCoder.of(keyCoder, valueCoder)),
          keyCoder, valueCoder);
    });
  }

  @Override
  public <K> WindowedStream<Pair<K, Long>> countByKey(
      Closure<K> keyExtractor) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    return descendant(pipeline -> {
      Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
      Coder<Long> valueCoder = getCoder(pipeline, TypeDescriptors.longs());
      return asPairs(
          ReduceByKey.of(collection.materialize(pipeline))
              .keyBy(keyDehydrated::call)
              .valueBy(e -> 1L, TypeDescriptors.longs())
              .combineBy(Sums.ofLongs(), TypeDescriptors.longs())
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .output(),
          keyCoder, valueCoder);
    });
  }

  @Override
  public WindowedStream<Double> average(Closure<Double> valueExtractor) {

    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
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
          .output()
          .setCoder(DoubleCoder.of());
    });
  }

  @Override
  public <K> WindowedStream<Pair<K, Double>> averageByKey(
      Closure<K> keyExtractor, Closure<Double> valueExtractor) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<Double> valueDehydrated = dehydrate(valueExtractor);

    return descendant(pipeline -> {
      Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
      Coder<KV<Double, Long>> valueCoder = getCoder(pipeline, TypeDescriptors.kvs(
          TypeDescriptors.doubles(), TypeDescriptors.longs()));
      PCollection<KV<K, KV<Double, Long>>> intermediate = ReduceByKey
          .of(collection.materialize(pipeline))
          .keyBy(keyDehydrated::call)
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
          .output()
          .setCoder(KvCoder.of(keyCoder, valueCoder));
      return MapElements.of(intermediate)
          .using(
              p -> Pair.of(
                  p.getKey(), p.getValue().getKey() / p.getValue().getValue()))
          .output()
          .setCoder(PairCoder.of(
              keyCoder,
              getCoder(pipeline, TypeDescriptors.doubles())));
    });
  }

  @Override
  public <K, OTHER> WindowedStream<Pair<T, OTHER>> join(
      WindowedStream<OTHER> right, Closure<K> leftKey,
      Closure<K> rightKey) {

    Closure<K> leftKeyDehydrated = dehydrate(leftKey);
    Closure<K> rightKeyDehydrated = dehydrate(rightKey);
    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, leftKey);
          PCollection<T> lc = collection.materialize(pipeline);
          PCollection<OTHER> rc = ((BeamWindowedStream<OTHER>) right)
              .collection.materialize(pipeline);
          return Join.of(lc, rc)
              .by(leftKeyDehydrated::call, rightKeyDehydrated::call)
              .using((T l, OTHER r, Collector<Pair<T, OTHER>> ctx) ->
                  ctx.collect(Pair.of(l, r)))
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .outputValues()
              .setCoder(PairCoder.of(lc.getCoder(), rc.getCoder()));
        });
  }

  @Override
  public <K, RIGHT> WindowedStream<Pair<T, RIGHT>> leftJoin(
      WindowedStream<RIGHT> right, Closure<K> leftKey,
      Closure<K> rightKey) {

    Closure<K> leftKeyDehydrated = dehydrate(leftKey);
    Closure<K> rightKeyDehydrated = dehydrate(rightKey);

    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, leftKey);
          PCollection<T> lc = collection.materialize(pipeline);
          PCollection<RIGHT> rc = ((BeamWindowedStream<RIGHT>) right)
              .collection.materialize(pipeline);
          return LeftJoin.of(lc, rc)
              .by(leftKeyDehydrated::call, rightKeyDehydrated::call)
              .using((T l, Optional<RIGHT> r, Collector<Pair<T, RIGHT>> ctx) ->
                  ctx.collect(Pair.of(l, r.orElse(null))))
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .outputValues()
              .setCoder(PairCoder.of(lc.getCoder(), rc.getCoder()));
        });
  }

  @Override
  public WindowedStream<T> sorted(Closure<Integer> compareFn) {
    Closure<Integer> dehydrated = dehydrate(compareFn);
    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      return ReduceByKey
          .of(in)
          .keyBy(e -> null, TypeDescriptors.nulls())
          .reduceBy((Stream<T> values, Collector<T> ctx) ->
              values.forEach(ctx::collect))
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
              values.forEach(e -> ctx.collect(e)))
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
    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
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

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
    return descendant(pipeline -> {
      Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
      Coder<Double> valueCoder = getCoder(pipeline, TypeDescriptors.doubles());
      return asPairs(
          ReduceByKey.of(collection.materialize(pipeline))
              .keyBy(keyDehydrated::call)
              .valueBy(valueDehydrated::call, TypeDescriptors.doubles())
              .combineBy(Fold.of(0.0, (a, b) -> a + b), TypeDescriptors.doubles())
              .windowBy(windowing)
              .triggeredBy(createTrigger())
              .accumulationMode(mode)
              .withAllowedLateness(Duration.millis(allowedLateness))
              .output()
              .setCoder(KvCoder.of(keyCoder, valueCoder)),
          keyCoder, valueCoder);
    });
  }

  @Override
  public WindowedStream<T> distinct() {
    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      PCollection<KV<T, Void>> distinct = ReduceByKey
          .of(in)
          .keyBy(e -> e)
          .valueBy(e -> null, TypeDescriptors.nulls())
          .combineBy(e -> null, TypeDescriptors.nulls())
          .windowBy(windowing)
          .triggeredBy(createTrigger())
          .accumulationMode(mode)
          .withAllowedLateness(Duration.millis(allowedLateness))
          .output()
          .setCoder(KvCoder.of(in.getCoder(), VoidCoder.of()));

      return MapElements.of(distinct)
          .using(KV::getKey)
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
    Closure<Object> dehydrated = (Closure) dehydrate(mapper);
    return descendant(pipeline -> {
      Coder<Object> keyCoder = (Coder) coderOf(pipeline, mapper);
      PCollection<T> in = collection.materialize(pipeline);
      return ReduceByKey
        .of(in)
        .keyBy(dehydrated::call)
        .combineBy(e -> e.findAny().orElseThrow(
            () -> new IllegalStateException("Processing empty key?")))
        .windowBy(windowing)
        .triggeredBy(createTrigger())
        .accumulationMode(mode)
        .withAllowedLateness(Duration.millis(allowedLateness))
        .outputValues()
        .setCoder(in.getCoder());
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
        config, bounded, provider, (WindowFn) windowing, mode, terminateCheck,
        pipelineFactory);
  }

  private static <K, V> PCollection<Pair<K, V>> asPairs(
      PCollection<KV<K, V>> kvs,
      Coder<K> keyCoder,
      Coder<V> valueCoder) {

    return MapElements.of(kvs)
        .using(kv -> Pair.of(kv.getKey(), kv.getValue()))
        .output()
        .setCoder(PairCoder.of(keyCoder, valueCoder));
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
