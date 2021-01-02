/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
import cz.o2.proxima.tools.groovy.util.Types;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Distinct;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

/** A {@link WindowedStream} backed by beam. */
class BeamWindowedStream<T> extends BeamStream<T> implements WindowedStream<T> {

  private static final String REDUCE_SUFFIX = ".reduce";

  @SuppressWarnings("unchecked")
  BeamWindowedStream(
      StreamConfig config,
      boolean bounded,
      PCollectionProvider<T> input,
      WindowingStrategy<Object, ?> windowingStrategy,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory) {

    super(config, bounded, input, windowingStrategy, terminateCheck, pipelineFactory);
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      @Nullable String name,
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initialValue,
      Closure<V> reducer) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<V> reducerDehydrated = dehydrate(reducer);
    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
          Coder<V> valueCoder = coderOf(pipeline, valueDehydrated);
          PCollection<T> input = collection.materialize(pipeline);
          PCollection<KV<K, V>> kvs =
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(keyDehydrated::call)
                  .valueBy(valueDehydrated::call)
                  .reduceBy(
                      (java.util.stream.Stream<V> in) -> {
                        V current = initialValue;
                        return in.reduce(current, reducerDehydrated::call);
                      })
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder));

          return asPairs(withSuffix(name, ".asPairs"), kvs, keyCoder, valueCoder);
        });
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      @Nullable String name, Closure<K> keyExtractor, V initialValue, Closure<V> reducer) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> reducerDehydrated = dehydrate(reducer);
    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
          Coder<V> valueCoder = coderOf(pipeline, reducerDehydrated);
          PCollection<T> input = collection.materialize(pipeline);
          return asPairs(
              withSuffix(name, ".asPairs"),
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(keyDehydrated::call)
                  .valueBy(e -> e)
                  .reduceBy(
                      (java.util.stream.Stream<T> in) -> {
                        V current = initialValue;
                        Iterable<T> iter = in::iterator;
                        for (T v : iter) {
                          current = reducerDehydrated.call(current, v);
                        }
                        return current;
                      })
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder)),
              keyCoder,
              valueCoder);
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<StreamElement> reduceToLatest(@Nullable String name) {
    return descendant(
        pipeline ->
            PCollectionTools.reduceAsSnapshot(
                name, (PCollection) collection.materialize(pipeline)));
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> groupReduce(
      @Nullable String name, Closure<K> keyExtractor, Closure<Iterable<V>> listReduce) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<Iterable<V>> reducerDehydrated = dehydrate(listReduce);

    return descendant(
        pipeline -> {
          final Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
          // FIXME: need a way to retrieve inner type of the list
          @SuppressWarnings("unchecked")
          final Coder<V> valueCoder = (Coder) getCoder(pipeline, TypeDescriptor.of(Object.class));
          PCollection<T> input = collection.materialize(pipeline);
          if (!input.getWindowingStrategy().equals(windowingStrategy)) {
            if (name != null) {
              input = input.apply(name + ".windowFn", createWindowFn());
            } else {
              input = input.apply(createWindowFn());
            }
          }
          PCollection<KV<K, T>> keyed =
              MapElements.named(withSuffix(name, ".mapToKvs"))
                  .of(input)
                  .using(el -> KV.of(keyDehydrated.call(el), el))
                  .output()
                  .setCoder(KvCoder.of(keyCoder, input.getCoder()));

          // use native beam, beamphoria doesn't allow access
          // to window label as of 2.12
          final PCollection<KV<K, Iterable<T>>> groupped;
          if (name != null) {
            groupped = keyed.apply(name + ".groupByKey", GroupByKey.create());
          } else {
            groupped = keyed.apply(GroupByKey.create());
          }
          return applyGroupReduce(
                  withSuffix(name, ".applyGroupReduce"), groupped, reducerDehydrated)
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
        @Element KV<K, Iterable<T>> elem, BoundedWindow window, OutputReceiver<Pair<K, O>> output) {

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
      @Nullable String name, PCollection<KV<K, Iterable<T>>> in, Closure<Iterable<V>> reducer) {

    if (name != null) {
      return in.apply(name, ParDo.of(new GroupReduce<>(reducer)));
    }
    return in.apply(ParDo.of(new GroupReduce<>(reducer)));
  }

  @SuppressWarnings("unchecked")
  private Window<T> createWindowFn() {
    return createWindowFn((WindowingStrategy) windowingStrategy, getTrigger());
  }

  private static <T> Window<T> createWindowFn(
      WindowingStrategy<T, ?> windowingStrategy, Trigger trigger) {
    Window<T> ret = Window.<T>into(windowingStrategy.getWindowFn());
    switch (windowingStrategy.getMode()) {
      case ACCUMULATING_FIRED_PANES:
        ret = ret.accumulatingFiredPanes();
        break;
      case DISCARDING_FIRED_PANES:
        ret = ret.discardingFiredPanes();
        break;
      default:
        throw new IllegalArgumentException("Unknown mode " + windowingStrategy.getMode());
    }
    return ret.triggering(trigger).withAllowedLateness(windowingStrategy.getAllowedLateness());
  }

  private <
          O extends W,
          W extends Builders.WindowedOutput<O>,
          A extends Builders.AccumulationMode<W>,
          T extends Builders.TriggeredBy<A>>
      O createWindowFn(Builders.WindowBy<T> b) {
    return b.windowBy(windowingStrategy.getWindowFn())
        .triggeredBy(getTrigger())
        .accumulationMode(windowingStrategy.getMode())
        .withAllowedLateness(
            windowingStrategy.getAllowedLateness(), windowingStrategy.getClosingBehavior())
        .withOnTimeBehavior(windowingStrategy.getOnTimeBehavior())
        .withTimestampCombiner(windowingStrategy.getTimestampCombiner());
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> combine(
      @Nullable String name,
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initial,
      Closure<V> combine) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<V> combineDehydrated = dehydrate(combine);

    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
          Coder<V> valueCoder = coderOf(pipeline, valueExtractor);
          PCollection<T> input = collection.materialize(pipeline);
          return asPairs(
              withSuffix(name, ".asPairs"),
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(keyDehydrated::call)
                  .valueBy(valueDehydrated::call)
                  .combineBy(
                      (java.util.stream.Stream<V> in) ->
                          in.reduce(initial, combineDehydrated::call))
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder)),
              keyCoder,
              valueCoder);
        });
  }

  @Override
  public <K> WindowedStream<Pair<K, T>> combine(
      @Nullable String name, Closure<K> keyExtractor, T initial, Closure<T> combine) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<T> combineDehydrated = dehydrate(combine);

    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
          Coder<T> valueCoder = coderOf(pipeline, combine);
          PCollection<T> input = collection.materialize(pipeline);
          return asPairs(
              withSuffix(name, ".asPairs"),
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(keyDehydrated::call)
                  .valueBy(e -> e)
                  .combineBy(in -> in.reduce(initial, combineDehydrated::call))
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder)),
              keyCoder,
              valueCoder);
        });
  }

  @Override
  public <K> WindowedStream<Pair<K, Long>> countByKey(
      @Nullable String name, Closure<K> keyExtractor) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
          Coder<Long> valueCoder = getCoder(pipeline, TypeDescriptors.longs());
          PCollection<T> input = collection.materialize(pipeline);
          return asPairs(
              withSuffix(name, ".asPairs"),
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(keyDehydrated::call)
                  .valueBy(e -> 1L, TypeDescriptors.longs())
                  .combineBy(Sums.ofLongs())
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .output(),
              keyCoder,
              valueCoder);
        });
  }

  @Override
  public WindowedStream<Double> average(@Nullable String name, Closure<Double> valueExtractor) {

    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
    return descendant(
        pipeline -> {
          PCollection<T> input = collection.materialize(pipeline);
          PCollection<KV<Double, Long>> intermediate =
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(e -> "", TypeDescriptors.strings())
                  .valueBy(
                      e -> KV.of(valueDehydrated.call(e), 1L),
                      TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()))
                  .combineBy(
                      Fold.of(
                          (a, b) -> KV.of(a.getKey() + b.getKey(), a.getValue() + b.getValue())),
                      TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()))
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .outputValues();
          intermediate.setTypeDescriptor(
              TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()));

          return MapElements.named(withSuffix(name, ".mapToResult"))
              .of(intermediate)
              .using(p -> p.getKey() / p.getValue(), TypeDescriptors.doubles())
              .output()
              .setCoder(DoubleCoder.of());
        });
  }

  @Override
  public <K> WindowedStream<Pair<K, Double>> averageByKey(
      @Nullable String name, Closure<K> keyExtractor, Closure<Double> valueExtractor) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<Double> valueDehydrated = dehydrate(valueExtractor);

    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
          Coder<KV<Double, Long>> valueCoder =
              getCoder(
                  pipeline,
                  TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()));
          PCollection<T> input = collection.materialize(pipeline);
          PCollection<KV<K, KV<Double, Long>>> intermediate =
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(keyDehydrated::call)
                  .valueBy(
                      e -> KV.of(valueDehydrated.call(e), 1L),
                      TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()))
                  .combineBy(
                      Fold.of(
                          (a, b) -> KV.of(a.getKey() + b.getKey(), a.getValue() + b.getValue())),
                      TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.longs()))
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          return MapElements.named(withSuffix(name, ".mapToResult"))
              .of(intermediate)
              .using(p -> Pair.of(p.getKey(), p.getValue().getKey() / p.getValue().getValue()))
              .output()
              .setCoder(PairCoder.of(keyCoder, getCoder(pipeline, TypeDescriptors.doubles())));
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, RIGHT> WindowedStream<Pair<T, RIGHT>> join(
      @Nullable String name, WindowedStream<RIGHT> right, Closure<K> leftKey, Closure<K> rightKey) {

    Closure<K> leftKeyDehydrated = dehydrate(leftKey);
    Closure<K> rightKeyDehydrated = dehydrate(rightKey);
    return descendant(
        pipeline -> {
          JoinInputs<K, T, RIGHT> joinInputs =
              new JoinInputs<>(
                  this,
                  (BeamWindowedStream<RIGHT>) right,
                  leftKeyDehydrated,
                  rightKeyDehydrated,
                  windowingStrategy,
                  this::getTrigger,
                  pipeline);
          PCollection<KV<K, T>> leftKv = joinInputs.getLeftKv();
          PCollection<KV<K, RIGHT>> rightKv = joinInputs.getRightKv();
          return MapElements.of(Join.innerJoin(leftKv, rightKv))
              .using(kv -> Pair.of(kv.getValue().getKey(), kv.getValue().getValue()))
              .output()
              .setCoder(PairCoder.of(joinInputs.getLeftCoder(), joinInputs.getRightCoder()));
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, RIGHT> WindowedStream<Pair<T, RIGHT>> leftJoin(
      @Nullable String name, WindowedStream<RIGHT> right, Closure<K> leftKey, Closure<K> rightKey) {

    Closure<K> leftKeyDehydrated = dehydrate(leftKey);
    Closure<K> rightKeyDehydrated = dehydrate(rightKey);
    return descendant(
        pipeline -> {
          JoinInputs<K, T, RIGHT> joinInputs =
              new JoinInputs<>(
                  this,
                  (BeamWindowedStream<RIGHT>) right,
                  leftKeyDehydrated,
                  rightKeyDehydrated,
                  windowingStrategy,
                  this::getTrigger,
                  pipeline);

          final TupleTag<T> leftTuple = new TupleTag<>();
          final TupleTag<RIGHT> rightTuple = new TupleTag<>();

          PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
              KeyedPCollectionTuple.of(leftTuple, joinInputs.getLeftKv())
                  .and(rightTuple, joinInputs.getRightKv())
                  .apply(CoGroupByKey.create());

          return coGbkResultCollection
              .apply(ParDo.of(new JoinFn<>(leftTuple, rightTuple)))
              .setCoder(
                  PairCoder.of(
                      joinInputs.getLeftCoder(), NullableCoder.of(joinInputs.getRightCoder())));
        });
  }

  @Override
  public WindowedStream<T> sorted(@Nullable String name, Closure<Integer> compareFn) {

    Closure<Integer> dehydrated = dehydrate(compareFn);
    return descendant(
        pipeline -> {
          PCollection<T> input = collection.materialize(pipeline);
          return ReduceByKey.named(name)
              .of(input)
              .keyBy(e -> null, TypeDescriptors.nulls())
              .reduceBy((Stream<T> values, Collector<T> ctx) -> values.forEach(ctx::collect))
              .withSortedValues(dehydrated::call)
              .applyIf(
                  !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
              .outputValues();
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<Comparable<T>> sorted(@Nullable String name) {
    return (WindowedStream)
        descendant(
            pipeline -> {
              PCollection<T> input = collection.materialize(pipeline);
              return ReduceByKey.named(name)
                  .of((PCollection<Comparable<T>>) input)
                  .keyBy(e -> null, TypeDescriptors.nulls())
                  .reduceBy((values, ctx) -> values.forEach(ctx::collect))
                  .withSortedValues((a, b) -> a.compareTo((T) b))
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .outputValues();
            });
  }

  @Override
  public WindowedStream<Long> count(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> input = collection.materialize(pipeline);
          return ReduceByKey.named(name)
              .of(input)
              .keyBy(e -> null, TypeDescriptors.nulls())
              .valueBy(e -> 1L, TypeDescriptors.longs())
              .combineBy(Sums.ofLongs())
              .applyIf(
                  !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
              .outputValues();
        });
  }

  @Override
  public WindowedStream<Double> sum(@Nullable String name, Closure<Double> valueExtractor) {

    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
    return descendant(
        pipeline -> {
          PCollection<T> input = collection.materialize(pipeline);
          return ReduceByKey.named(name)
              .of(input)
              .keyBy(e -> null, TypeDescriptors.nulls())
              .valueBy(valueDehydrated::call, TypeDescriptors.doubles())
              .combineBy(Sums.ofDoubles())
              .applyIf(
                  !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
              .outputValues();
        });
  }

  @Override
  public <K> WindowedStream<Pair<K, Double>> sumByKey(
      @Nullable String name, Closure<K> keyExtractor, Closure<Double> valueExtractor) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
    return descendant(
        pipeline -> {
          Coder<K> keyCoder = coderOf(pipeline, keyExtractor);
          Coder<Double> valueCoder = getCoder(pipeline, TypeDescriptors.doubles());
          PCollection<T> input = collection.materialize(pipeline);
          return asPairs(
              withSuffix(name, ".asPairs"),
              ReduceByKey.named(withSuffix(name, REDUCE_SUFFIX))
                  .of(input)
                  .keyBy(keyDehydrated::call)
                  .valueBy(valueDehydrated::call, TypeDescriptors.doubles())
                  .combineBy(Sums.ofDoubles())
                  .applyIf(
                      !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder)),
              keyCoder,
              valueCoder);
        });
  }

  @Override
  public WindowedStream<T> distinct(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> input = collection.materialize(pipeline);
          return Distinct.named(name)
              .of(input)
              .applyIf(
                  !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
              .output()
              .setCoder(input.getCoder());
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<T> distinct(@Nullable String name, Closure<?> mapper) {
    Closure<Object> dehydrated = (Closure) dehydrate(mapper);
    return descendant(
        pipeline -> {
          PCollection<T> input = collection.materialize(pipeline);
          return Distinct.named(name)
              .of(input)
              .projected(dehydrated::call, Distinct.SelectionPolicy.NEWEST)
              .applyIf(
                  !windowingStrategy.equals(input.getWindowingStrategy()), this::createWindowFn)
              .output()
              .setCoder(input.getCoder());
        });
  }

  @Override
  public WindowedStream<T> withEarlyEmitting(long duration) {
    this.windowingStrategy =
        windowingStrategy.withTrigger(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.millis(duration)))
                .withLateFirings(AfterPane.elementCountAtLeast(1)));
    return this;
  }

  @Override
  public WindowedStream<T> withAllowedLateness(long lateness) {
    this.windowingStrategy = windowingStrategy.withAllowedLateness(Duration.millis(lateness));
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  <X> BeamWindowedStream<X> descendant(Function<Pipeline, PCollection<X>> factory) {
    return new BeamWindowedStream<>(
        config,
        bounded,
        PCollectionProvider.withParents(factory, collection),
        windowingStrategy,
        terminateCheck,
        pipelineFactory);
  }

  BeamWindowedStream<T> intoGlobalWindow() {
    return new BeamWindowedStream<>(
        config,
        bounded,
        PCollectionProvider.withParents(
            pipeline -> collection.materialize(pipeline).apply(Window.into(new GlobalWindows())),
            collection),
        WindowingStrategy.globalDefault(),
        terminateCheck,
        pipelineFactory);
  }

  @Override
  WindowFn<Object, ? extends BoundedWindow> getWindowFn() {
    return this.windowingStrategy.getWindowFn();
  }

  @Override
  Trigger getTrigger() {
    return windowingStrategy.getTrigger();
  }

  @Override
  public BeamWindowedStream<T> windowAll() {
    if (!windowingStrategy.equals(WindowingStrategy.globalDefault())) {
      return intoGlobalWindow();
    }
    return this;
  }

  private static <K, V> PCollection<Pair<K, V>> asPairs(
      @Nullable String name, PCollection<KV<K, V>> kvs, Coder<K> keyCoder, Coder<V> valueCoder) {

    return MapElements.named(name)
        .of(kvs)
        .using(kv -> Pair.of(kv.getKey(), kv.getValue()))
        .output()
        .setCoder(PairCoder.of(keyCoder, valueCoder));
  }

  private static class JoinFn<K, LEFT, RIGHT> extends DoFn<KV<K, CoGbkResult>, Pair<LEFT, RIGHT>> {

    private final TupleTag<LEFT> leftTuple;
    private final TupleTag<RIGHT> rightTuple;

    JoinFn(TupleTag<LEFT> leftTuple, TupleTag<RIGHT> rightTuple) {
      this.leftTuple = leftTuple;
      this.rightTuple = rightTuple;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<K, CoGbkResult> e = c.element();

      Iterable<LEFT> leftValuesIterable = e.getValue().getAll(leftTuple);
      Iterable<RIGHT> rightValuesIterable = e.getValue().getAll(rightTuple);

      for (LEFT leftValue : leftValuesIterable) {
        if (rightValuesIterable.iterator().hasNext()) {
          for (RIGHT rightValue : rightValuesIterable) {
            c.output(Pair.of(leftValue, rightValue));
          }
        } else {
          c.output(Pair.of(leftValue, null));
        }
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public TypeDescriptor<Pair<LEFT, RIGHT>> getOutputTypeDescriptor() {
      return (TypeDescriptor)
          PairCoder.descriptor(TypeDescriptor.of(Object.class), TypeDescriptor.of(Object.class));
    }
  }

  private static class JoinInputs<K, LEFT, RIGHT> {

    @Getter private final PCollection<KV<K, LEFT>> leftKv;
    @Getter private final PCollection<KV<K, RIGHT>> rightKv;
    @Getter private final Coder<LEFT> leftCoder;
    @Getter private final Coder<RIGHT> rightCoder;

    @SuppressWarnings("unchecked")
    public JoinInputs(
        BeamWindowedStream<LEFT> left,
        BeamWindowedStream<RIGHT> right,
        Closure<K> leftKeyDehydrated,
        Closure<K> rightKeyDehydrated,
        WindowingStrategy<?, ?> windowingStrategy,
        Supplier<Trigger> triggerSupplier,
        Pipeline pipeline) {

      PCollection<LEFT> leftTmp = left.collection.materialize(pipeline);
      PCollection<RIGHT> rightTmp = right.collection.materialize(pipeline);
      if (!windowingStrategy.equals(leftTmp.getWindowingStrategy())) {
        leftTmp =
            leftTmp.apply(
                createWindowFn(
                    (WindowingStrategy<LEFT, ?>) windowingStrategy, triggerSupplier.get()));
      }
      if (!windowingStrategy.equals(rightTmp.getWindowingStrategy())) {
        rightTmp =
            (PCollection)
                rightTmp.apply(
                    createWindowFn(
                        (WindowingStrategy<RIGHT, ?>) windowingStrategy, triggerSupplier.get()));
      }
      TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(leftKeyDehydrated));
      Coder<K> keyCoder = getCoder(pipeline, keyType);
      this.leftKv =
          MapElements.of(leftTmp)
              .using(e -> KV.of(leftKeyDehydrated.call(e), e))
              .output()
              .setCoder(KvCoder.of(keyCoder, leftTmp.getCoder()));
      this.rightKv =
          MapElements.of(rightTmp)
              .using(e -> KV.of(rightKeyDehydrated.call(e), e))
              .output()
              .setCoder(KvCoder.of(keyCoder, rightTmp.getCoder()));
      this.leftCoder = leftTmp.getCoder();
      this.rightCoder = rightTmp.getCoder();
    }
  }
}
