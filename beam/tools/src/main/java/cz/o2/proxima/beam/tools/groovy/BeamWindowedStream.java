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
package cz.o2.proxima.beam.tools.groovy;

import cz.o2.proxima.beam.core.PCollectionTools;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.collect.Streams;
import cz.o2.proxima.tools.groovy.RepositoryProvider;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.tools.groovy.util.Types;
import groovy.lang.Closure;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.joda.time.Duration;

/** A {@link WindowedStream} backed by beam. */
class BeamWindowedStream<T> extends BeamStream<T> implements WindowedStream<T> {

  private static final String REDUCE_SUFFIX = ".reduce";
  private static final String WINDOW_SUFFIX = ".window";
  public static final String COMBINE_SUFFIX = ".combine";

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
          PCollection<T> input = getCollection().materialize(pipeline);
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyDehydrated));
          TypeDescriptor<V> valueType = TypeDescriptor.of(Types.returnClass(valueDehydrated));
          input = applyWindowing(name, REDUCE_SUFFIX + WINDOW_SUFFIX, input);
          PCollection<KV<K, V>> keyValues =
              input.apply(
                  MapElements.into(TypeDescriptors.kvs(keyType, valueType))
                      .via(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e))));
          PCollection<KV<K, V>> kvs =
              applyNamedTransform(name, REDUCE_SUFFIX + ".gbk", keyValues, GroupByKey.create())
                  .apply(
                      MapElements.into(TypeDescriptors.kvs(keyType, valueType))
                          .via(
                              e ->
                                  KV.of(
                                      e.getKey(),
                                      Streams.stream(e.getValue())
                                          .reduce(initialValue, reducerDehydrated::call))))
                  .setCoder(KvCoder.of(keyCoder, valueCoder));

          return asPairs(kvs, keyCoder, valueCoder);
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
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyDehydrated));
          TypeDescriptor<V> valueType = TypeDescriptor.of(Types.returnClass(reducerDehydrated));
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, REDUCE_SUFFIX + WINDOW_SUFFIX, input);
          PCollection<KV<K, T>> withKeys =
              input.apply(WithKeys.<K, T>of(keyDehydrated::call).withKeyType(keyType));
          PCollection<KV<K, V>> kvs =
              applyNamedTransform(name, REDUCE_SUFFIX + ".gbk", withKeys, GroupByKey.create())
                  .apply(
                      MapElements.into(TypeDescriptors.kvs(keyType, valueType))
                          .via(
                              e -> {
                                V current = initialValue;
                                for (T v : e.getValue()) {
                                  current = reducerDehydrated.call(current, v);
                                }
                                return KV.of(e.getKey(), current);
                              }))
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          return asPairs(kvs, keyCoder, valueCoder);
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<StreamElement> reduceToLatest(@Nullable String name) {
    return descendant(
        pipeline ->
            PCollectionTools.reduceAsSnapshot(
                name, (PCollection) getCollection().materialize(pipeline)));
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
          @SuppressWarnings({"unchecked", "rawtypes"})
          final Coder<V> valueCoder = (Coder) getCoder(pipeline, TypeDescriptor.of(Object.class));
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyDehydrated));
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<KV<K, T>> withKeys =
              input
                  .apply(WithKeys.<K, T>of(keyDehydrated::call).withKeyType(keyType))
                  .setCoder(KvCoder.of(keyCoder, input.getCoder()));

          return applyNamedTransform(name, ".gbk", withKeys, GroupByKey.create())
              .apply(ParDo.of(new GroupReduce<K, T, V>(reducerDehydrated)))
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public TypeDescriptor<Pair<K, O>> getOutputTypeDescriptor() {
      return (TypeDescriptor) TypeDescriptor.of(Object.class);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Window<T> createWindowTransform() {
    return createWindowTransform((WindowingStrategy) windowingStrategy, getTrigger());
  }

  private static <T> Window<T> createWindowTransform(
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
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyDehydrated));
          TypeDescriptor<V> valueType = TypeDescriptor.of(Types.returnClass(valueDehydrated));
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<KV<K, V>> keyValues =
              input.apply(
                  MapElements.into(TypeDescriptors.kvs(keyType, valueType))
                      .via(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e))));
          PCollection<KV<K, V>> kvs =
              applyNamedTransform(
                      name,
                      COMBINE_SUFFIX,
                      keyValues,
                      Combine.perKey((SerializableBiFunction<V, V, V>) combineDehydrated::call))
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          return asPairs(kvs, keyCoder, valueCoder);
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
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyDehydrated));
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<KV<K, T>> keyValues =
              input
                  .apply(WithKeys.<K, T>of(keyDehydrated::call).withKeyType(keyType))
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          PCollection<KV<K, T>> kvs =
              applyNamedTransform(
                      name,
                      COMBINE_SUFFIX,
                      keyValues,
                      Combine.perKey(
                          values ->
                              Streams.stream(values).reduce(initial, combineDehydrated::call)))
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          return asPairs(kvs, keyCoder, valueCoder);
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
          PCollection<T> input = getCollection().materialize(pipeline);
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyDehydrated));
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<KV<K, T>> keyValues =
              input.apply(WithKeys.<K, T>of(keyDehydrated::call).withKeyType(keyType));
          PCollection<KV<K, Long>> kvs =
              applyNamedTransform(name, ".count", keyValues, Count.perKey());
          return asPairs(kvs, keyCoder, valueCoder);
        });
  }

  @Override
  public WindowedStream<Double> average(@Nullable String name, Closure<Double> valueExtractor) {

    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
    return descendant(
        pipeline -> {
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<Double> keyValues =
              input.apply(MapElements.into(TypeDescriptors.doubles()).via(valueDehydrated::call));
          return applyNamedTransform(
              name, ".mean", keyValues, Combine.globally(Mean.<Double>of()).withoutDefaults());
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
          PCollection<T> input = getCollection().materialize(pipeline);
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyExtractor));
          Coder<Double> doubleCoder = getCoder(pipeline, TypeDescriptors.doubles());
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<KV<K, Double>> keyValues =
              input.apply(
                  MapElements.into(TypeDescriptors.kvs(keyType, TypeDescriptors.doubles()))
                      .via(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e))));
          PCollection<KV<K, Double>> kvs =
              applyNamedTransform(name, ".mean", keyValues, Mean.perKey());
          return asPairs(kvs, keyCoder, doubleCoder);
        });
  }

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
          PairCoder<T, RIGHT> resultCoder =
              PairCoder.of(joinInputs.getLeftCoder(), joinInputs.getRightCoder());

          PCollection<KV<K, KV<T, RIGHT>>> joined =
              name == null
                  ? Join.innerJoin(leftKv, rightKv)
                  : Join.innerJoin(name + ".join", leftKv, rightKv);
          return joined.apply(
              MapElements.into(resultCoder.getEncodedTypeDescriptor())
                  .via(kv -> Pair.of(kv.getValue().getKey(), kv.getValue().getValue())));
        });
  }

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

          KeyedPCollectionTuple<K> tuple =
              KeyedPCollectionTuple.of(leftTuple, joinInputs.getLeftKv())
                  .and(rightTuple, joinInputs.getRightKv());
          PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
              name == null
                  ? tuple.apply(CoGroupByKey.create())
                  : tuple.apply(name, CoGroupByKey.create());

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
          PCollection<T> input = getCollection().materialize(pipeline);
          TypeDescriptor<T> inputType = input.getCoder().getEncodedTypeDescriptor();
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          // FIXME: this stores everything in memory
          PCollection<KV<Void, T>> withKeys =
              input.apply(WithKeys.<Void, T>of((Void) null).withKeyType(TypeDescriptors.voids()));

          return applyNamedTransform(name, ".gbk", withKeys, GroupByKey.create())
              .apply(
                  FlatMapElements.into(inputType)
                      .via(
                          e ->
                              Streams.stream(e.getValue())
                                  .sorted(dehydrated::call)
                                  .collect(Collectors.toList())));
        });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public WindowedStream<Comparable<T>> sorted(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> input = getCollection().materialize(pipeline);
          TypeDescriptor<T> inputType = input.getCoder().getEncodedTypeDescriptor();
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          // FIXME: this stores everything in memory
          PCollection<KV<Void, T>> withKeys =
              input.apply(WithKeys.<Void, T>of((Void) null).withKeyType(TypeDescriptors.voids()));

          PCollection<KV<Void, Iterable<Comparable<T>>>> grouped =
              (PCollection) applyNamedTransform(name, ".gbk", withKeys, GroupByKey.create());
          return grouped.apply(
              FlatMapElements.into((TypeDescriptor<Comparable<T>>) inputType)
                  .via(
                      e ->
                          Streams.stream(e.getValue())
                              .sorted((Comparator<Comparable<T>>) Comparator.naturalOrder())
                              .collect(Collectors.toList())));
        });
  }

  @Override
  public WindowedStream<Long> count(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          return applyNamedTransform(
              name,
              COMBINE_SUFFIX,
              input,
              Combine.globally(Count.<T>combineFn()).withoutDefaults());
        });
  }

  @SuppressWarnings("unchecked")
  private PCollection<T> applyWindowing(
      @Nullable String name, String suffix, PCollection<T> input) {

    if (!equalWindowing(input.getWindowingStrategy())) {
      PCollection<T> ret = applyNamedTransform(name, suffix, input, createWindowTransform());
      windowingStrategy = (WindowingStrategy<Object, ?>) ret.getWindowingStrategy();
      return ret;
    }
    return input;
  }

  private boolean equalWindowing(WindowingStrategy<?, ?> other) {
    return windowingStrategy.isAlreadyMerged() == other.isAlreadyMerged()
        && windowingStrategy.getMode().equals(other.getMode())
        && windowingStrategy.getAllowedLateness().equals(other.getAllowedLateness())
        && windowingStrategy.getClosingBehavior().equals(other.getClosingBehavior())
        && windowingStrategy.getOnTimeBehavior().equals(other.getOnTimeBehavior())
        && windowingStrategy.getTrigger().equals(other.getTrigger())
        && windowingStrategy.getTimestampCombiner().equals(other.getTimestampCombiner())
        && windowingStrategy.getWindowFn().equals(other.getWindowFn())
        && windowingStrategy.getEnvironmentId().equals(other.getEnvironmentId());
  }

  @Override
  public WindowedStream<Double> sum(@Nullable String name, Closure<Double> valueExtractor) {

    Closure<Double> valueDehydrated = dehydrate(valueExtractor);
    return descendant(
        pipeline -> {
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<Double> keyValues =
              input.apply(MapElements.into(TypeDescriptors.doubles()).via(valueDehydrated::call));
          return applyNamedTransform(
              name, COMBINE_SUFFIX, keyValues, Combine.globally(Sum.ofDoubles()).withoutDefaults());
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
          TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(keyDehydrated));
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          PCollection<KV<K, Double>> keyValues =
              input.apply(
                  MapElements.into(TypeDescriptors.kvs(keyType, TypeDescriptors.doubles()))
                      .via(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e))));
          PCollection<KV<K, Double>> kvs =
              applyNamedTransform(name, ".sum", keyValues, Sum.doublesPerKey())
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          return asPairs(kvs, keyCoder, valueCoder);
        });
  }

  @Override
  public WindowedStream<T> distinct(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> input = getCollection().materialize(pipeline);
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          return applyNamedTransform(name, ".distinct", input, Distinct.create())
              .setCoder(input.getCoder());
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<T> distinct(@Nullable String name, Closure<?> mapper) {
    Closure<Object> dehydrated = (Closure) dehydrate(mapper);
    return descendant(
        pipeline -> {
          PCollection<T> input = getCollection().materialize(pipeline);
          SerializableFunction<T, Object> representativeFn = dehydrated::call;
          TypeDescriptor<Object> representativeType =
              TypeDescriptor.of(Types.returnClass(dehydrated));
          input = applyWindowing(name, WINDOW_SUFFIX, input);
          return applyNamedTransform(
                  name,
                  ".distinct",
                  input,
                  Distinct.withRepresentativeValueFn(representativeFn)
                      .withRepresentativeType(representativeType))
              .setCoder(input.getCoder());
        });
  }

  @Override
  public WindowedStream<T> withEarlyEmitting(long duration) {
    this.windowingStrategy =
        windowingStrategy
            .withTrigger(
                AfterWatermark.pastEndOfWindow()
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.millis(duration)))
                    .withLateFirings(AfterPane.elementCountAtLeast(1)))
            .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    return this;
  }

  @Override
  public WindowedStream<T> withAllowedLateness(long lateness) {
    this.windowingStrategy = windowingStrategy.withAllowedLateness(Duration.millis(lateness));
    return this;
  }

  @Override
  <X> BeamWindowedStream<X> descendant(Function<Pipeline, PCollection<X>> factory) {
    return new BeamWindowedStream<X>(
        config,
        bounded,
        PCollectionProvider.withParents(factory, getCollection()),
        windowingStrategy,
        getTerminateCheck(),
        getPipelineFactory()) {};
  }

  BeamWindowedStream<T> intoGlobalWindow() {
    return new BeamWindowedStream<T>(
        config,
        bounded,
        PCollectionProvider.withParents(
            pipeline ->
                getCollection().materialize(pipeline).apply(Window.into(new GlobalWindows())),
            getCollection()),
        WindowingStrategy.globalDefault(),
        getTerminateCheck(),
        getPipelineFactory()) {};
  }

  @Override
  public BeamWindowedStream<T> windowAll() {
    if (!windowingStrategy.equals(WindowingStrategy.globalDefault())) {
      return intoGlobalWindow();
    }
    return this;
  }

  private static <K, V> PCollection<Pair<K, V>> asPairs(
      PCollection<KV<K, V>> kvs, Coder<K> keyCoder, Coder<V> valueCoder) {

    TypeDescriptor<Pair<K, V>> type = new TypeDescriptor<Pair<K, V>>() {};
    return kvs.apply(MapElements.into(type).via(e -> Pair.of(e.getKey(), e.getValue())))
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

      PCollection<LEFT> leftTmp = left.getCollection().materialize(pipeline);
      PCollection<RIGHT> rightTmp = right.getCollection().materialize(pipeline);
      if (!windowingStrategy.equals(leftTmp.getWindowingStrategy())) {
        leftTmp =
            leftTmp.apply(
                createWindowTransform(
                    (WindowingStrategy<LEFT, ?>) windowingStrategy, triggerSupplier.get()));
      }
      if (!windowingStrategy.equals(rightTmp.getWindowingStrategy())) {
        rightTmp =
            (PCollection)
                rightTmp.apply(
                    createWindowTransform(
                        (WindowingStrategy<RIGHT, ?>) windowingStrategy, triggerSupplier.get()));
      }
      TypeDescriptor<K> keyType = TypeDescriptor.of(Types.returnClass(leftKeyDehydrated));
      Coder<K> keyCoder = getCoder(pipeline, keyType);
      this.leftKv =
          leftTmp
              .apply(
                  MapElements.into(TypeDescriptors.kvs(keyType, leftTmp.getTypeDescriptor()))
                      .via(e -> KV.of(leftKeyDehydrated.call(e), e)))
              .setCoder(KvCoder.of(keyCoder, leftTmp.getCoder()));
      this.rightKv =
          rightTmp
              .apply(
                  MapElements.into(TypeDescriptors.kvs(keyType, rightTmp.getTypeDescriptor()))
                      .via(e -> KV.of(rightKeyDehydrated.call(e), e)))
              .setCoder(KvCoder.of(keyCoder, rightTmp.getCoder()));
      this.leftCoder = leftTmp.getCoder();
      this.rightCoder = rightTmp.getCoder();
    }
  }

  // type-safety for methods not modifying windowfn

  @Override
  public <X> WindowedStream<X> flatMap(@Nullable String name, Closure<Iterable<X>> mapper) {
    return (WindowedStream<X>) super.flatMap(name, mapper);
  }

  @Override
  public <X> WindowedStream<X> map(@Nullable String name, Closure<X> mapper) {
    return (WindowedStream<X>) super.map(name, mapper);
  }

  @Override
  public WindowedStream<T> filter(@Nullable String name, Closure<Boolean> predicate) {
    return (WindowedStream<T>) super.filter(name, predicate);
  }

  @Override
  public WindowedStream<T> assignEventTime(@Nullable String name, Closure<Long> assigner) {
    return (WindowedStream<T>) super.assignEventTime(name, assigner);
  }

  @Override
  public WindowedStream<Pair<Object, T>> withWindow(@Nullable String name) {
    return (WindowedStream<Pair<Object, T>>) super.withWindow(name);
  }

  @Override
  public WindowedStream<Pair<T, Long>> withTimestamp(@Nullable String name) {
    return (WindowedStream<Pair<T, Long>>) super.withTimestamp(name);
  }

  @Override
  public WindowedStream<T> asUnbounded() {
    return (WindowedStream<T>) super.asUnbounded();
  }

  @Override
  public <V> BeamWindowedStream<StreamElement> asStreamElements(
      RepositoryProvider repoProvider,
      EntityDescriptor entity,
      Closure<CharSequence> keyExtractor,
      Closure<CharSequence> attributeExtractor,
      Closure<V> valueExtractor,
      Closure<Long> timeExtractor) {
    return (BeamWindowedStream<StreamElement>)
        super.asStreamElements(
            repoProvider, entity, keyExtractor, attributeExtractor, valueExtractor, timeExtractor);
  }
}
