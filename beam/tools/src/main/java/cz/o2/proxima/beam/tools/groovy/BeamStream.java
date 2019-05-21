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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Streams;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.tools.groovy.RepositoryProvider;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.tools.groovy.util.Types;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import fi.iki.elonen.NanoHTTPD;
import static fi.iki.elonen.NanoHTTPD.newFixedLengthResponse;
import groovy.lang.Closure;
import groovy.lang.Tuple;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.repackaged.beam_sdks_java_extensions_kryo.com.esotericsoftware.kryo.Kryo;
import org.apache.beam.repackaged.beam_sdks_java_extensions_kryo.org.objenesis.strategy.StdInstantiatorStrategy;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link Stream} implementation based on beam.
 */
@Slf4j
class BeamStream<T> implements Stream<T> {

  static <T, S extends BeamStream<T>> S withRegisteredTypes(
      Repository repo, S in) {

    return in.addRegistrar(r -> r.registerCoderForClass(
        StreamElement.class, StreamElementCoder.of(repo)));
  }

  @SafeVarargs
  static Stream<StreamElement> stream(
      BeamDataOperator beam,
      Position position, boolean stopAtCurrent, boolean eventTime,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>... attrs) {

    return withRegisteredTypes(beam.getRepository(),
        new BeamStream<>(
            asConfig(beam),
            stopAtCurrent,
            pipeline -> beam.getStream(
                pipeline, position, stopAtCurrent, eventTime, attrs),
            terminateCheck,
            pipelineFactory));
  }

  static WindowedStream<StreamElement> batchUpdates(
      BeamDataOperator beam,
      long startStamp, long endStamp,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>[] attrs) {

    return withRegisteredTypes(
        beam.getRepository(),
        new BeamStream<>(
            asConfig(beam),
            true,
            pipeline -> beam.getBatchUpdates(
                pipeline, startStamp, endStamp, attrs),
            terminateCheck,
            pipelineFactory))
        .windowAll();
  }

  static WindowedStream<StreamElement> batchSnapshot(
      BeamDataOperator beam,
      long fromStamp, long toStamp,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>[] attrs) {

    return withRegisteredTypes(
        beam.getRepository(),
        new BeamStream<>(
            asConfig(beam),
            true,
            pipeline -> beam.getBatchSnapshot(
                pipeline, fromStamp, toStamp, attrs),
            terminateCheck,
            pipelineFactory))
        .windowAll();
  }

  private static StreamConfig asConfig(BeamDataOperator beam) {
    return StreamConfig.of(beam);
  }

  final StreamConfig config;
  final boolean bounded;
  final PCollectionProvider<T> collection;
  final List<Consumer<CoderRegistry>> registrars = new ArrayList<>();
  final StreamProvider.TerminatePredicate terminateCheck;
  final Factory<Pipeline> pipelineFactory;

  BeamStream(
      StreamConfig config, boolean bounded, PCollectionProvider<T> input,
      StreamProvider.TerminatePredicate terminateCheck) {

    this(config, bounded, input, terminateCheck, BeamStream::createPipelineDefault);
  }


  BeamStream(
      StreamConfig config, boolean bounded, PCollectionProvider<T> input,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory) {

    this.config = config;
    this.bounded = bounded;
    this.collection = input;
    this.terminateCheck = terminateCheck;
    this.pipelineFactory = pipelineFactory;
  }

  @SuppressWarnings("unchecked")
  <S extends BeamStream<T>> S addRegistrar(Consumer<CoderRegistry> registrar) {
    registrars.add(registrar);
    return (S) this;
  }

  @Override
  public <X> Stream<X> flatMap(
      @Nullable String name, Closure<Iterable<X>> mapper) {

    Closure<Iterable<X>> dehydrated = dehydrate(mapper);
    return descendant(
        pipeline -> {
          // FIXME: need a way to retrieve inner type of the list
          @SuppressWarnings("unchecked")
          final Coder<Object> valueCoder = (Coder) getCoder(
              pipeline, TypeDescriptor.of(Object.class));
          @SuppressWarnings("unchecked")
          final PCollection<X> ret = (PCollection) FlatMap
              .named(name)
              .of(collection.materialize(pipeline))
              .using((elem, ctx) -> {
                dehydrated.call(elem).forEach(ctx::collect);
              })
              .output()
              .setCoder(valueCoder);
          return ret;
        });
  }

  @Override
  public <X> Stream<X> map(@Nullable String name, Closure<X> mapper) {
    Closure<X> dehydrated = dehydrate(mapper);
    return descendant(
        pipeline -> {
          Coder<X> coder = coderOf(pipeline, dehydrated);
          return MapElements
              .named(name)
              .of(collection.materialize(pipeline))
              .using(e -> dehydrated.call(e))
              .output()
              .setCoder(coder);
        });
  }

  @Override
  public Stream<T> filter(@Nullable String name, Closure<Boolean> predicate) {
    Closure<Boolean> dehydrated = dehydrate(predicate);
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return Filter
              .named(name)
              .of(in)
              .by(dehydrated::call)
              .output()
              .setCoder(in.getCoder());
        });
  }

  @Override
  public Stream<T> assignEventTime(@Nullable String name, Closure<Long> assigner) {
    Closure<Long> dehydrated = dehydrate(assigner);
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return AssignEventTime
              .named(name)
              .of(in)
              .using(dehydrated::call)
              .output()
              .setCoder(in.getCoder());
        });
  }

  @Override
  public Stream<Pair<Object, T>> withWindow(@Nullable String name) {
    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      return applyExtractWindow(name, in, pipeline);
    });
  }

  @SuppressWarnings("unchecked")
  static <T> PCollection<Pair<Object, T>> applyExtractWindow(
      @Nullable String name, PCollection<T> in, Pipeline pipeline) {

    TypeDescriptor<Object> windowType = (TypeDescriptor)
        in.getWindowingStrategy().getWindowFn().getWindowTypeDescriptor();
    Coder<Object> windowCoder = getCoder(pipeline, windowType);

    final PCollection<Pair<Object, T>> ret;
    if (name != null) {
      ret = (PCollection) in.apply(name, ParDo.of(ExtractWindow.of()));
    } else {
      ret = (PCollection) in.apply(ParDo.of(ExtractWindow.of()));
    }
    return ret.setCoder((Coder) PairCoder.of(windowCoder, in.getCoder()));
  }

  private void forEach(@Nullable String name, Consumer<T> consumer) {
    forEach(name, consumer, true);
  }

  private void forEach(
      @Nullable String name,
      Consumer<T> consumer,
      boolean gatherLocally) {

    Pipeline pipeline = createPipeline();
    PCollection<T> pcoll = collection.materialize(pipeline);
    if (gatherLocally) {
      try (RemoteConsumer<T> remoteConsumer = createRemoteConsumer(
          pcoll.getCoder(), consumer)) {
        forEachRemote(name, pcoll, remoteConsumer::add, pipeline);
      }
    } else {
      forEachRemote(name, pcoll, consumer, pipeline);
    }
  }

  private void forEachRemote(
      @Nullable String name,
      PCollection<T> pcoll,
      Consumer<T> consumer,
      Pipeline pipeline) {

    if (name != null) {
      pcoll.apply(name, asWriteTransform(asDoFn(consumer)));
    } else {
      pcoll.apply(asWriteTransform(asDoFn(consumer)));
    }
    AtomicReference<PipelineResult> result = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    Thread running = runThread("pipeline-start-thread", () -> {
      try {
        log.debug("Running pipeline with class loader {}, pipeline classloader {}",
            Thread.currentThread().getContextClassLoader(),
            pipeline.getClass().getClassLoader());
        result.set(pipeline.run());
        result.get().waitUntilFinish();
      } catch (Exception ex) {
        if (!(ex.getCause() instanceof InterruptedException)) {
          throw ex;
        } else {
          log.debug("Swallowing interrupted exception.", ex);
        }
      } finally {
        latch.countDown();
      }
    });
    AtomicBoolean watchTerminating = new AtomicBoolean();
    AtomicBoolean gracefulExit = new AtomicBoolean();
    Thread watch = runWatchThread(() -> {
      watchTerminating.set(true);
      if (!gracefulExit.get()) {
        cancelIfResultExists(result);
        running.interrupt();
        ExceptionUtils.unchecked(latch::await);
        cancelIfResultExists(result);
      }
    });
    ExceptionUtils.unchecked(latch::await);
    gracefulExit.set(true);
    if (!watchTerminating.get()) {
      watch.interrupt();
    }
  }

  @Override
  public void print() {
    forEach("print", BeamStream::print);
  }

  private static <T> void print(T what) {
    if (what instanceof StreamElement) {
      StreamElement el = (StreamElement) what;
      System.out.println(String.format(
          "%s %s %s %d %s", el.getKey(), el.getAttribute(), el.getUuid(),
          el.getStamp(), el.getValue() != null ? el.getParsed().get() : "(null)"));
    } else {
      System.out.println(what);
    }
  }

  @Override
  public List<T> collect() {
    List<T> ret = new ArrayList<>();
    forEach("collect", ret::add);
    return ret;
  }

  @Override
  public boolean isBounded() {
    return bounded;
  }

  @Override
  public void persistIntoTargetReplica(
      RepositoryProvider repoProvider, String replicationName, String target) {

    @SuppressWarnings("unchecked")
    BeamStream<StreamElement> toWrite = descendant(pipeline -> FlatMap
        .named("persistIntoTargetReplica")
        .of((PCollection<StreamElement>) collection.materialize(pipeline))
        .using((StreamElement in, Collector<StreamElement> ctx) -> {
          String key = in.getKey();
          String attribute = in.getAttribute();
          EntityDescriptor entity = in.getEntityDescriptor();
          String replicatedName = String.format(
              "_%s_%s$%s", replicationName, target, attribute);
          Optional<AttributeDescriptor<Object>> attr = entity.findAttribute(
              replicatedName, true);
          if (attr.isPresent()) {
            long stamp = in.getStamp();
            byte[] value = in.getValue();
            ctx.collect(StreamElement.update(
                entity, attr.get(),
                UUID.randomUUID().toString(),
                key, replicatedName, stamp, value));
          } else {
            log.warn("Cannot find attribute {} in {}", replicatedName, entity);
          }
        })
        .output());

    toWrite.write(repoProvider);
  }

  @Override
  public <V> BeamStream<StreamElement> asStreamElements(
      RepositoryProvider repoProvider, EntityDescriptor entity,
      Closure<CharSequence> keyExtractor, Closure<CharSequence> attributeExtractor,
      Closure<V> valueExtractor, Closure<Long> timeExtractor) {

    Repository repo = repoProvider.getRepo();
    Closure<CharSequence> keyDehydrated = dehydrate(keyExtractor);
    Closure<CharSequence> attributeDehydrated = dehydrate(attributeExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<Long> timeDehydrated = dehydrate(timeExtractor);

    return descendant(pipeline -> MapElements
        .named("asStreamElements")
        .of(collection.materialize(pipeline))
        .using(data -> {
          CharSequence key = keyDehydrated.call(data);
          CharSequence attribute = attributeDehydrated.call(data);
          AttributeDescriptor<Object> attrDesc = entity
              .findAttribute(attribute.toString(), true)
              .orElseThrow(() -> new IllegalArgumentException(
                  "No attribute " + attribute + " in " + entity));
          long timestamp = timeDehydrated.call(data);
          byte[] value = attrDesc.getValueSerializer()
              .serialize(valueDehydrated.call(data));
          return StreamElement.update(
              entity, attrDesc, UUID.randomUUID().toString(), key.toString(),
              attribute.toString(), timestamp, value);
        }, TypeDescriptor.of(StreamElement.class))
        .output()
        .setCoder(StreamElementCoder.of(repo)));
  }


  @Override
  public <V> void persist(
      RepositoryProvider repoProvider, EntityDescriptor entity,
      Closure<CharSequence> keyExtractor, Closure<CharSequence> attributeExtractor,
      Closure<V> valueExtractor, Closure<Long> timeExtractor) {

    asStreamElements(
        repoProvider, entity, keyExtractor, attributeExtractor,
        valueExtractor, timeExtractor)
        .write(repoProvider);
  }

  @Override
  public void write(RepositoryProvider repoProvider) {

    Repository repo = repoProvider.getRepo();

    @SuppressWarnings("unchecked")
    BeamStream<StreamElement> elements = (BeamStream<StreamElement>) this;
    elements.forEach("write", el -> {
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<Throwable> err = new AtomicReference<>();
      OnlineAttributeWriter writer = repo.getOrCreateOperator(DirectDataOperator.class)
          .getWriter(el.getAttributeDescriptor())
          .orElseThrow(() -> new IllegalStateException("Missing writer for " + el));

      writer.write(el, (succ, exc) -> {
        latch.countDown();
        err.set(exc);
      });
      try {
        latch.await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
      if (err.get() != null) {
        throw new RuntimeException(err.get());
      }
    }, false);
  }

  @Override
  public WindowedStream<T> timeWindow(long millis) {
    return windowed(
        collection,
        FixedWindows.of(Duration.millis(millis)));
  }

  @Override
  public WindowedStream<T> timeSlidingWindow(long millis, long slide) {
    return windowed(
        collection,
        SlidingWindows.of(Duration.millis(millis)).every(Duration.millis(slide)));
  }

  @Override
  public <K> WindowedStream<Pair<K, T>> sessionWindow(
      Closure<K> keyExtractor, long gapDuration) {

    Closure<K> dehydrated = dehydrate(keyExtractor);

    return windowed(
        pipeline -> {
          Coder<K> coder = coderOf(pipeline, dehydrated);
          PCollection<T> in = collection.materialize(pipeline);
          return MapElements.of(in)
              .using(
                  e -> Pair.of(dehydrated.call(e), e))
              .output()
              .setCoder(PairCoder.of(coder, in.getCoder()));
        },
        Sessions.withGapDuration(Duration.millis(gapDuration)));
  }

  @Override
  public WindowedStream<T> windowAll() {
    return windowed(collection, new GlobalWindows());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Stream<T> union(@Nullable String name, List<Stream<T>> others) {
    return descendant(
        pipeline -> {
          List<PCollection<T>> streams = java.util.stream.Stream.concat(
              java.util.stream.Stream.of(this),
              others.stream())
              .map(s -> ((BeamStream<T>) s).collection.materialize(pipeline))
              .collect(Collectors.toList());
          PCollection<T> any = streams.stream().findAny().orElse(null);
          if (name != null) {
            return PCollectionList.of(streams)
                .apply(name, Flatten.pCollections())
                .setCoder(any.getCoder());
          }
          return PCollectionList.of(streams)
              .apply(Flatten.pCollections())
              .setCoder(any.getCoder());
        });
  }

  @Override
  public <K, V> Stream<Pair<K, V>> integratePerKey(
      @Nullable String name,
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initialValue,
      Closure<V> combiner,
      long allowedLateness) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<V> combinerDehydrated = dehydrate(combiner);
    // always return unwindowed stream
    return new BeamStream<>(this.config, this.bounded, pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
      Coder<V> valueCoder = coderOf(pipeline, valueDehydrated);
      PCollection<KV<K, V>> kvs = MapElements
          .named(withSuffix(name, ".mapToKv"))
          .of(in)
          .using(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e)))
          .output()
          .setCoder(KvCoder.of(keyCoder, valueCoder));
      KvCoder<K, V> coder = (KvCoder<K, V>) kvs.getCoder();
      return kvs.apply(ParDo.of(IntegrateDoFn.of(
          combinerDehydrated, initialValue, coder, allowedLateness)))
          .setCoder(PairCoder.of(keyCoder, valueCoder));
    }, this.terminateCheck, this.pipelineFactory);
  }

  @Override
  public Stream<Pair<T, Long>> withTimestamp(@Nullable String name) {
    return descendant(pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      return applyExtractTimestamp(name, in, pipeline);
    });
  }

  static <T> PCollection<Pair<T, Long>> applyExtractTimestamp(
      @Nullable String name, PCollection<T> in, Pipeline pipeline) {

    final PCollection<Pair<T, Long>> ret;
    if (name != null) {
      ret = in.apply(name, ParDo.of(ExtractTimestamp.of()));
    } else {
      ret = in.apply(ParDo.of(ExtractTimestamp.of()));
    }
    return ret.setCoder(PairCoder.of(in.getCoder(), VarLongCoder.of()));
  }

  @Override
  public <K, S, V, O> Stream<Pair<K, O>> reduceValueStateByKey(
      @Nullable String name, Closure<K> keyExtractor, Closure<V> valueExtractor,
      S initialState, Closure<O> outputFn, Closure<S> stateUpdate,
      long allowedLateness) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<S> stateUpdateDehydrated = dehydrate(stateUpdate);
    Closure<O> outputDehydrated = dehydrate(outputFn);
    return new BeamStream<>(config, bounded, pipeline -> {
      PCollection<T> in = collection.materialize(pipeline);
      Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
      Coder<V> valueCoder = coderOf(pipeline, valueDehydrated);
      Coder<O> outputCoder = coderOf(pipeline, outputDehydrated);
      @SuppressWarnings("unchecked")
      Class<S> stateClass = (Class) initialState.getClass();
      Coder<S> stateCoder = ExceptionUtils.uncheckedFactory(() ->
          pipeline.getCoderRegistry().getCoder(stateClass));
      PCollection<KV<K, V>> kvs = MapElements
          .named(withSuffix(name, ".mapToKvs"))
          .of(in)
          .using(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e)))
          .output()
          .setCoder(KvCoder.of(keyCoder, valueCoder));
      final PCollection<Pair<K, O>> ret;
      if (name != null) {
        ret = kvs.apply(withSuffix(name, ".reduce"),
            ParDo.of(ReduceValueStateByKey.of(
                initialState, stateUpdateDehydrated, outputDehydrated,
                stateCoder, (KvCoder<K, V>) kvs.getCoder(), allowedLateness)));
      } else {
        ret = kvs.apply(
            ParDo.of(ReduceValueStateByKey.of(
                initialState, stateUpdateDehydrated, outputDehydrated,
                stateCoder, (KvCoder<K, V>) kvs.getCoder(), allowedLateness)));
      }
      return ret.setCoder(PairCoder.of(keyCoder, outputCoder));
    }, terminateCheck, pipelineFactory);
  }

  <X> BeamStream<X> descendant(PCollectionProvider<X> provider) {
    return new BeamStream<>(
        config, bounded, provider, terminateCheck, pipelineFactory);
  }

  Pipeline createPipeline() {
    Pipeline ret = pipelineFactory.apply();
    registerCoders(ret.getCoderRegistry());
    return ret;
  }

  static Pipeline createPipelineDefault() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    return Pipeline.create(opts);
  }


  @VisibleForTesting
  <T> Coder<T> coderOf(Pipeline pipeline, Closure<T> closure) {
    return getCoder(pipeline, TypeDescriptor.of(Types.returnClass(closure)));
  }

  static <T> Coder<T> getCoder(Pipeline pipeline, TypeDescriptor<T> type) {
    try {
      return pipeline.getCoderRegistry().getCoder(type);
    } catch (CannotProvideCoderException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  <X> BeamWindowedStream<X> windowed(
      PCollectionProvider<X> provider,
      WindowFn<? super X, ?> window) {

    return new BeamWindowedStream<>(
        config, bounded, provider, window,
        WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES,
        terminateCheck, pipelineFactory);
  }

  @SuppressWarnings("unchecked")
  private void registerCoders(CoderRegistry registry) {
    registry.registerCoderForClass(
        GlobalWindow.class, GlobalWindow.Coder.INSTANCE);
    registrars.forEach(r -> r.accept(registry));
    // FIXME: need to get rid of this fallback
    KryoCoder<Object> coder = KryoCoder.of(
        kryo -> kryo.setInstantiatorStrategy(
            new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy())),
        kryo -> kryo.addDefaultSerializer(Tuple.class, (Class) TupleSerializer.class));
    registry.registerCoderForClass(
        Object.class,
        coder);
    registry.registerCoderForClass(
        Tuple.class,
        TupleCoder.of(coder));
    registry.registerCoderForClass(
        Pair.class, PairCoder.of(coder, coder));
  }

  private <T> RemoteConsumer<T> createRemoteConsumer(
      Coder<T> coder, Consumer<T> consumer) {

    return RemoteConsumer.create(
        this, config.getCollectHostname(),
        config.getPreferredCollectPort(), consumer, coder);
  }

  private static class ConsumeFn<T> extends DoFn<T, Void> {

    private final Consumer<T> consumer;

    ConsumeFn(Consumer<T> consumer) {
      this.consumer = consumer;
    }

    @ProcessElement
    public void process(@Element T elem) {
      consumer.accept(elem);
    }

  }

  private static class ExtractWindow<T> extends DoFn<T, Pair<BoundedWindow, T>> {

    static <T> ExtractWindow<T> of() {
      return new ExtractWindow<>();
    }

    @ProcessElement
    public void process(
        @Element T elem,
        BoundedWindow window,
        OutputReceiver<Pair<BoundedWindow, T>> output) {

      output.output(Pair.of(window, elem));
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeDescriptor<Pair<BoundedWindow, T>> getOutputTypeDescriptor() {
      return PairCoder.descriptor(
          TypeDescriptor.of(BoundedWindow.class),
          (TypeDescriptor) TypeDescriptor.of(Object.class));
    }
  }

  private static class ExtractTimestamp<T> extends DoFn<T, Pair<T, Long>> {

    static <T> ExtractTimestamp<T> of() {
      return new ExtractTimestamp<>();
    }

    @ProcessElement
    public void process(ProcessContext context) {
      context.output(Pair.of(context.element(), context.timestamp().getMillis()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeDescriptor<Pair<T, Long>> getOutputTypeDescriptor() {
      return PairCoder.descriptor(
          (TypeDescriptor) TypeDescriptor.of(Object.class),
          TypeDescriptor.of(Long.class));
    }

  }

  public abstract static class ElementOrderedDoFn<IN, OUT> extends DoFn<IN, OUT> {

    private static final long MIN_STAMP = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();
    private final long allowedLateness;

    ElementOrderedDoFn(long allowedLateness) {
      this.allowedLateness = allowedLateness;
    }

    void onProcessElement(
        ProcessContext context,
        BagState<Pair<Long, IN>> unprocessed,
        ValueState<Long> watermark,
        Timer flushTimer,
        BiConsumer<Long, IN> consumer) {

      IN elem = context.element();
      long timestamp = context.timestamp().getMillis();
      long currentWatermark = MoreObjects.firstNonNull(watermark.read(), MIN_STAMP);
      if (currentWatermark >= timestamp) {
        // drop
        log.debug(
            "Dropping element {}, currentWatermark {}, timestamp {}",
            elem, currentWatermark, timestamp);
      } else {
        unprocessed.add(Pair.of(timestamp, elem));
        unprocessed.readLater();
        // update watermark
        long updatedWatermark = timestamp - allowedLateness;
        if (currentWatermark < updatedWatermark) {
          watermark.write(updatedWatermark);
          watermark.readLater();
          currentWatermark = updatedWatermark;
        }
        consumeElementsAfterWatermark(
            unprocessed, currentWatermark, flushTimer, consumer);
      }
    }

    void onFlushTimer(
        OnTimerContext context,
        BagState<Pair<Long, IN>> unprocessed,
        ValueState<Long> watermark,
        Timer flushTimer,
        BiConsumer<Long, IN> consumer) {

      long stamp = context.timestamp().getMillis();
      long currentWatermark = MoreObjects.firstNonNull(watermark.read(), MIN_STAMP);
      if (currentWatermark < stamp) {
        watermark.write(stamp);
        consumeElementsAfterWatermark(
            unprocessed, stamp, flushTimer, consumer);
      }
    }

    private void consumeElementsAfterWatermark(
        BagState<Pair<Long, IN>> unprocessed,
        long watermark,
        Timer flushTimer,
        BiConsumer<Long, IN> consumer) {

      List<Pair<Long, IN>> keep = new ArrayList<>();
      List<Pair<Long, IN>> output = new ArrayList<>();
      Streams.stream(unprocessed.read()).forEach(e -> {
        if (e.getFirst() <= watermark) {
          output.add(e);
        } else {
          keep.add(e);
        }
      });
      unprocessed.clear();
      keep.forEach(unprocessed::add);
      output.stream()
          .sorted((a, b) -> Long.compare(a.getFirst(), b.getFirst()))
          .forEachOrdered(e -> consumer.accept(e.getFirst(), e.getSecond()));
      if (!keep.isEmpty()) {
        long minStamp = keep.stream()
            .map(Pair::getFirst).min(Long::compare).orElse(-1L);
        flushTimer.set(new Instant(minStamp));
      }
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(Long.MAX_VALUE);
    }

  }

  @VisibleForTesting
  public static class IntegrateDoFn<K, V>
      extends ElementOrderedDoFn<KV<K, V>, Pair<K, V>> {

    static <K, V> DoFn<KV<K, V>, Pair<K, V>> of(
        Closure<V> combiner, V initialValue, KvCoder<K, V> kvCoder,
        long allowedLateness) {

      return new IntegrateDoFn<>(
          (a, b) -> combiner.call(a, b), initialValue, kvCoder, allowedLateness);
    }

    @StateId("unprocessed")
    private final StateSpec<BagState<Pair<Long, KV<K, V>>>> unprocessed;

    @StateId("watermark")
    private final StateSpec<ValueState<Long>> watermark;

    @StateId("combined")
    private final StateSpec<ValueState<V>> stateSpec;

    @TimerId("flush")
    private final TimerSpec flushTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    final BiFunction<V, V, V> combiner;
    final V initialValue;

    IntegrateDoFn(
        BiFunction<V, V, V> combiner, V initialValue, KvCoder<K, V> kvCoder,
        long allowedLateness) {

      super(allowedLateness);
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<V> valueCoder = kvCoder.getValueCoder();
      this.unprocessed = StateSpecs.bag(
          PairCoder.of(VarLongCoder.of(), KvCoder.of(keyCoder, valueCoder)));
      this.watermark = StateSpecs.value(VarLongCoder.of());
      this.stateSpec = StateSpecs.value(valueCoder);
      this.combiner = combiner;
      this.initialValue = initialValue;
    }

    @ProcessElement
    public void process(
        ProcessContext context,
        @StateId("unprocessed") BagState<Pair<Long, KV<K, V>>> unprocessed,
        @StateId("watermark") ValueState<Long> watermark,
        @TimerId("flush") Timer flushTimer,
        @StateId("combined") ValueState<V> state) {

      onProcessElement(context, unprocessed, watermark, flushTimer, (stamp, elem) -> {
        consume(elem, state, context, stamp);
      });
    }

    @OnTimer("flush")
    public void onFlush(
        OnTimerContext context,
        @StateId("unprocessed") BagState<Pair<Long, KV<K, V>>> unprocessed,
        @StateId("watermark") ValueState<Long> watermark,
        @TimerId("flush") Timer flushTimer,
        @StateId("combined") ValueState<V> state) {

      onFlushTimer(context, unprocessed, watermark, flushTimer, (stamp, elem) -> {
        consume(elem, state, context, stamp);
      });
    }

    void consume(
        KV<K, V> in,
        ValueState<V> state,
        WindowedContext context,
        long stamp) {
      V val = MoreObjects.firstNonNull(state.read(), initialValue);
      V result = combiner.apply(in.getValue(), val);
      state.write(result);
      context.outputWithTimestamp(
          Pair.of(in.getKey(), result), new Instant(stamp));
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeDescriptor<Pair<K, V>> getOutputTypeDescriptor() {
      return PairCoder.descriptor(
          (TypeDescriptor) TypeDescriptor.of(Object.class),
          (TypeDescriptor) TypeDescriptor.of(Object.class));
    }

  }

  @VisibleForTesting
  static class ReduceValueStateByKey<K, V, S, O>
      extends ElementOrderedDoFn<KV<K, V>, Pair<K, O>> {

    static <K, V, S, O> ReduceValueStateByKey<K, V, S, O> of(
        S initialState,
        Closure<S> stateUpdate,
        Closure<O> output,
        Coder<S> stateCoder,
        KvCoder<K, V> kvCoder,
        long allowedLateness) {

      return new ReduceValueStateByKey<>(
          initialState, stateUpdate, output, stateCoder,
          kvCoder, allowedLateness);
    }

    private final S initialState;
    private final Closure<S> stateUpdate;
    private final Closure<O> output;

    @StateId("unprocessed")
    private final StateSpec<BagState<Pair<Long, KV<K, V>>>> unprocessed;

    @StateId("watermark")
    private final StateSpec<ValueState<Long>> watermark;

    @TimerId("flush")
    private final TimerSpec flushTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("value")
    private final StateSpec<ValueState<S>> state;

    ReduceValueStateByKey(
        S initialState,
        Closure<S> stateUpdate,
        Closure<O> output,
        Coder<S> stateCoder,
        KvCoder<K, V> kvCoder,
        long allowedLateness) {

      super(allowedLateness);
      this.state = StateSpecs.value(stateCoder);
      this.initialState = initialState;
      this.stateUpdate = stateUpdate;
      this.output = output;

      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<V> valueCoder = kvCoder.getValueCoder();
      this.unprocessed = StateSpecs.bag(
          PairCoder.of(VarLongCoder.of(), KvCoder.of(keyCoder, valueCoder)));
      this.watermark = StateSpecs.value(VarLongCoder.of());

    }

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("unprocessed") BagState<Pair<Long, KV<K, V>>> unprocessed,
        @StateId("watermark") ValueState<Long> watermark,
        @TimerId("flush") Timer flushTimer,
        @StateId("value") ValueState<S> value) {

      onProcessElement(context, unprocessed, watermark, flushTimer, (stamp, elem) -> {
        consume(elem, value, context, stamp);
      });
    }

    @OnTimer("flush")
    public void onFlush(
        OnTimerContext context,
        @StateId("unprocessed") BagState<Pair<Long, KV<K, V>>> unprocessed,
        @StateId("watermark") ValueState<Long> watermark,
        @TimerId("flush") Timer flushTimer,
        @StateId("value") ValueState<S> value) {

      onFlushTimer(context, unprocessed, watermark, flushTimer, (stamp, elem) -> {
        consume(elem, value, context, stamp);
      });
    }

    void consume(
        KV<K, V> in,
        ValueState<S> value,
        WindowedContext context,
        long stamp) {

      S current = MoreObjects.firstNonNull(value.read(), initialState);
      O o = output.call(current, in.getValue());
      S updated = stateUpdate.call(current, in.getValue());
      value.write(updated);
      context.outputWithTimestamp(Pair.of(in.getKey(), o), new Instant(stamp));
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeDescriptor<Pair<K, O>> getOutputTypeDescriptor() {
      return PairCoder.descriptor(
          (TypeDescriptor) TypeDescriptor.of(Object.class),
          (TypeDescriptor) TypeDescriptor.of(Object.class));
    }

  }



  private static <T> DoFn<T, Void> asDoFn(Consumer<T> consumer) {
    return new ConsumeFn<>(consumer);
  }

  private static <T> PTransform<PCollection<T>, PDone> asWriteTransform(DoFn<T, ?> doFn) {
    return new PTransform<PCollection<T>, PDone>() {
      @Override
      public PDone expand(PCollection<T> input) {
        input.apply(ParDo.of(doFn));
        return PDone.in(input.getPipeline());
      }
    };
  }


  private Thread runWatchThread(Runnable terminate) {
    return runThread("pipeline-terminate-check", () -> {
      try {
        while (!terminateCheck.check()) {
          // nop
        }
        terminate.run();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    });
  }

  private Thread runThread(String name, Runnable runnable) {
    Thread ret = new Thread(runnable, name);
    ret.setDaemon(true);
    ret.start();
    return ret;
  }

  private void cancelIfResultExists(AtomicReference<PipelineResult> result) {
    PipelineResult res = result.getAndSet(null);
    if (res != null) {
      try {
        res.cancel();
      } catch (UnsupportedOperationException ex) {
        log.debug("Ignoring UnsupportedOperationException from cancel()");
      } catch (IOException ex) {
        log.warn("Failed to cancel pipeline", ex);
      }
    }
  }

  // iterable that collects elements using HTTP
  // this is first shot implementation with no optimizations
  private static class RemoteConsumer<T> implements Serializable, AutoCloseable {

    private static final Random RANDOM = new Random();

    private static <T> RemoteConsumer<T> create(
        Object seed, String hostname, int preferredPort,
        Consumer<T> consumer, Coder<T> coder) {

      int retries = 3;
      while (retries > 0) {
        retries--;
        int port = getPort(preferredPort, System.identityHashCode(seed));
        try {
          // start HTTP server and store host and port
          RemoteConsumer<T> ret = new RemoteConsumer<>(hostname, port, consumer, coder);
          ret.start();
          return ret;
        } catch (BindException ex) {
          log.debug("Failed to bind on port {}", port, ex);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      throw new RuntimeException("Retries exhausted trying to start server");
    }

    static int getPort(int preferredPort, int seed) {
      return preferredPort > 0
          ? preferredPort
          : RANDOM.nextInt(seed & Integer.MAX_VALUE) % 50000 + 10000;
    }

    private class Server extends NanoHTTPD {

      Server(int port) {
        super(port);
      }

      @Override
      public NanoHTTPD.Response serve(NanoHTTPD.IHTTPSession session) {
        synchronized (RemoteConsumer.this) {
          try {
            consumer.accept(deserialize(session.getInputStream()));
            return newFixedLengthResponse("OK");
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }

    }

    private final Coder<T> coder;
    final URL url;
    private final transient Consumer<T> consumer;
    private final transient Server server;

    private RemoteConsumer(
        String hostname, int port, Consumer<T> consumer,
        Coder<T> coder) throws MalformedURLException {

      this.server = new Server(port);
      this.url = new URL("http://" + hostname + ":" + port);
      this.consumer = consumer;
      this.coder = coder;
    }

    public void add(T what) {
      HttpURLConnection connection = null;
      try {
        connection = (HttpURLConnection) url.openConnection();
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("Connection", "close");
        IOUtils.copy(serialize(what), connection.getOutputStream());
        connection.connect();
        String response = new String(IOUtils.toByteArray(
            connection.getInputStream()), StandardCharsets.US_ASCII);
        if (!"OK".equals(response)) {
          throw new IllegalStateException("Server replied " + response);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
    }

    void stop() {
      server.stop();
    }

    void start() throws IOException {
      server.start(NanoHTTPD.SOCKET_READ_TIMEOUT, true);
    }

    InputStream serialize(T what) throws IOException {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        coder.encode(what, baos);
        return new ByteArrayInputStream(baos.toByteArray());
      }
    }

    T deserialize(InputStream in) throws IOException {
      return coder.decode(in);
    }

    @Override
    public void close() {
      stop();
    }

  }

  <X> Closure<X> dehydrate(Closure<X> closure) {
    if (closure.getOwner() instanceof Serializable) {
      return closure;
    }
    return closure.dehydrate();
  }

  static @Nullable String withSuffix(@Nullable String prefix, String suffix) {
    return prefix == null ? null : prefix + suffix;
  }

}
