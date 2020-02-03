/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import static fi.iki.elonen.NanoHTTPD.newFixedLengthResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.scheme.ValueSerializer;
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
import groovy.lang.Closure;
import groovy.lang.Tuple;
import groovy.transform.stc.ClosureParams;
import groovy.transform.stc.FromString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Kryo;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Serializer;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Input;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Output;
import org.apache.beam.repackaged.kryo.org.objenesis.strategy.StdInstantiatorStrategy;
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
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.codehaus.groovy.runtime.GStringImpl;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A {@link Stream} implementation based on beam. */
@Slf4j
class BeamStream<T> implements Stream<T> {

  @SuppressWarnings("unchecked")
  static <T> BeamStream<T> wrap(PCollection<T> collection) {
    return new BeamStream<>(
        StreamConfig.empty(),
        collection.isBounded() == IsBounded.BOUNDED,
        PCollectionProvider.wrap(collection),
        (WindowingStrategy<Object, ?>) collection.getWindowingStrategy(),
        () -> false,
        collection::getPipeline);
  }

  static <T, S extends BeamStream<T>> S withRegisteredTypes(Repository repo, S in) {

    return in.addRegistrar(
        r -> r.registerCoderForClass(StreamElement.class, StreamElementCoder.of(repo)));
  }

  @SafeVarargs
  static Stream<StreamElement> stream(
      BeamDataOperator beam,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>... attrs) {

    return withRegisteredTypes(
        beam.getRepository(),
        new BeamStream<>(
            asConfig(beam),
            stopAtCurrent,
            PCollectionProvider.fixedType(
                pipeline -> beam.getStream(pipeline, position, stopAtCurrent, eventTime, attrs)),
            WindowingStrategy.globalDefault(),
            terminateCheck,
            pipelineFactory));
  }

  static WindowedStream<StreamElement> batchUpdates(
      BeamDataOperator beam,
      long startStamp,
      long endStamp,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>[] attrs) {

    return withRegisteredTypes(
            beam.getRepository(),
            new BeamStream<>(
                asConfig(beam),
                true,
                PCollectionProvider.boundedOrUnbounded(
                    pipeline -> beam.getBatchUpdates(pipeline, startStamp, endStamp, attrs),
                    pipeline -> beam.getBatchUpdates(pipeline, startStamp, endStamp, true, attrs),
                    true),
                WindowingStrategy.globalDefault(),
                terminateCheck,
                pipelineFactory))
        .windowAll();
  }

  static WindowedStream<StreamElement> batchSnapshot(
      BeamDataOperator beam,
      long fromStamp,
      long toStamp,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>[] attrs) {

    return withRegisteredTypes(
            beam.getRepository(),
            new BeamStream<>(
                asConfig(beam),
                true,
                PCollectionProvider.fixedType(
                    pipeline -> beam.getBatchSnapshot(pipeline, fromStamp, toStamp, attrs)),
                WindowingStrategy.globalDefault(),
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
  final List<RemoteConsumer<?>> remoteConsumers = new ArrayList<>();

  @Getter WindowingStrategy<Object, ?> windowingStrategy;

  BeamStream(
      StreamConfig config,
      boolean bounded,
      PCollectionProvider<T> input,
      WindowingStrategy<Object, ?> windowingStrategy,
      StreamProvider.TerminatePredicate terminateCheck) {

    this(
        config,
        bounded,
        input,
        windowingStrategy,
        terminateCheck,
        BeamStream::createPipelineDefault);
  }

  BeamStream(
      StreamConfig config,
      boolean bounded,
      PCollectionProvider<T> input,
      WindowingStrategy<Object, ?> windowingStrategy,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory) {

    this.config = config;
    this.bounded = bounded;
    this.collection = PCollectionProvider.cached(input);
    this.terminateCheck = terminateCheck;
    this.pipelineFactory = pipelineFactory;
    this.windowingStrategy = windowingStrategy;
  }

  @SuppressWarnings("unchecked")
  <S extends BeamStream<T>> S addRegistrar(Consumer<CoderRegistry> registrar) {
    registrars.add(registrar);
    return (S) this;
  }

  @Override
  public <X> Stream<X> flatMap(@Nullable String name, Closure<Iterable<X>> mapper) {

    Closure<Iterable<X>> dehydrated = dehydrate(mapper);
    return descendant(
        pipeline -> {
          // FIXME: need a way to retrieve inner type of the list
          final Coder<Object> valueCoder = getCoder(pipeline, TypeDescriptor.of(Object.class));
          @SuppressWarnings("unchecked")
          final PCollection<X> ret =
              (PCollection)
                  FlatMap.named(name)
                      .of(collection.materialize(pipeline))
                      .using((elem, ctx) -> dehydrated.call(elem).forEach(ctx::collect))
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
          return MapElements.named(name)
              .of(collection.materialize(pipeline))
              .using(e -> dehydrated.call(e))
              .output()
              .setCoder(coder);
        });
  }

  @Override
  public Stream<T> filter(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<Boolean> predicate) {

    Closure<Boolean> dehydrated = dehydrate(predicate);
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return Filter.named(name).of(in).by(dehydrated::call).output().setCoder(in.getCoder());
        });
  }

  @Override
  public Stream<T> assignEventTime(@Nullable String name, Closure<Long> assigner) {
    Closure<Long> dehydrated = dehydrate(assigner);
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return AssignEventTime.named(name)
              .of(in)
              .using(dehydrated::call)
              .output()
              .setCoder(in.getCoder());
        });
  }

  @Override
  public Stream<Pair<Object, T>> withWindow(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return applyExtractWindow(name, in, pipeline);
        });
  }

  @SuppressWarnings("unchecked")
  static <T> PCollection<Pair<Object, T>> applyExtractWindow(
      @Nullable String name, PCollection<T> in, Pipeline pipeline) {

    TypeDescriptor<Object> windowType =
        (TypeDescriptor) in.getWindowingStrategy().getWindowFn().getWindowTypeDescriptor();
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

  private void forEach(@Nullable String name, Consumer<T> consumer, boolean gatherLocally) {

    Pipeline pipeline = createPipeline();
    PCollection<T> pcoll = collection.materialize(pipeline);
    if (gatherLocally) {
      try (RemoteConsumer<T> remoteConsumer = createRemoteConsumer(pcoll.getCoder(), consumer)) {
        forEachRemote(name, pcoll, remoteConsumer::add, pipeline);
      }
    } else {
      forEachRemote(name, pcoll, consumer, pipeline);
    }
  }

  private void forEachRemote(
      @Nullable String name, PCollection<T> pcoll, Consumer<T> consumer, Pipeline pipeline) {

    if (name != null) {
      pcoll.apply(name, asWriteTransform(asDoFn(consumer)));
    } else {
      pcoll.apply(asWriteTransform(asDoFn(consumer)));
    }
    AtomicReference<PipelineResult> result = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    Thread running =
        runThread(
            "pipeline-start-thread",
            () -> {
              try {
                log.debug(
                    "Running pipeline with class loader {}, pipeline classloader {}",
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
                stopRemoteConsumers();
                latch.countDown();
              }
            });
    AtomicBoolean watchTerminating = new AtomicBoolean();
    AtomicBoolean gracefulExit = new AtomicBoolean();
    Thread watch =
        runWatchThread(
            () -> {
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

  private void stopRemoteConsumers() {
    remoteConsumers.forEach(RemoteConsumer::stop);
    remoteConsumers.clear();
  }

  @Override
  public void print() {
    forEach("print", BeamStream::print);
  }

  private static <T> void print(T what) {
    if (what instanceof StreamElement) {
      StreamElement el = (StreamElement) what;
      Optional<?> parsed = el.getParsed();
      System.out.println(
          String.format(
              "%s %s %s %d %s",
              el.getKey(),
              el.getAttribute(),
              el.getUuid(),
              el.getStamp(),
              parsed.isPresent() ? parsed.get() : "(null)"));
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
  public Stream<T> asUnbounded() {
    collection.asUnbounded();
    return this;
  }

  @Override
  public void persistIntoTargetReplica(
      RepositoryProvider repoProvider, String replicationName, String target) {

    @SuppressWarnings("unchecked")
    BeamStream<StreamElement> toWrite =
        descendant(
            pipeline ->
                FlatMap.named("persistIntoTargetReplica")
                    .of((PCollection<StreamElement>) collection.materialize(pipeline))
                    .using(
                        (StreamElement in, Collector<StreamElement> ctx) -> {
                          String key = in.getKey();
                          String attribute = in.getAttribute();
                          EntityDescriptor entity = in.getEntityDescriptor();
                          String replicatedName =
                              String.format("_%s_%s$%s", replicationName, target, attribute);
                          Optional<AttributeDescriptor<Object>> attr =
                              entity.findAttribute(replicatedName, true);
                          if (attr.isPresent()) {
                            long stamp = in.getStamp();
                            byte[] value = in.getValue();
                            ctx.collect(
                                StreamElement.update(
                                    entity,
                                    attr.get(),
                                    UUID.randomUUID().toString(),
                                    key,
                                    replicatedName,
                                    stamp,
                                    value));
                          } else {
                            log.warn("Cannot find attribute {} in {}", replicatedName, entity);
                          }
                        })
                    .output());

    toWrite.write(repoProvider);
  }

  @Override
  public <V> BeamStream<StreamElement> asStreamElements(
      RepositoryProvider repoProvider,
      EntityDescriptor entity,
      Closure<CharSequence> keyExtractor,
      Closure<CharSequence> attributeExtractor,
      Closure<V> valueExtractor,
      Closure<Long> timeExtractor) {

    RepositoryFactory factory = repoProvider.getRepo().asFactory();
    Closure<CharSequence> keyDehydrated = dehydrate(keyExtractor);
    Closure<CharSequence> attributeDehydrated = dehydrate(attributeExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<Long> timeDehydrated = dehydrate(timeExtractor);

    return descendant(
        pipeline ->
            MapElements.named("asStreamElements")
                .of(collection.materialize(pipeline))
                .using(
                    data -> {
                      CharSequence key = keyDehydrated.call(data);
                      CharSequence attribute = attributeDehydrated.call(data);
                      AttributeDescriptor<Object> attrDesc =
                          entity
                              .findAttribute(attribute.toString(), true)
                              .orElseThrow(
                                  () ->
                                      new IllegalArgumentException(
                                          "No attribute " + attribute + " in " + entity));
                      long timestamp = timeDehydrated.call(data);
                      byte[] value =
                          attrDesc.getValueSerializer().serialize(valueDehydrated.call(data));
                      return StreamElement.update(
                          entity,
                          attrDesc,
                          UUID.randomUUID().toString(),
                          key.toString(),
                          attribute.toString(),
                          timestamp,
                          value);
                    },
                    TypeDescriptor.of(StreamElement.class))
                .output()
                .setCoder(StreamElementCoder.of(factory)));
  }

  @Override
  public <V> void persist(
      RepositoryProvider repoProvider,
      EntityDescriptor entity,
      Closure<CharSequence> keyExtractor,
      Closure<CharSequence> attributeExtractor,
      Closure<V> valueExtractor,
      Closure<Long> timeExtractor) {

    asStreamElements(
            repoProvider, entity, keyExtractor, attributeExtractor, valueExtractor, timeExtractor)
        .write(repoProvider);
  }

  @Override
  public void write(RepositoryProvider repoProvider) {

    RepositoryFactory factory = repoProvider.getRepo().asFactory();

    @SuppressWarnings("unchecked")
    BeamStream<StreamElement> elements = (BeamStream<StreamElement>) this;
    elements.forEach(
        "write",
        el -> {
          CountDownLatch latch = new CountDownLatch(1);
          AtomicReference<Throwable> err = new AtomicReference<>();
          OnlineAttributeWriter writer =
              factory
                  .apply()
                  .getOrCreateOperator(DirectDataOperator.class)
                  .getWriter(el.getAttributeDescriptor())
                  .orElseThrow(() -> new IllegalStateException("Missing writer for " + el));

          writer.write(
              el,
              (succ, exc) -> {
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
        },
        false);
  }

  @Override
  public WindowedStream<T> timeWindow(long millis) {
    return windowed(collection::materialize, FixedWindows.of(Duration.millis(millis)));
  }

  @Override
  public WindowedStream<T> timeSlidingWindow(long millis, long slide) {
    return windowed(
        collection::materialize,
        SlidingWindows.of(Duration.millis(millis)).every(Duration.millis(slide)));
  }

  @Override
  public <K> WindowedStream<Pair<K, T>> sessionWindow(Closure<K> keyExtractor, long gapDuration) {

    Closure<K> dehydrated = dehydrate(keyExtractor);

    return windowed(
        pipeline -> {
          Coder<K> coder = coderOf(pipeline, dehydrated);
          PCollection<T> in = collection.materialize(pipeline);
          return MapElements.of(in)
              .using(e -> Pair.of(dehydrated.call(e), e))
              .output()
              .setCoder(PairCoder.of(coder, in.getCoder()));
        },
        Sessions.withGapDuration(Duration.millis(gapDuration)));
  }

  @Override
  public WindowedStream<T> windowAll() {
    return windowed(collection::materialize, new GlobalWindows());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Stream<T> union(@Nullable String name, List<Stream<T>> others) {
    boolean allBounded = others.stream().allMatch(Stream::isBounded) && isBounded();
    PCollectionProvider<?>[] parents = new PCollectionProvider[others.size() + 1];
    int i = 0;
    for (Stream<T> s : others) {
      parents[i++] = ((BeamStream<T>) s).collection;
    }
    parents[parents.length - 1] = collection;
    if (!allBounded) {
      // turn all inputs to reading in unbouded mode
      // this propagates to sources
      others.stream().forEach(s -> ((BeamStream<T>) s).collection.asUnbounded());
      collection.asUnbounded();
    }
    boolean sameWindows =
        java.util.stream.Stream.concat(java.util.stream.Stream.of(this), others.stream())
                .map(s -> (BeamStream<T>) s)
                .map(s -> Pair.of(s.getWindowFn(), s.getTrigger()))
                .distinct()
                .count()
            == 1;
    return childOf(
        sameWindows ? windowingStrategy : WindowingStrategy.globalDefault(),
        pipeline -> {
          java.util.stream.Stream<BeamStream<T>> streams =
              java.util.stream.Stream.concat(java.util.stream.Stream.of(this), others.stream())
                  .map(s -> (BeamStream<T>) s);
          if (!sameWindows) {
            streams = streams.map(BeamStream::asUnwindowed);
          }
          List<PCollection<T>> collections =
              streams.map(s -> s.collection.materialize(pipeline)).collect(Collectors.toList());
          PCollection<T> any = collections.stream().findAny().orElse(null);
          if (name != null) {
            return PCollectionList.of(collections)
                .apply(name, Flatten.pCollections())
                .setTypeDescriptor(any.getTypeDescriptor())
                .setCoder(any.getCoder());
          }
          return PCollectionList.of(collections)
              .apply(Flatten.pCollections())
              .setTypeDescriptor(any.getTypeDescriptor())
              .setCoder(any.getCoder());
        },
        parents);
  }

  @Override
  public <K, V> Stream<Pair<K, V>> integratePerKey(
      @Nullable String name,
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      Closure<V> initialValue,
      Closure<V> combiner) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<V> combinerDehydrated = dehydrate(combiner);
    Closure<V> initialValueDehydrated = dehydrate(initialValue);
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
          Coder<V> valueCoder = coderOf(pipeline, valueDehydrated);
          if (!in.getWindowingStrategy().equals(windowingStrategy)) {
            @SuppressWarnings("unchecked")
            WindowingStrategy<T, ?> strategy = (WindowingStrategy<T, ?>) windowingStrategy;
            in = in.apply(withWindowingStrategy(strategy));
          }
          PCollection<KV<K, V>> kvs =
              MapElements.named(withSuffix(name, ".mapToKv"))
                  .of(in)
                  .using(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e)))
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          KvCoder<K, V> coder = (KvCoder<K, V>) kvs.getCoder();
          PCollection<Pair<K, V>> ret =
              kvs.apply(
                      ParDo.of(IntegrateDoFn.of(combinerDehydrated, initialValueDehydrated, coder)))
                  .setCoder(PairCoder.of(keyCoder, valueCoder));
          if (!ret.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
            ret = ret.apply(Window.into(new GlobalWindows()));
          }
          return ret;
        });
  }

  @Override
  public Stream<Pair<T, Long>> withTimestamp(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return applyExtractTimestamp(name, in);
        });
  }

  static <T> PCollection<Pair<T, Long>> applyExtractTimestamp(
      @Nullable String name, PCollection<T> in) {

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
      @Nullable String name,
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      Closure<S> initialState,
      Closure<O> outputFn,
      Closure<S> stateUpdate) {

    Closure<K> keyDehydrated = dehydrate(keyExtractor);
    Closure<V> valueDehydrated = dehydrate(valueExtractor);
    Closure<S> stateUpdateDehydrated = dehydrate(stateUpdate);
    Closure<O> outputDehydrated = dehydrate(outputFn);
    Closure<S> initialStateDehydrated = dehydrate(initialState);
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          Coder<K> keyCoder = coderOf(pipeline, keyDehydrated);
          Coder<V> valueCoder = coderOf(pipeline, valueDehydrated);
          Coder<O> outputCoder = coderOf(pipeline, outputDehydrated);
          @SuppressWarnings("unchecked")
          Coder<S> stateCoder = coderOf(pipeline, initialStateDehydrated);
          if (!in.getWindowingStrategy().equals(windowingStrategy)) {
            @SuppressWarnings("unchecked")
            WindowingStrategy<T, ?> strategy = (WindowingStrategy<T, ?>) windowingStrategy;
            in = in.apply(withWindowingStrategy(strategy));
          }
          PCollection<KV<K, V>> kvs =
              MapElements.named(withSuffix(name, ".mapToKvs"))
                  .of(in)
                  .using(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e)))
                  .output()
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          PCollection<Pair<K, O>> ret;
          if (name != null) {
            ret =
                kvs.apply(
                    withSuffix(name, ".reduce"),
                    ParDo.of(
                        ReduceValueStateByKey.of(
                            initialStateDehydrated,
                            stateUpdateDehydrated,
                            outputDehydrated,
                            stateCoder)));
          } else {
            ret =
                kvs.apply(
                    ParDo.of(
                        ReduceValueStateByKey.of(
                            initialStateDehydrated,
                            stateUpdateDehydrated,
                            outputDehydrated,
                            stateCoder)));
          }
          ret = ret.setCoder(PairCoder.of(keyCoder, outputCoder));
          if (!ret.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
            ret = ret.apply(Window.into(new GlobalWindows()));
          }
          return ret;
        });
  }

  static <T, W extends BoundedWindow>
      PTransform<PCollection<T>, PCollection<T>> withWindowingStrategy(
          WindowingStrategy<T, W> strategy) {
    return new PTransform<PCollection<T>, PCollection<T>>() {
      @Override
      public PCollection<T> expand(PCollection<T> input) {
        Window<T> window =
            Window.<T>into(strategy.getWindowFn())
                .withAllowedLateness(strategy.getAllowedLateness(), strategy.getClosingBehavior())
                .withTimestampCombiner(strategy.getTimestampCombiner())
                .withOnTimeBehavior(strategy.getOnTimeBehavior())
                .triggering(strategy.getTrigger());
        if (strategy.getMode() == AccumulationMode.ACCUMULATING_FIRED_PANES) {
          window = window.accumulatingFiredPanes();
        } else if (strategy.getMode() == AccumulationMode.DISCARDING_FIRED_PANES) {
          window = window.discardingFiredPanes();
        } else {
          throw new UnsupportedOperationException(
              "Unsupported accumulation mode " + strategy.getMode());
        }
        return input.apply(window);
      }
    };
  }

  <X> BeamStream<X> descendant(Function<Pipeline, PCollection<X>> factory) {
    return child(factory);
  }

  private <X> BeamStream<X> child(Function<Pipeline, PCollection<X>> factory) {
    return child(factory, windowingStrategy);
  }

  private <X> BeamStream<X> child(
      Function<Pipeline, PCollection<X>> factory, WindowingStrategy<Object, ?> windowingStrategy) {
    return childOf(windowingStrategy, factory, collection);
  }

  private <X> BeamStream<X> childOf(
      WindowingStrategy<Object, ?> windowingStrategy,
      Function<Pipeline, PCollection<X>> factory,
      PCollectionProvider<?>... parents) {

    return new BeamStream<>(
        config,
        bounded,
        PCollectionProvider.withParents(factory, parents),
        windowingStrategy,
        terminateCheck,
        pipelineFactory);
  }

  Pipeline createPipeline() {
    Pipeline ret = pipelineFactory.apply();
    registerCoders(ret.getCoderRegistry(), registrars, config);
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
      Function<Pipeline, PCollection<X>> factory, WindowFn<? super X, ?> window) {

    return new BeamWindowedStream<>(
        config,
        bounded,
        PCollectionProvider.withParents(factory, collection),
        WindowingStrategy.of(window)
            .withMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES)
            .fixDefaults(),
        terminateCheck,
        pipelineFactory);
  }

  WindowFn<Object, ? extends BoundedWindow> getWindowFn() {
    return new GlobalWindows();
  }

  Trigger getTrigger() {
    return DefaultTrigger.of();
  }

  @SuppressWarnings("unchecked")
  private static void registerCoders(
      CoderRegistry registry, List<Consumer<CoderRegistry>> registrars, StreamConfig config) {
    registry.registerCoderForClass(GlobalWindow.class, GlobalWindow.Coder.INSTANCE);
    registry.registerCoderForClass(IntervalWindow.class, IntervalWindowCoder.of());
    registrars.forEach(r -> r.accept(registry));
    // FIXME: need to get rid of this fallback
    KryoCoder<Object> coder =
        KryoCoder.of(
            kryo ->
                kryo.setInstantiatorStrategy(
                    new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy())),
            kryo -> kryo.addDefaultSerializer(Tuple.class, (Class) TupleSerializer.class),
            kryo -> BeamStream.registerCodersForSchemes(kryo, config),
            kryo -> BeamStream.registerCommonTypes(kryo, config),
            kryo -> kryo.setRegistrationRequired(true));
    registry.registerCoderForClass(Object.class, coder);
    registry.registerCoderForClass(Tuple.class, TupleCoder.of(coder));
    registry.registerCoderForClass(Pair.class, PairCoder.of(coder, coder));
  }

  private static void registerCodersForSchemes(Kryo kryo, StreamConfig conf) {
    conf.getRepo()
        .getAllEntities()
        .flatMap(d -> d.getAllAttributes().stream())
        .filter(d -> Objects.nonNull(d.getValueSerializer().getDefault()))
        .distinct()
        .map(d -> Pair.of(d.getValueSerializer().getDefault().getClass(), d.getValueSerializer()))
        .forEach(p -> kryo.register(p.getFirst(), asSerializer(p.getSecond())));
  }

  private static <T> Serializer<T> asSerializer(ValueSerializer<T> serializer) {
    return new Serializer<T>() {

      @Override
      public void write(Kryo kryo, Output output, T t) {
        byte[] bytes = serializer.serialize(t);
        output.writeInt(bytes.length);
        output.write(bytes);
      }

      @Override
      public T read(Kryo kryo, Input input, Class<T> cls) {
        int length = input.readInt();
        byte[] bytes = input.readBytes(length);
        return serializer
            .deserialize(bytes)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Cannot deserialize bytes with class [%s] and serializer [%s]",
                            cls, serializer)));
      }
    };
  }

  private static void registerCommonTypes(Kryo kryo, StreamConfig conf) {
    java.util.stream.Stream<Class<?>> basicClasses =
        java.util.stream.Stream.of(
            StreamElement.class,
            Date.class,
            ArrayList.class,
            GlobalWindow.class,
            IntervalWindow.class,
            Pair.class,
            Instant.class,
            java.time.Instant.class,
            Tuple.class,
            GStringImpl.class,
            String[].class,
            Integer[].class,
            Long[].class,
            Float[].class,
            int[].class,
            long[].class,
            float[].class,
            char[].class,
            Object[].class);

    // register all types of all serializers that provide default values
    java.util.stream.Stream<Class<?>> serializerClasses =
        conf.getRepo()
            .getAllEntities()
            .flatMap(d -> d.getAllAttributes().stream())
            .filter(d -> Objects.nonNull(d.getValueSerializer().getDefault()))
            .flatMap(d -> fieldsRecursively(d.getValueSerializer().getDefault().getClass()))
            .distinct();

    Streams.concat(basicClasses, serializerClasses).distinct().forEach(kryo::register);
  }

  @VisibleForTesting
  static <T> java.util.stream.Stream<Class<?>> fieldsRecursively(T obj) {
    Set<Class<?>> extracted = new HashSet<>();
    extractFieldsRecursivelyInto(obj.getClass(), extracted);
    for (Field f : obj.getClass().getDeclaredFields()) {
      f.setAccessible(true);
      Object fieldVal = ExceptionUtils.uncheckedFactory(() -> f.get(obj));
      if (fieldVal != null && !extracted.contains(fieldVal)) {
        extractFieldsRecursivelyInto(fieldVal.getClass(), extracted);
      }
    }
    return extracted.stream();
  }

  private static void extractFieldsRecursivelyInto(Class<?> cls, Set<Class<?>> extracted) {
    extracted.add(cls);
    Arrays.stream(cls.getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .map(Field::getType)
        .filter(f -> !extracted.contains(f))
        .forEach(t -> extractFieldsRecursivelyInto(t, extracted));
    Arrays.stream(cls.getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .map(Field::getType)
        .forEach(extracted::add);
    if (cls.getSuperclass() != null
        && cls.getSuperclass() != Object.class
        && !extracted.contains(cls.getSuperclass())) {
      extractFieldsRecursivelyInto(cls.getSuperclass(), extracted);
    }
  }

  private <T> RemoteConsumer<T> createRemoteConsumer(Coder<T> coder, Consumer<T> consumer) {

    RemoteConsumer<T> ret =
        RemoteConsumer.create(
            this, config.getCollectHostname(), config.getCollectPort(), consumer, coder);
    remoteConsumers.add(ret);
    return ret;
  }

  private BeamStream<T> asUnwindowed() {
    if (getWindowFn().equals(new GlobalWindows()) && getTrigger().equals(DefaultTrigger.of())) {
      return new BeamStream<>(
          this.config,
          this.bounded,
          this.collection,
          this.windowingStrategy,
          this.terminateCheck,
          this.pipelineFactory);
    } else {
      return child(
          pipeline -> {
            PCollection<T> in = this.collection.materialize(pipeline);
            return in.apply(
                    Window.<T>into(new GlobalWindows())
                        .triggering(DefaultTrigger.of())
                        .discardingFiredPanes())
                .setCoder(in.getCoder());
          });
    }
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
        @Element T elem, BoundedWindow window, OutputReceiver<Pair<BoundedWindow, T>> output) {

      output.output(Pair.of(window, elem));
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeDescriptor<Pair<BoundedWindow, T>> getOutputTypeDescriptor() {
      return PairCoder.descriptor(
          TypeDescriptor.of(BoundedWindow.class), (TypeDescriptor) TypeDescriptor.of(Object.class));
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
          (TypeDescriptor) TypeDescriptor.of(Object.class), TypeDescriptor.of(Long.class));
    }
  }

  @VisibleForTesting
  public static class IntegrateDoFn<K, V> extends DoFn<KV<K, V>, Pair<K, V>> {

    static <K, V> DoFn<KV<K, V>, Pair<K, V>> of(
        Closure<V> combiner, Closure<V> initialValue, KvCoder<K, V> kvCoder) {

      return new IntegrateDoFn<>(combiner::call, initialValue::call, kvCoder);
    }

    @StateId("combined")
    private final StateSpec<ValueState<V>> stateSpec;

    private final BiFunction<V, V, V> combiner;
    private final UnaryFunction<K, V> initialValue;

    IntegrateDoFn(
        BiFunction<V, V, V> combiner, UnaryFunction<K, V> initialValue, KvCoder<K, V> kvCoder) {

      this.stateSpec = StateSpecs.value(kvCoder.getValueCoder());
      this.combiner = combiner;
      this.initialValue = initialValue;
    }

    @RequiresTimeSortedInput
    @ProcessElement
    public void process(ProcessContext context, @StateId("combined") ValueState<V> state) {

      KV<K, V> element = context.element();
      K key = element.getKey();
      V value = element.getValue();
      V current = state.read();
      if (current == null) {
        current = Objects.requireNonNull(initialValue.apply(key));
      }
      V result = combiner.apply(value, current);
      state.write(result);
      context.outputWithTimestamp(Pair.of(key, result), context.timestamp());
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
  static class ReduceValueStateByKey<K, V, S, O> extends DoFn<KV<K, V>, Pair<K, O>> {

    static <K, V, S, O> ReduceValueStateByKey<K, V, S, O> of(
        Closure<S> initialState, Closure<S> stateUpdate, Closure<O> output, Coder<S> stateCoder) {

      return new ReduceValueStateByKey<>(initialState, stateUpdate, output, stateCoder);
    }

    private final Closure<S> initialState;
    private final Closure<S> stateUpdate;
    private final Closure<O> output;

    @StateId("value")
    private final StateSpec<ValueState<S>> state;

    ReduceValueStateByKey(
        Closure<S> initialState, Closure<S> stateUpdate, Closure<O> output, Coder<S> stateCoder) {

      this.state = StateSpecs.value(stateCoder);
      this.initialState = initialState;
      this.stateUpdate = stateUpdate;
      this.output = output;
    }

    @RequiresTimeSortedInput
    @ProcessElement
    public void processElement(ProcessContext context, @StateId("value") ValueState<S> valueState) {

      KV<K, V> element = context.element();
      K key = element.getKey();
      V value = element.getValue();
      S current = valueState.read();
      if (current == null) {
        current = Objects.requireNonNull(initialState.call(key));
      }
      O o = output.call(current, value);
      S updated = stateUpdate.call(current, value);
      valueState.write(updated);
      context.outputWithTimestamp(Pair.of(key, o), context.timestamp());
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
    return runThread(
        "pipeline-terminate-check",
        () -> {
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

    private static <T> RemoteConsumer<T> create(
        Object seed, String hostname, int preferredPort, Consumer<T> consumer, Coder<T> coder) {

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
          : ((ThreadLocalRandom.current().nextInt() ^ seed) & Integer.MAX_VALUE) % 50000 + 10000;
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

    private RemoteConsumer(String hostname, int port, Consumer<T> consumer, Coder<T> coder)
        throws MalformedURLException {

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
        String response =
            new String(IOUtils.toByteArray(connection.getInputStream()), StandardCharsets.US_ASCII);
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
