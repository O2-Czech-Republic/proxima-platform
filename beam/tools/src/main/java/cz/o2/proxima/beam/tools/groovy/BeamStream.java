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

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.beam.core.transforms.AssignEventTime;
import cz.o2.proxima.beam.core.transforms.CalendarWindows;
import cz.o2.proxima.core.functional.BiFunction;
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeDescriptorBase;
import cz.o2.proxima.core.repository.AttributeDescriptorImpl;
import cz.o2.proxima.core.repository.AttributeProxyDescriptor;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptorImpl;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.RepositoryFactory;
import cz.o2.proxima.core.scheme.ValueSerializer;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.core.util.SerializableScopedValue;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.internal.com.google.common.collect.Streams;
import cz.o2.proxima.tools.groovy.RepositoryProvider;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamProvider.TerminatePredicate;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.tools.groovy.util.Types;
import groovy.lang.Closure;
import groovy.lang.Tuple;
import groovy.transform.stc.ClosureParams;
import groovy.transform.stc.FromString;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Kryo;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Serializer;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Input;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Output;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer;
import org.apache.beam.repackaged.kryo.org.objenesis.strategy.StdInstantiatorStrategy;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampTransform;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.codehaus.groovy.runtime.GStringImpl;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A {@link Stream} implementation based on beam. */
@Slf4j
class BeamStream<T> implements Stream<T> {

  /**
   * Create a {@link Stream} from given {@link PCollection} and {@link Repository}
   *
   * @return {@link PCollection} wrapped as {@link Stream}.
   */
  static <T> BeamStream<T> wrap(Repository repo, PCollection<T> collection) {
    return wrapPCollection(
        StreamConfig.of(repo.getOrCreateOperator(BeamDataOperator.class)), collection);
  }

  /**
   * Create a {@link Stream} from given {@link PCollection}.
   *
   * @return {@link PCollection} wrapped as {@link Stream}.
   */
  static <T> BeamStream<T> wrap(PCollection<T> collection) {
    return wrapPCollection(StreamConfig.empty(), collection);
  }

  @SuppressWarnings("unchecked")
  private static <T> BeamStream<T> wrapPCollection(StreamConfig config, PCollection<T> collection) {
    return new BeamStream<>(
        config,
        collection.isBounded() == IsBounded.BOUNDED,
        PCollectionProvider.wrap(collection),
        (WindowingStrategy<Object, ?>) collection.getWindowingStrategy(),
        () -> false,
        collection::getPipeline) {};
  }

  @SafeVarargs
  static Stream<StreamElement> stream(
      BeamDataOperator beam,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>... attrs) {

    return new BeamStream<>(
        asConfig(beam),
        stopAtCurrent,
        PCollectionProvider.fixedType(
            pipeline -> beam.getStream(pipeline, position, stopAtCurrent, eventTime, attrs)),
        WindowingStrategy.globalDefault(),
        terminateCheck,
        pipelineFactory) {};
  }

  static WindowedStream<StreamElement> batchUpdates(
      BeamDataOperator beam,
      long startStamp,
      long endStamp,
      TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>[] attrs) {

    return new BeamStream<>(
        asConfig(beam),
        true,
        PCollectionProvider.boundedOrUnbounded(
            pipeline -> beam.getBatchUpdates(pipeline, startStamp, endStamp, attrs),
            pipeline -> beam.getBatchUpdates(pipeline, startStamp, endStamp, true, attrs),
            true),
        WindowingStrategy.globalDefault(),
        terminateCheck,
        pipelineFactory) {}.windowAll();
  }

  static WindowedStream<StreamElement> batchSnapshot(
      BeamDataOperator beam,
      long fromStamp,
      long toStamp,
      TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory,
      AttributeDescriptor<?>[] attrs) {

    return new BeamStream<>(
        asConfig(beam),
        true,
        PCollectionProvider.fixedType(
            pipeline -> beam.getBatchSnapshot(pipeline, fromStamp, toStamp, attrs)),
        WindowingStrategy.globalDefault(),
        terminateCheck,
        pipelineFactory) {}.windowAll();
  }

  @SuppressWarnings("unchecked")
  static <T> WindowedStream<T> impulse(
      @Nullable String name,
      BeamDataOperator beam,
      Factory<Pipeline> pipelineFactory,
      Factory<T> factory) {

    return new BeamWindowedStream<>(
        asConfig(beam),
        true,
        PCollectionProvider.fixedType(
            p ->
                (name == null ? p.apply(Impulse.create()) : p.apply(name, Impulse.create()))
                    .apply(
                        org.apache.beam.sdk.transforms.MapElements.into(
                                (TypeDescriptor<T>) TypeDescriptor.of(Object.class))
                            .via(e -> factory.apply()))),
        WindowingStrategy.globalDefault(),
        () -> false,
        pipelineFactory) {}.windowAll();
  }

  static <T> WindowedStream<T> periodicImpulse(
      @Nullable String name,
      BeamDataOperator beam,
      Factory<Pipeline> pipelineFactory,
      Factory<T> factory,
      long durationMs) {

    return periodicImpulse(name, beam, pipelineFactory, factory, durationMs, () -> false);
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  static <T> WindowedStream<T> periodicImpulse(
      @Nullable String name,
      BeamDataOperator beam,
      Factory<Pipeline> pipelineFactory,
      Factory<T> factory,
      long durationMs,
      TerminatePredicate terminatePredicate) {

    final GenerateSequence transform =
        GenerateSequence.from(0).withRate(1, Duration.millis(durationMs));
    return new BeamWindowedStream<>(
        asConfig(beam),
        false,
        PCollectionProvider.fixedType(
            p ->
                (name == null ? p.apply(transform) : p.apply(name, transform))
                    .apply(
                        org.apache.beam.sdk.transforms.MapElements.into(
                                (TypeDescriptor<T>) TypeDescriptor.of(Object.class))
                            .via(e -> factory.apply()))),
        WindowingStrategy.globalDefault(),
        terminatePredicate,
        pipelineFactory) {}.windowAll();
  }

  private static StreamConfig asConfig(BeamDataOperator beam) {
    return StreamConfig.of(beam);
  }

  final StreamConfig config;
  final boolean bounded;
  final PCollectionProvider<T> collection;
  final TerminatePredicate terminateCheck;
  final Factory<Pipeline> pipelineFactory;

  private final List<RemoteConsumer<?>> remoteConsumers = new ArrayList<>();

  @Getter WindowingStrategy<Object, ?> windowingStrategy;

  BeamStream(
      StreamConfig config,
      boolean bounded,
      PCollectionProvider<T> input,
      WindowingStrategy<Object, ?> windowingStrategy,
      TerminatePredicate terminateCheck) {

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
      TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory) {

    this.config = config;
    this.bounded = bounded;
    this.collection = PCollectionProvider.cached(input);
    this.terminateCheck = terminateCheck;
    this.pipelineFactory = pipelineFactory;
    this.windowingStrategy = windowingStrategy;
  }

  PCollectionProvider<T> getCollection() {
    return collection;
  }

  TerminatePredicate getTerminateCheck() {
    return terminateCheck;
  }

  Factory<Pipeline> getPipelineFactory() {
    return pipelineFactory;
  }

  @Override
  public <X> Stream<X> flatMap(@Nullable String name, Closure<Iterable<X>> mapper) {

    Closure<Iterable<X>> dehydrated = dehydrate(mapper);
    return descendant(
        pipeline -> {
          // FIXME: need a way to retrieve inner type of the list
          @SuppressWarnings({"unchecked", "rawtypes"})
          final Coder<X> valueCoder = (Coder) getCoder(pipeline, TypeDescriptor.of(Object.class));
          TypeDescriptor<X> valueType = valueCoder.getEncodedTypeDescriptor();
          return collection
              .materialize(pipeline)
              .apply(FlatMapElements.into(valueType).via(dehydrated::call))
              .setCoder(valueCoder);
        });
  }

  @Override
  public <X> Stream<X> map(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<X> mapper) {

    Closure<X> dehydrated = dehydrate(mapper);
    return descendant(
        pipeline -> {
          Coder<X> coder = coderOf(pipeline, dehydrated);
          TypeDescriptor<X> type = coder.getEncodedTypeDescriptor();
          return collection
              .materialize(pipeline)
              .apply(MapElements.into(type).via(dehydrated::call))
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
          return in.apply(Filter.by(dehydrated::call));
        });
  }

  @Override
  public Stream<T> assignEventTime(@Nullable String name, Closure<Long> assigner) {
    Closure<Long> dehydrated = dehydrate(assigner);
    SerializableFunction<T, Instant> timestampFn = in -> Instant.ofEpochMilli(dehydrated.call(in));
    return descendant(
        pipeline ->
            collection.materialize(pipeline).apply(AssignEventTime.forTimestampFn(timestampFn)));
  }

  @Override
  public Stream<Pair<Object, T>> withWindow(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return applyExtractWindow(name, in, pipeline);
        });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
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
    return ret.setCoder(PairCoder.of(windowCoder, in.getCoder()));
  }

  private void forEach(@Nullable String name, Consumer<T> consumer) {
    forEach(name, consumer, true);
  }

  private void forEach(@Nullable String name, Consumer<T> consumer, boolean gatherLocally) {
    Pipeline pipeline = createPipeline();
    PCollection<T> pcoll = collection.materialize(pipeline);
    if (gatherLocally) {
      try (RemoteConsumer<T> remoteConsumer = createRemoteConsumer(pcoll.getCoder(), consumer)) {
        forEachRemote(name, pcoll, remoteConsumer, true, pipeline);
      }
    } else {
      forEachRemote(name, pcoll, consumer, false, pipeline);
    }
  }

  private void forEachRemote(
      @Nullable String name,
      PCollection<T> pcoll,
      Consumer<T> consumer,
      boolean allowStable,
      Pipeline pipeline) {

    if (name != null) {
      pcoll.apply(
          name, asWriteTransform(asDoFn(pcoll.getPipeline().getOptions(), allowStable, consumer)));
    } else {
      pcoll.apply(
          asWriteTransform(asDoFn(pcoll.getPipeline().getOptions(), allowStable, consumer)));
    }
    runPipeline(pipeline);
  }

  private void runPipeline(Pipeline pipeline) {
    AtomicReference<PipelineResult> result = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> errorCaught = new AtomicReference<>();
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
              } catch (Throwable ex) {
                if (!(ex.getCause() instanceof InterruptedException)) {
                  errorCaught.set(ex);
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
    Optional.ofNullable(errorCaught.getAndSet(null)).ifPresent(BeamStream::rethrow);
    Preconditions.checkState(
        Optional.ofNullable(result.get()).map(res -> res.getState() == State.DONE).orElse(true),
        "Expected the Pipeline to be in DONE state after finishing, got %s",
        result.get());
  }

  @VisibleForTesting
  static void rethrow(Throwable err) {
    log.error("Pipeline failed", err);
    if (err instanceof Error) {
      throw (Error) err;
    }
    if (err instanceof RuntimeException) {
      throw (RuntimeException) err;
    }
    throw new IllegalStateException("Failed to execute pipeline", err);
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
      System.out.printf(
          "%s %s %s %d %s%n",
          el.getKey(),
          el.getAttribute(),
          el.getUuid(),
          el.getStamp(),
          parsed.isPresent() ? parsed.get() : "(null)");
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
                ((PCollection<StreamElement>) collection.materialize(pipeline))
                    .apply(
                        "persistIntoTargetReplica",
                        FlatMapElements.into(TypeDescriptor.of(StreamElement.class))
                            .via(
                                in -> {
                                  String key = in.getKey();
                                  String attribute = in.getAttribute();
                                  EntityDescriptor entity = in.getEntityDescriptor();
                                  String replicatedName =
                                      String.format(
                                          "_%s_%s$%s", replicationName, target, attribute);
                                  Optional<AttributeDescriptor<Object>> attr =
                                      entity.findAttribute(replicatedName, true);
                                  if (attr.isPresent()) {
                                    long stamp = in.getStamp();
                                    byte[] value = in.getValue();
                                    return Collections.singletonList(
                                        StreamElement.upsert(
                                            entity,
                                            attr.get(),
                                            UUID.randomUUID().toString(),
                                            key,
                                            replicatedName,
                                            stamp,
                                            value));
                                  }
                                  log.warn(
                                      "Cannot find attribute {} in {}", replicatedName, entity);
                                  return Collections.emptyList();
                                })));

    toWrite.write(repoProvider);
  }

  @Override
  public void persistIntoTargetFamily(
      RepositoryProvider repoProvider, String targetFamilyname, int parallelism) {
    DirectAttributeFamilyDescriptor familyDescriptor =
        repoProvider
            .getDirect()
            .getAllFamilies()
            .filter(af -> af.getDesc().getName().equals(targetFamilyname))
            .findAny()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Family [%s] does not exist", targetFamilyname)));
    Preconditions.checkArgument(!familyDescriptor.getDesc().getAccess().isReadonly());
    AttributeWriterBase rawWriter =
        familyDescriptor
            .getWriter()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Family [%s] does not have writer", targetFamilyname)));
    RepositoryFactory repositoryFactory = repoProvider.getRepo().asFactory();
    AttributeWriterBase.Factory<?> writerFactory = rawWriter.asFactory();
    SerializableScopedValue<Integer, AttributeWriterBase> writer =
        new SerializableScopedValue<>(() -> writerFactory.apply(repositoryFactory.apply()));
    Set<String> allowedAttributes =
        familyDescriptor.getAttributes().stream()
            .map(AttributeDescriptor::getName)
            .collect(Collectors.toSet());
    switch (rawWriter.getType()) {
      case ONLINE:
        writeUsingOnlineWriterFactory(
            "write-to-" + targetFamilyname,
            el -> {
              Preconditions.checkArgument(
                  el == null || allowedAttributes.contains(el.getAttributeDescriptor().getName()));
              return writer.get(0).online();
            });
        break;
      case BULK:
        writeUsingBulkWriterFactory(
            "write-bulk-to-" + targetFamilyname,
            parallelism,
            BulkWriterFactory.wrap(writer, allowedAttributes));
        break;
      default:
        throw new IllegalArgumentException("Unknonw type " + rawWriter.getType());
    }
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
            collection
                .materialize(pipeline)
                .apply(
                    "asStreamElements",
                    MapElements.into(TypeDescriptor.of(StreamElement.class))
                        .via(
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
                                  attrDesc
                                      .getValueSerializer()
                                      .serialize(valueDehydrated.call(data));
                              return StreamElement.upsert(
                                  entity,
                                  attrDesc,
                                  UUID.randomUUID().toString(),
                                  key.toString(),
                                  attribute.toString(),
                                  timestamp,
                                  value);
                            }))
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

    writeUsingOnlineWriterFactory(
        "write",
        el ->
            factory
                .apply()
                .getOrCreateOperator(DirectDataOperator.class)
                .getWriter(el.getAttributeDescriptor())
                .orElseThrow(() -> new IllegalStateException("Missing writer for " + el)));
  }

  void writeUsingOnlineWriterFactory(
      String name, UnaryFunction<StreamElement, OnlineAttributeWriter> writerFactory) {

    @SuppressWarnings("unchecked")
    BeamStream<StreamElement> elements = (BeamStream<StreamElement>) this;
    elements.forEach(
        name,
        el -> {
          while (true) {
            OnlineAttributeWriter writer = writerFactory.apply(el);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Throwable> err = new AtomicReference<>();
            writer
                .online()
                .write(
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
              if (err.get() instanceof TransactionRejectedException) {
                log.debug("Caught TransactionRejectedException, retrying write", err.get());
              } else {
                throw new RuntimeException(err.get());
              }
            } else {
              break;
            }
          }
        },
        false);
  }

  private void writeUsingBulkWriterFactory(
      String name, int parallelism, BulkWriterFactory factory) {

    Pipeline pipeline = createPipeline();
    @SuppressWarnings("unchecked")
    PCollection<StreamElement> elements =
        (PCollection<StreamElement>) windowAll().getCollection().materialize(pipeline);
    Preconditions.checkArgument(
        elements.isBounded() == IsBounded.BOUNDED,
        "Persisting into bulk families is currently supported in batch mode only.");
    elements.apply(name, createBulkWriteTransform(parallelism, factory));
    runPipeline(pipeline);
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
          TypeDescriptor<Pair<K, T>> type = new TypeDescriptor<>() {};
          return in.apply(MapElements.into(type).via(e -> Pair.of(dehydrated.call(e), e)))
              .setCoder(PairCoder.of(coder, in.getCoder()));
        },
        Sessions.withGapDuration(Duration.millis(gapDuration)));
  }

  @Override
  public WindowedStream<T> calendarWindow(String window, int count, TimeZone timeZone) {
    WindowFn<? super T, ?> windowFn;
    int multiplier = 1;
    switch (window) {
      case "weeks":
        multiplier = 7;
      case "days":
        windowFn = CalendarWindows.days(count * multiplier).withTimeZone(timeZone);
        break;
      case "months":
        windowFn = CalendarWindows.months(count).withTimeZone(timeZone);
        break;
      case "years":
        windowFn = CalendarWindows.years(count).withTimeZone(timeZone);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unknown window %s, supported are 'days', 'weeks', 'months' and 'years'", window));
    }
    return windowed(collection::materialize, windowFn);
  }

  @Override
  public BeamWindowedStream<T> windowAll() {
    return windowed(collection::materialize, new GlobalWindows());
  }

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
      others.forEach(s -> ((BeamStream<T>) s).collection.asUnbounded());
      collection.asUnbounded();
    }
    boolean sameWindows =
        java.util.stream.Stream.concat(java.util.stream.Stream.of(this), others.stream())
                .map(s -> (BeamStream<T>) s)
                .map(BeamStream::getWindowingStrategy)
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
                .setCoder(any.getCoder());
          }
          return PCollectionList.of(collections)
              .apply(Flatten.pCollections())
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
          TypeDescriptor<K> keyType = keyCoder.getEncodedTypeDescriptor();
          TypeDescriptor<V> valueType = valueCoder.getEncodedTypeDescriptor();
          if (!in.getWindowingStrategy().equals(windowingStrategy)) {
            @SuppressWarnings("unchecked")
            WindowingStrategy<T, ?> strategy = (WindowingStrategy<T, ?>) windowingStrategy;
            in = in.apply(withWindowingStrategy(strategy));
          }
          PCollection<KV<K, V>> kvs =
              applyNamedTransform(
                      name,
                      ".mapToKv",
                      in,
                      MapElements.into(TypeDescriptors.kvs(keyType, valueType))
                          .via(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e))))
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
      Closure<S> stateUpdate,
      boolean sorted) {

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
          Coder<S> stateCoder = coderOf(pipeline, initialStateDehydrated);
          TypeDescriptor<K> keyType = keyCoder.getEncodedTypeDescriptor();
          TypeDescriptor<V> valueType = valueCoder.getEncodedTypeDescriptor();
          if (!in.getWindowingStrategy().equals(windowingStrategy)) {
            @SuppressWarnings("unchecked")
            WindowingStrategy<T, ?> strategy = (WindowingStrategy<T, ?>) windowingStrategy;
            in = in.apply(withWindowingStrategy(strategy));
          }
          Duration earlyEmitting = extractEarlyEmitting(windowingStrategy.getTrigger());
          PCollection<KV<K, V>> kvs =
              applyNamedTransform(
                      name,
                      ".mapToKvs",
                      in,
                      MapElements.into(TypeDescriptors.kvs(keyType, valueType))
                          .via(e -> KV.of(keyDehydrated.call(e), valueDehydrated.call(e))))
                  .setCoder(KvCoder.of(keyCoder, valueCoder));
          PCollection<Pair<K, O>> ret;
          if (name != null) {
            ret =
                applyNamedTransform(
                    name,
                    ".reduce",
                    kvs,
                    ParDo.of(
                        AbstractReduceValueStateByKey.of(
                            initialStateDehydrated,
                            stateUpdateDehydrated,
                            outputDehydrated,
                            keyCoder,
                            stateCoder,
                            earlyEmitting,
                            sorted)));
          } else {
            ret =
                kvs.apply(
                    ParDo.of(
                        AbstractReduceValueStateByKey.of(
                            initialStateDehydrated,
                            stateUpdateDehydrated,
                            outputDehydrated,
                            keyCoder,
                            stateCoder,
                            earlyEmitting,
                            sorted)));
          }
          ret = ret.setCoder(PairCoder.of(keyCoder, outputCoder));
          if (!ret.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
            ret = ret.apply(Window.into(new GlobalWindows()));
          }
          return ret;
        });
  }

  @Override
  public Stream<T> reshuffle(@Nullable String name) {
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return in.apply(Reshuffle.viaRandomKey());
        });
  }

  @VisibleForTesting
  static Duration extractEarlyEmitting(Trigger trigger) {
    Duration extracted = null;
    List<Trigger> subTriggers =
        MoreObjects.firstNonNull(trigger.subTriggers(), Collections.emptyList());
    for (Trigger t : subTriggers) {
      extracted = tryExtractFromTrigger(t);
      if (extracted != null) {
        break;
      }
    }
    if (extracted == null) {
      extracted = tryExtractFromTrigger(trigger);
    }
    return MoreObjects.firstNonNull(extracted, Duration.ZERO);
  }

  @Nullable
  private static Duration tryExtractFromTrigger(Trigger t) {
    if (t instanceof AfterProcessingTime) {
      AfterProcessingTime processing = (AfterProcessingTime) t;
      List<TimestampTransform> transforms = processing.getTimestampTransforms();
      if (transforms.size() == 1) {
        TimestampTransform transform = Iterables.getOnlyElement(transforms);
        if (transform instanceof TimestampTransform.Delay) {
          return ((TimestampTransform.Delay) transform).getDelay();
        }
      }
    }
    return null;
  }

  static <T, W extends BoundedWindow>
      PTransform<PCollection<T>, PCollection<T>> withWindowingStrategy(
          WindowingStrategy<T, W> strategy) {
    return new PTransform<>() {
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

    return new BeamStream<X>(
        config,
        bounded,
        PCollectionProvider.withParents(factory, parents),
        windowingStrategy,
        terminateCheck,
        pipelineFactory) {};
  }

  Pipeline createPipeline() {
    Pipeline ret = pipelineFactory.apply();
    registerCoders(ret.getCoderRegistry(), config.getRepo());
    return ret;
  }

  static Pipeline createPipelineDefault() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    return Pipeline.create(opts);
  }

  <X> BeamWindowedStream<X> windowed(
      Function<Pipeline, PCollection<X>> factory, WindowFn<? super X, ?> window) {

    @SuppressWarnings({"unchecked", "rawtypes"})
    WindowingStrategy<Object, ?> strategy =
        (WindowingStrategy)
            WindowingStrategy.of(window)
                .withMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES);

    return new BeamWindowedStream<X>(
        config,
        bounded,
        PCollectionProvider.withParents(factory, collection),
        strategy,
        terminateCheck,
        pipelineFactory) {};
  }

  WindowFn<Object, ? extends BoundedWindow> getWindowFn() {
    return windowingStrategy.getWindowFn();
  }

  Trigger getTrigger() {
    return windowingStrategy.getTrigger();
  }

  @SuppressWarnings("unchecked")
  private static void registerCoders(CoderRegistry registry, Repository repo) {
    // FIXME: need to get rid of this fallback
    KryoCoder<Object> coder =
        KryoCoder.of(
            kryo ->
                kryo.setInstantiatorStrategy(
                    new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy())),
            kryo ->
                kryo.addDefaultSerializer(UnboundedSource.class, KryoSerializableSerializer.class),
            kryo ->
                kryo.addDefaultSerializer(BoundedSource.class, KryoSerializableSerializer.class),
            BeamStream::addDefaultSerializersForGroovyTypes,
            kryo -> BeamStream.registerCodersForSchemes(kryo, repo),
            kryo -> BeamStream.registerCommonTypes(kryo, repo),
            kryo -> kryo.setRegistrationRequired(true));
    registry.registerCoderForClass(
        UnboundedSource.class, SerializableCoder.of(UnboundedSource.class));
    registry.registerCoderForClass(BoundedSource.class, SerializableCoder.of(BoundedSource.class));
    registry.registerCoderForClass(Object.class, coder);
    registry.registerCoderForClass(Tuple.class, TupleCoder.of(coder));
    registry.registerCoderForClass(Pair.class, PairCoder.of(coder, coder));
  }

  private static void addDefaultSerializersForGroovyTypes(Kryo kryo) {
    kryo.addDefaultSerializer(Tuple.class, TupleSerializer.class);
    kryo.addDefaultSerializer(GStringImpl.class, GStringSerializer.class);
  }

  private static void registerCodersForSchemes(Kryo kryo, Repository repo) {
    repo.getAllEntities()
        .flatMap(d -> d.getAllAttributes().stream())
        .filter(d -> Objects.nonNull(d.getValueSerializer().getDefault()))
        .distinct()
        .map(d -> Pair.of(d.getValueSerializer().getDefault().getClass(), d.getValueSerializer()))
        .forEach(p -> kryo.register(p.getFirst(), asSerializer(p.getSecond())));
  }

  private static <T> Serializer<T> asSerializer(ValueSerializer<T> serializer) {
    return new Serializer<>() {

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

  private static void registerCommonTypes(Kryo kryo, Repository repo) {
    java.util.stream.Stream<Class<?>> basicClasses =
        java.util.stream.Stream.of(
            StreamElement.class,
            AttributeDescriptor.class,
            AttributeDescriptorBase.class,
            AttributeDescriptorImpl.class,
            AttributeProxyDescriptor.class,
            EntityAwareAttributeDescriptor.class,
            EntityAwareAttributeDescriptor.Regular.class,
            EntityAwareAttributeDescriptor.Wildcard.class,
            EntityDescriptor.class,
            EntityDescriptorImpl.class,
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
        repo.getAllEntities()
            .flatMap(d -> d.getAllAttributes().stream())
            .filter(d -> Objects.nonNull(d.getValueSerializer().getDefault()))
            .flatMap(d -> fieldsRecursively(d.getValueSerializer().getDefault()))
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
      if (fieldVal != null && !extracted.contains(fieldVal.getClass())) {
        extractFieldsRecursivelyInto(fieldVal.getClass(), extracted);
      }
    }
    return extracted.stream();
  }

  private static boolean isJDKInternal(Class<?> cls) {
    return cls.getName().startsWith("jdk.internal.")
        || cls.getName().startsWith("sun.")
        || cls.getName().startsWith("java.lang.reflect.")
        || cls.getName().startsWith("java.lang.module.");
  }

  private static void extractFieldsRecursivelyInto(Class<?> cls, Set<Class<?>> extracted) {
    extracted.add(cls);
    Arrays.stream(cls.getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .map(Field::getType)
        .filter(type -> !isJDKInternal(type))
        .filter(f -> !extracted.contains(f))
        .forEach(t -> extractFieldsRecursivelyInto(t, extracted));
    Arrays.stream(cls.getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .map(Field::getType)
        .filter(type -> !isJDKInternal(type))
        .forEach(extracted::add);
    if (cls.getSuperclass() != null
        && cls.getSuperclass() != Object.class
        && !extracted.contains(cls.getSuperclass())) {
      extractFieldsRecursivelyInto(cls.getSuperclass(), extracted);
    }
  }

  private RemoteConsumer<T> createRemoteConsumer(Coder<T> coder, Consumer<T> consumer) {
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
          this.pipelineFactory) {};
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

  private static PTransform<PCollection<StreamElement>, PDone> createBulkWriteTransform(
      int parallelism, BulkWriterFactory factory) {

    return createBulkWriteTransform(
        el -> (el.getKey().hashCode() & Integer.MAX_VALUE) % parallelism,
        new BulkWriteDoFn(factory));
  }

  @VisibleForTesting
  static PTransform<PCollection<StreamElement>, PDone> createBulkWriteTransform(
      UnaryFunction<StreamElement, Integer> keyFn, BulkWriteDoFn bulkWriteDoFn) {

    return new PTransform<>() {

      @Override
      public PDone expand(PCollection<StreamElement> input) {
        input
            .apply(
                org.apache.beam.sdk.transforms.MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.integers(), TypeDescriptor.of(StreamElement.class)))
                    .via(el -> KV.of(keyFn.apply(el), el)))
            .apply(ParDo.of(bulkWriteDoFn));
        return PDone.in(input.getPipeline());
      }
    };
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

    @Teardown
    public void tearDown() {
      if (consumer instanceof AutoCloseable) {
        ExceptionUtils.unchecked(((AutoCloseable) consumer)::close);
      }
    }
  }

  private static class StableConsumeFn<T> extends ConsumeFn<T> {

    StableConsumeFn(Consumer<T> consumer) {
      super(consumer);
    }

    @Override
    @ProcessElement
    @RequiresStableInput
    public void process(@Element T elem) {
      super.process(elem);
    }

    @Teardown
    @Override
    public void tearDown() {
      super.tearDown();
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
          TypeDescriptor.of(BoundedWindow.class),
          (TypeDescriptor<T>) TypeDescriptor.of(Object.class));
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
          (TypeDescriptor<T>) TypeDescriptor.of(Object.class), TypeDescriptor.of(Long.class));
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
          (TypeDescriptor<K>) TypeDescriptor.of(Object.class),
          (TypeDescriptor<V>) TypeDescriptor.of(Object.class));
    }
  }

  private static class AbstractReduceValueStateByKey<K, V, S, O>
      extends DoFn<KV<K, V>, Pair<K, O>> {

    private static final Instant MAX_ACCEPTABLE_STAMP =
        BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(300));

    static <K, V, S, O> AbstractReduceValueStateByKey<K, V, S, O> of(
        Closure<S> initialState,
        Closure<S> stateUpdate,
        Closure<O> output,
        Coder<K> keyCoder,
        Coder<S> stateCoder,
        Duration earlyEmitting,
        boolean sorted) {

      if (sorted) {
        return new ReduceValueStateByKey<>(
            initialState, stateUpdate, output, earlyEmitting, keyCoder, stateCoder);
      }
      return new ReduceValueStateByKeyUnsorted<>(
          initialState, stateUpdate, output, earlyEmitting, keyCoder, stateCoder);
    }

    private final Closure<S> initialState;
    private final Closure<S> stateUpdate;
    private final Closure<O> output;
    private final Duration earlyEmitting;

    AbstractReduceValueStateByKey(
        Closure<S> initialState,
        Closure<S> stateUpdate,
        Closure<O> output,
        Duration earlyEmitting) {

      this.initialState = initialState;
      this.stateUpdate = stateUpdate;
      this.output = output;
      this.earlyEmitting = earlyEmitting;
    }

    void processElement(
        KV<K, V> element,
        Instant ts,
        ValueState<Pair<K, S>> valueState,
        Timer earlyTimer,
        OutputReceiver<Pair<K, O>> outputReceiver) {

      K key = element.getKey();
      V value = element.getValue();
      Pair<K, S> current = valueState.read();
      if (current == null) {
        current = Pair.of(key, Objects.requireNonNull(initialState.call(key)));
        if (!earlyEmitting.equals(Duration.ZERO)) {
          earlyTimer.offset(earlyEmitting).setRelative();
        }
      }
      O outputElem = output.call(current.getSecond(), value);
      S updated = stateUpdate.call(current.getSecond(), value);
      valueState.write(Pair.of(key, updated));
      if (outputElem != null) {
        outputReceiver.outputWithTimestamp(Pair.of(key, outputElem), ts);
      }
    }

    void onTimer(
        Instant ts,
        ValueState<Pair<K, S>> valueState,
        Timer earlyTimer,
        OutputReceiver<Pair<K, O>> collector) {

      Pair<K, S> current = Objects.requireNonNull(valueState.read());
      O outputElem = output.call(current.getSecond(), null);
      if (outputElem != null) {
        collector.output(Pair.of(current.getFirst(), outputElem));
      }
      if (ts.isBefore(MAX_ACCEPTABLE_STAMP)) {
        earlyTimer.offset(earlyEmitting).setRelative();
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeDescriptor<Pair<K, O>> getOutputTypeDescriptor() {
      return PairCoder.descriptor(
          (TypeDescriptor<K>) TypeDescriptor.of(Object.class),
          (TypeDescriptor<O>) TypeDescriptor.of(Object.class));
    }
  }

  public static class ReduceValueStateByKey<K, V, S, O>
      extends AbstractReduceValueStateByKey<K, V, S, O> {

    @StateId("value")
    private final StateSpec<ValueState<Pair<K, S>>> state;

    @TimerId("earlyTimer")
    private final TimerSpec earlyTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    ReduceValueStateByKey(
        Closure<S> initialState,
        Closure<S> stateUpdate,
        Closure<O> output,
        Duration earlyEmitting,
        Coder<K> keyCoder,
        Coder<S> stateCoder) {

      super(initialState, stateUpdate, output, earlyEmitting);
      this.state = StateSpecs.value(PairCoder.of(keyCoder, stateCoder));
    }

    @RequiresTimeSortedInput
    @ProcessElement
    @Override
    public void processElement(
        @Element KV<K, V> element,
        @Timestamp Instant ts,
        @StateId("value") ValueState<Pair<K, S>> valueState,
        @TimerId("earlyTimer") Timer earlyTimer,
        OutputReceiver<Pair<K, O>> outputReceiver) {

      super.processElement(element, ts, valueState, earlyTimer, outputReceiver);
    }

    @OnTimer("earlyTimer")
    @Override
    public void onTimer(
        @Timestamp Instant ts,
        @StateId("value") ValueState<Pair<K, S>> valueState,
        @TimerId("earlyTimer") Timer earlyTimer,
        OutputReceiver<Pair<K, O>> collector) {

      super.onTimer(ts, valueState, earlyTimer, collector);
    }
  }

  public static class ReduceValueStateByKeyUnsorted<K, V, S, O>
      extends AbstractReduceValueStateByKey<K, V, S, O> {

    @StateId("value")
    private final StateSpec<ValueState<Pair<K, S>>> state;

    @TimerId("earlyTimer")
    private final TimerSpec earlyTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    ReduceValueStateByKeyUnsorted(
        Closure<S> initialState,
        Closure<S> stateUpdate,
        Closure<O> output,
        Duration earlyEmitting,
        Coder<K> keyCoder,
        Coder<S> stateCoder) {

      super(initialState, stateUpdate, output, earlyEmitting);
      this.state = StateSpecs.value(PairCoder.of(keyCoder, stateCoder));
    }

    @ProcessElement
    @Override
    public void processElement(
        @Element KV<K, V> element,
        @Timestamp Instant ts,
        @StateId("value") ValueState<Pair<K, S>> valueState,
        @TimerId("earlyTimer") Timer earlyTimer,
        OutputReceiver<Pair<K, O>> outputReceiver) {

      super.processElement(element, ts, valueState, earlyTimer, outputReceiver);
    }

    @OnTimer("earlyTimer")
    @Override
    public void onTimer(
        @Timestamp Instant ts,
        @StateId("value") ValueState<Pair<K, S>> valueState,
        @TimerId("earlyTimer") Timer earlyTimer,
        OutputReceiver<Pair<K, O>> collector) {

      super.onTimer(ts, valueState, earlyTimer, collector);
    }
  }

  private <InputT> DoFn<InputT, Void> asDoFn(
      PipelineOptions opts, boolean allowStable, Consumer<InputT> consumer) {

    if (allowStable && supportsStableInput(opts)) {
      return new StableConsumeFn<>(consumer);
    }
    return new ConsumeFn<>(consumer);
  }

  private boolean supportsStableInput(PipelineOptions opts) {
    return DirectRunner.class.isAssignableFrom(opts.getRunner())
        || FlinkRunner.class.isAssignableFrom(opts.getRunner());
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
              TimeUnit.MILLISECONDS.sleep(50);
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

  static <X> Closure<X> dehydrate(Closure<X> closure) {
    if (closure.getOwner() instanceof Serializable) {
      return closure;
    }
    return closure.dehydrate();
  }

  <InputT> Coder<InputT> coderOf(Pipeline pipeline, Closure<InputT> closure) {
    return getCoder(pipeline, TypeDescriptor.of(Types.returnClass(closure)));
  }

  static <T> Coder<T> getCoder(Pipeline pipeline, TypeDescriptor<T> type) {
    try {
      return pipeline.getCoderRegistry().getCoder(type);
    } catch (CannotProvideCoderException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  static <
          InputT,
          OutputT,
          TransformT extends PTransform<? super PCollection<InputT>, PCollection<OutputT>>>
      PCollection<OutputT> applyNamedTransform(
          @Nullable String prefix, String suffix, PCollection<InputT> in, TransformT transform) {

    return prefix == null ? in.apply(transform) : in.apply(prefix + suffix, transform);
  }

  @VisibleForTesting
  static class BulkWriteDoFn extends DoFn<KV<Integer, StreamElement>, Void> {

    private static final String TIMER_NAME = "flushTimer";
    private static final Instant END_OF_TIME =
        BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(100));

    @TimerId(TIMER_NAME)
    private final TimerSpec finishTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final BulkWriterFactory factory;
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    public BulkWriteDoFn(BulkWriterFactory factory) {
      this.factory = factory;
    }

    @RequiresTimeSortedInput
    @ProcessElement
    public void process(
        @Element KV<Integer, StreamElement> element, @TimerId(TIMER_NAME) Timer timer) {

      throwOnError();
      timer.set(END_OF_TIME);
      factory.checkAllowed(element.getValue().getAttributeDescriptor());
      BulkAttributeWriter writer = factory.getWriter(element.getKey());
      writer.write(
          element.getValue(),
          element.getValue().getStamp(),
          (succ, exc) -> {
            if (!succ) {
              error.set(exc);
            }
          });
    }

    @OnTimer(TIMER_NAME)
    public void flush(@Key Integer key) {
      throwOnError();
      BulkAttributeWriter writer = factory.getWriter(key);
      writer.updateWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
      writer.close();
      factory.resetWriter(key);
    }

    void throwOnError() {
      Throwable err = error.getAndSet(null);
      if (err != null) {
        throw new RuntimeException(err);
      }
    }
  }

  @VisibleForTesting
  interface BulkWriterFactory extends Serializable {

    static BulkWriterFactory wrap(
        SerializableScopedValue<Integer, AttributeWriterBase> writer,
        Set<String> allowedAttributes) {

      return new BulkWriterFactory() {
        @Override
        public BulkAttributeWriter getWriter(int key) {
          return writer.get(key).bulk();
        }

        @Override
        public void checkAllowed(AttributeDescriptor<?> attribute) {
          Preconditions.checkArgument(allowedAttributes.contains(attribute.getName()));
        }

        @Override
        public void resetWriter(int key) {
          writer.reset(key);
        }
      };
    }

    BulkAttributeWriter getWriter(int key);

    void checkAllowed(AttributeDescriptor<?> attribute);

    /** After the writer has been closed, reset internal reference. */
    void resetWriter(int key);
  }
}
