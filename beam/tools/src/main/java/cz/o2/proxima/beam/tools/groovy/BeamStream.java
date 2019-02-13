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

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
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
import groovy.lang.Closure;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

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

    return withRegisteredTypes(
        beam.getRepository(),
        new BeamStream<>(
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
            true,
            pipeline -> beam.getBatchSnapshot(
                pipeline, fromStamp, toStamp, attrs),
            terminateCheck,
            pipelineFactory))
        .windowAll();
  }

  final boolean bounded;
  final PCollectionProvider<T> collection;
  final List<Consumer<CoderRegistry>> registrars = new ArrayList<>();
  final StreamProvider.TerminatePredicate terminateCheck;
  final Factory<Pipeline> pipelineFactory;

  BeamStream(
      boolean bounded, PCollectionProvider<T> input,
      StreamProvider.TerminatePredicate terminateCheck) {

    this(bounded, input, terminateCheck, BeamStream::createPipelineDefault);
  }


  BeamStream(
      boolean bounded, PCollectionProvider<T> input,
      StreamProvider.TerminatePredicate terminateCheck,
      Factory<Pipeline> pipelineFactory) {

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
  public <X> Stream<X> map(Closure<X> mapper) {
    Closure<X> dehydrated = mapper.dehydrate();
    TypeDescriptor<X> typeDesc = typeOf(dehydrated);
    return descendant(
        pipeline -> MapElements
            .of(collection.materialize(pipeline))
            .using(e -> dehydrated.call(e), typeDesc)
            .output());
  }

  @Override
  public Stream<T> filter(Closure<Boolean> predicate) {
    Closure<Boolean> dehydrated = predicate.dehydrate();
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return Filter
            .of(in)
            .by(dehydrated::call)
            .output()
            .setCoder(in.getCoder());
        });
  }

  @Override
  public Stream<T> assignEventTime(Closure<Long> assigner) {
    Closure<Long> dehydrated = assigner.dehydrate();
    return descendant(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return AssignEventTime
              .of(in)
              .using(dehydrated::call)
              .output()
              .setTypeDescriptor(in.getTypeDescriptor())
              .setCoder(in.getCoder());
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public Stream<Pair<Object, T>> withWindow() {
    return descendant(
        pipeline -> {
          try {
            PCollection<T> in = collection.materialize(pipeline);
            TypeDescriptor<BoundedWindow> windowType = (TypeDescriptor)
                in.getWindowingStrategy().getWindowFn().getWindowTypeDescriptor();
            CoderRegistry registry = pipeline.getCoderRegistry();
            Coder<BoundedWindow> windowCoder = registry.getCoder(windowType);
            return (PCollection) in.apply(ParDo.of(extractWindow()))
                .setCoder(PairCoder.of(windowCoder, in.getCoder()));
          } catch (CannotProvideCoderException ex) {
            throw new RuntimeException(ex);
          }
        });
  }

  private void forEach(Consumer<T> consumer) {
    Pipeline pipeline = createPipeline();
    PCollection<T> materialized = collection.materialize(pipeline);
    materialized.apply(write(asDoFn(consumer)));
    AtomicReference<PipelineResult> result = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    Thread running = runThread("pipeline-start-thread", () -> {
      try {
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
  public void forEach(Closure<?> consumer) {
    Closure<?> dehydrated = consumer.dehydrate();
    forEach(asConsumer(dehydrated));
  }

  private static <T> Consumer<T> asConsumer(Closure<?> dehydrated) {
    return new Consumer<T>() {
      @Override
      public void accept(T input) {
        dehydrated.call(input);
      }
    };
  }

  @Override
  public List<T> collect() {
    List<T> result = newUnserializableList();
    forEach(result::add);
    return result;
  }

  @Override
  public boolean isBounded() {
    return bounded;
  }

  @Override
  public void persistIntoTargetReplica(
      RepositoryProvider repoProvider, String replicationName, String target) {

    // @todo
  }

  @Override
  public void persist(
      RepositoryProvider repoProvider, EntityDescriptor entity,
      Closure<String> keyExtractor, Closure<String> attributeExtractor,
      Closure<T> valueExtractor, Closure<Long> timeExtractor) {

    Repository repo = repoProvider.getRepo();
    Closure<String> keyDehydrated = keyExtractor.dehydrate();
    Closure<String> attributeDehydrated = attributeExtractor.dehydrate();
    Closure<T> valueDehydrated = valueExtractor.dehydrate();
    Closure<Long> timeDehydrated = timeExtractor.dehydrate();

    forEach(data -> {
      String key = keyDehydrated.call(data);
      String attribute = attributeDehydrated.call(data);
      AttributeDescriptor<Object> attrDesc = entity.findAttribute(attribute, true)
          .orElseThrow(() -> new IllegalArgumentException(
              "No attribute " + attribute + " in " + entity));
      long timestamp = timeDehydrated.call(data);
      byte[] value = attrDesc.getValueSerializer().serialize(valueDehydrated.call(data));
      StreamElement el = StreamElement.update(
          entity, attrDesc, UUID.randomUUID().toString(), key, attribute,
          timestamp, value);
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<Throwable> err = new AtomicReference<>();
      OnlineAttributeWriter writer = repo.getOrCreateOperator(DirectDataOperator.class)
          .getWriter(attrDesc)
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
    });
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

    Closure<K> dehydrated = keyExtractor.dehydrate();
    TypeDescriptor<K> type = typeOf(keyExtractor);

    return windowed(
        pipeline -> {
          PCollection<T> in = collection.materialize(pipeline);
          return MapElements.of(in)
              .using(
                  e -> Pair.of(dehydrated.call(e), e),
                  PairCoder.descriptor(type, in.getTypeDescriptor()))
              .output()
              .setTypeDescriptor(PairCoder.descriptor(
                  type, in.getTypeDescriptor()));
        },
        Sessions.withGapDuration(Duration.millis(gapDuration)));
  }

  @Override
  public WindowedStream<T> windowAll() {
    return windowed(collection, new GlobalWindows());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Stream<T> union(Stream<T> other) {
    return descendant(
        pipeline -> {
          PCollection<T> left = collection.materialize(pipeline);
          PCollection<T> right = ((BeamStream<T>) other)
              .collection.materialize(pipeline);
          return PCollectionList.of(Arrays.asList(left, right))
              .apply(Flatten.pCollections())
              .setTypeDescriptor(left.getTypeDescriptor())
              .setCoder(left.getCoder());
        });
  }

  <X> BeamStream<X> descendant(PCollectionProvider<X> provider) {
    return new BeamStream<>(bounded, provider, terminateCheck, pipelineFactory);
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


  <T> TypeDescriptor<T> typeOf(Closure<T> closure) {
    return TypeDescriptor.of(Types.returnClass(closure));
  }

  <X> BeamWindowedStream<X> windowed(
      PCollectionProvider<X> provider,
      WindowFn<? super X, ?> window) {

    return new BeamWindowedStream<>(
        bounded, provider, window,
        WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES,
        terminateCheck, pipelineFactory);
  }

  private void registerCoders(CoderRegistry registry) {
    registry.registerCoderForClass(
        GlobalWindow.class, GlobalWindow.Coder.INSTANCE);
    registrars.forEach(r -> r.accept(registry));
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
    @ProcessElement
    public void process(
        @Element T elem,
        BoundedWindow window,
        OutputReceiver<Pair<BoundedWindow, T>> output) {

      output.output(Pair.of(window, elem));
    }
  }

  private static <T> DoFn<T, Void> asDoFn(Consumer<T> consumer) {
    return new ConsumeFn<>(consumer);
  }

  private static <T> PTransform<PCollection<T>, PDone> write(DoFn<T, ?> doFn) {
    return new PTransform<PCollection<T>, PDone>() {
      @Override
      public PDone expand(PCollection<T> input) {
        input.apply(ParDo.of(doFn));
        return PDone.in(input.getPipeline());
      }
    };
  }

  private static <T> DoFn<T, Pair<BoundedWindow, T>> extractWindow() {
    return new ExtractWindow<>();
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

  // this is hackish and should be replaced with
  // implementation that is able to store data even remotely
  // this implies serializalization safety

  private static class SingletonList<T> extends ArrayList<T> {

    @SuppressWarnings("unchecked")
    private static final SingletonList INSTANCE = new SingletonList<>();

    Object readResolve() {
      return INSTANCE;
    }

    @Override
    public void add(int index, T element) {
      synchronized (INSTANCE) {
        super.add(index, element);
      }
    }

    @Override
    public boolean add(T e) {
      synchronized (INSTANCE) {
        return super.add(e);
      }
    }

  }

  @SuppressWarnings("unchecked")
  private static <T> List<T> newUnserializableList() {
    SingletonList.INSTANCE.clear();
    return SingletonList.INSTANCE;
  }

}