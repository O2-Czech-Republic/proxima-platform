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
import fi.iki.elonen.NanoHTTPD;
import static fi.iki.elonen.NanoHTTPD.newFixedLengthResponse;
import groovy.lang.Closure;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
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
    PCollection<T> pcoll = collection.materialize(pipeline);
    try (RemoteConsumer<T> remoteConsumer = createRemoteConsumer(pcoll, consumer)) {
      pcoll.apply(asWriteTransform(asDoFn(remoteConsumer::add)));
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
  }

  @Override
  public void print() {
    forEach(BeamStream::print);
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
    forEach(ret::add);
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
  public BeamStream<StreamElement> asStreamElements(
      RepositoryProvider repoProvider, EntityDescriptor entity,
      Closure<String> keyExtractor, Closure<String> attributeExtractor,
      Closure<T> valueExtractor, Closure<Long> timeExtractor) {

    Repository repo = repoProvider.getRepo();
    Closure<String> keyDehydrated = keyExtractor.dehydrate();
    Closure<String> attributeDehydrated = attributeExtractor.dehydrate();
    Closure<T> valueDehydrated = valueExtractor.dehydrate();
    Closure<Long> timeDehydrated = timeExtractor.dehydrate();

    return descendant(pipeline -> MapElements
        .of(collection.materialize(pipeline))
        .using(data -> {
          String key = keyDehydrated.call(data);
          String attribute = attributeDehydrated.call(data);
          AttributeDescriptor<Object> attrDesc = entity.findAttribute(attribute, true)
              .orElseThrow(() -> new IllegalArgumentException(
                  "No attribute " + attribute + " in " + entity));
          long timestamp = timeDehydrated.call(data);
          byte[] value = attrDesc.getValueSerializer()
              .serialize(valueDehydrated.call(data));
          return StreamElement.update(
              entity, attrDesc, UUID.randomUUID().toString(), key, attribute,
              timestamp, value);
        }, TypeDescriptor.of(StreamElement.class))
        .output()
        .setCoder(StreamElementCoder.of(repo)));
  }


  @Override
  public void persist(
      RepositoryProvider repoProvider, EntityDescriptor entity,
      Closure<String> keyExtractor, Closure<String> attributeExtractor,
      Closure<T> valueExtractor, Closure<Long> timeExtractor) {

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

    elements.forEach(el -> {
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
    return new BeamStream<>(config, bounded, provider, terminateCheck, pipelineFactory);
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
        config, bounded, provider, window,
        WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES,
        terminateCheck, pipelineFactory);
  }

  private void registerCoders(CoderRegistry registry) {
    registry.registerCoderForClass(
        GlobalWindow.class, GlobalWindow.Coder.INSTANCE);
    registrars.forEach(r -> r.accept(registry));
    // FIXME: need to get rid of this fallback
    registry.registerCoderForClass(Object.class, KryoCoder.of());
  }

  private <T> RemoteConsumer<T> createRemoteConsumer(
      PCollection<T> collection, Consumer<T> consumer) {

    return RemoteConsumer.create(
        this, config.getCollectHostname(),
        config.getPreferredCollectPort(), consumer, collection);
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

  private static <T> PTransform<PCollection<T>, PDone> asWriteTransform(DoFn<T, ?> doFn) {
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

  // iterable that collects elements using HTTP
  // this is first shot implementation with no optimizations
  private static class RemoteConsumer<T> implements Serializable, AutoCloseable {

    private static final Random RANDOM = new Random();

    private static <T> RemoteConsumer<T> create(
        Object seed, String hostname, int preferredPort,
        Consumer<T> consumer, PCollection<T> collection) {

      int retries = 3;
      while (retries > 0) {
        retries--;
        int port = getPort(preferredPort, System.identityHashCode(seed));
        try {
          // start HTTP server and store host and port

          Coder<T> coder = collection.getCoder();
          if (coder == null) {
            // can this happen?
            coder = collection.getPipeline().getCoderRegistry()
                .getCoder(collection.getTypeDescriptor());
          }
          RemoteConsumer<T> ret = new RemoteConsumer<>(hostname, port, consumer, coder);
          ret.start();
          return ret;
        } catch (BindException ex) {
          log.debug("Failed to bind on port {}", port, ex);
        } catch (Exception ex) {
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

}