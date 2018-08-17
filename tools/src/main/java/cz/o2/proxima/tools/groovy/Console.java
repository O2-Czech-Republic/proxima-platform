/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.groovy;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc.RetrieveServiceBlockingStub;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.source.BatchSource;
import cz.o2.proxima.source.BoundedStreamSource;
import cz.o2.proxima.tools.io.ConsoleRandomReader;
import cz.o2.proxima.source.UnboundedStreamSource;
import cz.o2.proxima.tools.io.TypedStreamElement;
import cz.o2.proxima.util.Classpath;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.executor.local.ProcessingTimeTriggerScheduler;
import cz.seznam.euphoria.executor.local.WatermarkEmitStrategy;
import cz.seznam.euphoria.executor.local.WatermarkTriggerScheduler;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

/**
 * This is the groovysh based console.
 */
@Slf4j
public class Console {

  private static volatile Console INSTANCE = null;

  /**
   * This is supposed to be called only from the groovysh initialized in this
   * main method.
   * @return the singleton instance
   */
  public static final Console get() {
    return INSTANCE;
  }

  public static Console get(String[] args) {
    if (INSTANCE == null) {
      synchronized (Console.class) {
        if (INSTANCE == null) {
          INSTANCE = new Console(args);
        }
      }
    }
    return INSTANCE;
  }

  public static void main(String[] args) {
    Console console = Console.get(args);
    Runtime.getRuntime().addShutdownHook(new Thread(console::close));

    console.runInputForwarding();
    Groovysh shell = new Groovysh(new IO(
        console.getInputStream(), System.out, System.err));
    shell.run("env = " + Console.class.getName() + ".get().getEnv()");
    System.out.println();
    console.close();
  }

  final AtomicReference<Flow> flow = new AtomicReference<>(createFlow());
  final BlockingQueue<Byte> input = new ArrayBlockingQueue<>(1000);
  final Repository repo;
  final List<ConsoleRandomReader> readers = new ArrayList<>();
  final Configuration conf;
  final ExecutorService executor = Executors.newCachedThreadPool(r -> {
    Thread t = new Thread(r);
    t.setName("input-forwarder");
    t.setDaemon(true);
    t.setUncaughtExceptionHandler((thrd, err) ->
        log.error("Error in thread {}", thrd.getName(), err));
    return t;
  });

  Console(String[] paths) {
    ClassLoader old = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(new GroovyClassLoader(old));
    repo = getRepo(paths);
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
  }

  public GroovyObject getEnv() throws Exception {
    return GroovyEnv.of(conf,
        (GroovyClassLoader) Thread.currentThread().getContextClassLoader(),
        repo);
  }

  private Repository getRepo(String[] paths) {
    Config config;
    if (paths.length > 0) {
      config = Arrays.stream(paths)
        .map(p -> ConfigFactory.parseFile(new File(p)))
        .reduce(ConfigFactory.empty(), (a, b) -> b.withFallback(a));
    } else {
      config = ConfigFactory.load();
    }
    return Repository.of(config.resolve());
  }

  Flow createFlow() {
    return Flow.create();
  }

  Flow createFlow(String name) {
    return Flow.create(name);
  }

  void resetFlow() {
    flow.set(createFlow());
  }


  @SuppressWarnings("unchecked")
  public <T> Stream<TypedStreamElement<?>> getStream(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      Position position,
      boolean stopAtCurrent) {

    return getStream(
        entityDesc, attrDesc, position, stopAtCurrent, false);
  }


  @SuppressWarnings("unchecked")
  public <T> Stream<TypedStreamElement<?>> getStream(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime) {

    CommitLogReader reader = repo.getFamiliesForAttribute(attrDesc)
        .stream()
        .filter(af -> af.getAccess().canReadCommitLog())
        .map(af -> af.getCommitLogReader().get())
        .findAny()
        .orElseThrow(() -> new IllegalArgumentException(
            "Attribute " + attrDesc + " has no commit log"));

    final DatasetBuilder<TypedStreamElement<Object>> builder;
    builder = () -> {
      final DataSource source;
      if (stopAtCurrent) {
        source = BoundedStreamSource.of(reader, position);
      } else {
        source = UnboundedStreamSource.of(reader, position);
      }

      Dataset<StreamElement> ds = flow.get().createInput(source);

      String prefix = attrDesc.toAttributePrefix();
      if (eventTime) {
        ds = AssignEventTime.of(ds)
            .using(StreamElement::getStamp)
            .output();
      }
      Dataset<StreamElement> filtered = Filter.of(ds)
          .by(t -> t.getAttributeDescriptor().toAttributePrefix().equals(prefix))
          .output();
      return MapElements.of(filtered)
          .using(TypedStreamElement::of)
          .output();
    };
    return Stream.wrap(
        new LocalExecutor()
            .setTriggeringSchedulerSupplier(() ->
                eventTime
                    ? new WatermarkTriggerScheduler(500)
                    : new ProcessingTimeTriggerScheduler())
            .setWatermarkEmitStrategySupplier(WatermarkEmitStrategy.Default::new),
        (DatasetBuilder) builder,
        this::resetFlow,
        this::unboundedStreamInterrupt);
  }

  public <T> WindowedStream<TypedStreamElement<T>, GlobalWindowing> getBatchSnapshot(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc) {

    return getBatchSnapshot(entityDesc, attrDesc, Long.MIN_VALUE, Long.MAX_VALUE);
  }


  @SuppressWarnings("unchecked")
  public <T> WindowedStream<TypedStreamElement<T>, GlobalWindowing> getBatchSnapshot(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      long fromStamp,
      long toStamp) {

    DatasetBuilder<StreamElement> builder = () -> {
      final Dataset<StreamElement> ds;
      AttributeFamilyDescriptor family = repo.getFamiliesForAttribute(attrDesc)
          .stream()
          .filter(af -> af.getAccess().canReadBatchSnapshot())
          .filter(af -> af.getBatchObservable().isPresent())
          .findAny()
          .orElse(null);

      if (family == null || fromStamp > Long.MIN_VALUE || toStamp < Long.MAX_VALUE) {
        ds = reduceUpdatesToSnapshot(attrDesc, fromStamp, toStamp);
      } else {
        Dataset<StreamElement> raw = flow.get().createInput(BatchSource.of(
            family.getBatchObservable().get(),
            family,
            fromStamp,
            toStamp));
        ds = Filter.of(raw)
            .by(i -> i.getStamp() >= fromStamp && i.getStamp() < toStamp)
            .output();
      }

      String prefix = attrDesc.toAttributePrefix();
      return Filter.of(ds)
          .by(t -> t.getAttributeDescriptor().toAttributePrefix().equals(prefix))
          .output();
    };

    return Stream.wrap(
        new LocalExecutor()
            .setTriggeringSchedulerSupplier(ProcessingTimeTriggerScheduler::new)
            .setWatermarkEmitStrategySupplier(WatermarkEmitStrategy.Default::new),
        (DatasetBuilder) builder,
        this::resetFlow,
        this::unboundedStreamInterrupt).windowAll();

  }

  private Dataset<StreamElement> reduceUpdatesToSnapshot(
      AttributeDescriptor<?> attrDesc,
      long fromStamp,
      long toStamp) {

    // create the data by reducing stream updates
    CommitLogReader reader = repo.getFamiliesForAttribute(attrDesc)
        .stream()
        .filter(af -> af.getAccess().isStateCommitLog())
        .map(af -> af.getCommitLogReader().get())
        .findAny()
        .orElseThrow(() -> new IllegalStateException(
            "Cannot create batch snapshot, missing random access family "
                + "and state commit log for " + attrDesc));
    Dataset<StreamElement> stream = flow.get().createInput(
        UnboundedStreamSource.of(reader, Position.OLDEST));
    // filter by stamp
    stream = Filter.of(stream)
        .by(i -> i.getStamp() >= fromStamp && i.getStamp() < toStamp)
        .output();
    final Dataset<Pair<Pair<String, String>, StreamElement>> reduced;
    reduced = ReduceByKey.of(stream)
        .keyBy(i -> Pair.of(i.getKey(), i.getAttribute()))
        .combineBy(values -> {
          StreamElement res = null;
          Iterable<StreamElement> iter = values::iterator;
          for (StreamElement v : iter) {
            if (res == null || v.getStamp() > res.getStamp()) {
              res = v;
            }
          }
          return res;
        })
        .output();
    return FlatMap.of(reduced)
        .using((
            Pair<Pair<String, String>, StreamElement> e,
            Collector<StreamElement> ctx) -> {
          if (e.getSecond().getValue() != null) {
            ctx.collect(e.getSecond());
          }
        })
        .output();
  }


  @SuppressWarnings("unchecked")
  public <T> WindowedStream<StreamElement, GlobalWindowing> getBatchUpdates(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      long startStamp,
      long endStamp) {

    AttributeFamilyDescriptor family = repo.getFamiliesForAttribute(attrDesc)
        .stream()
        .filter(af -> af.getAccess().canReadBatchUpdates())
        .filter(af -> af.getBatchObservable().isPresent())
        .findAny()
        .orElseThrow(() -> new IllegalStateException("Attribute "
            + attrDesc.getName() + " has no random access reader"));

    DatasetBuilder<StreamElement> builder = () -> {
      Dataset<StreamElement> ds = flow.get().createInput(BatchSource.of(
          family.getBatchObservable().get(),
          family,
          startStamp, endStamp));

      ds = Filter.of(ds)
          .by(i -> i.getStamp() >= startStamp && i.getStamp() < endStamp)
          .output();

      String prefix = attrDesc.toAttributePrefix();
      Dataset<StreamElement> filtered = Filter.of(ds)
          .by(t -> t.getAttributeDescriptor().toAttributePrefix().equals(prefix))
          .output();
      return AssignEventTime.of(filtered)
          .using(StreamElement::getStamp)
          .output();
    };

    return Stream.wrap(
        new LocalExecutor()
            .setTriggeringSchedulerSupplier(ProcessingTimeTriggerScheduler::new)
            .setWatermarkEmitStrategySupplier(WatermarkEmitStrategy.Default::new),
        (DatasetBuilder) builder,
        this::resetFlow,
        this::unboundedStreamInterrupt).windowAll();

  }


  public ConsoleRandomReader getRandomAccessReader(String entity) {

    EntityDescriptor entityDesc = findEntityDescriptor(entity);

    ConsoleRandomReader reader = new ConsoleRandomReader(entityDesc, repo);
    readers.add(reader);
    return reader;
  }

  public void put(
      EntityDescriptor entityDesc,
      AttributeDescriptor attrDesc,
      String key, String attribute, String textFormat)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
          ClassNotFoundException, InvalidProtocolBufferException, InterruptedException,
          TextFormat.ParseException {

    if (attrDesc.getSchemeUri().getScheme().equals("proto")) {
      String protoClass = attrDesc.getSchemeUri().getSchemeSpecificPart();
      Class<AbstractMessage> cls = Classpath.findClass(protoClass, AbstractMessage.class);
      byte[] payload = null;
      if (textFormat != null) {
        Method newBuilder = cls.getDeclaredMethod("newBuilder");
        Builder builder = (Builder) newBuilder.invoke(null);
        TextFormat.merge(textFormat, builder);
        payload = builder.build().toByteArray();
      }
      OnlineAttributeWriter writer = repo.getWriter(attrDesc)
          .orElseThrow(() -> new IllegalArgumentException(
              "Missing writer for " + attrDesc));
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<Throwable> exc = new AtomicReference<>();
      writer.write(StreamElement.update(
          entityDesc, attrDesc, UUID.randomUUID().toString(),
          key, attribute, System.currentTimeMillis(), payload), (success, ex) -> {
            if (!success) {
              exc.set(ex);
            }
            latch.countDown();
          });
      latch.await();
      if (exc.get() != null) {
        throw new RuntimeException(exc.get());
      }
    } else {
      throw new IllegalArgumentException(
          "Don't know how to make builder for "
          + attrDesc.getSchemeUri());
    }

  }

  public void delete(
      EntityDescriptor entityDesc, AttributeDescriptor<?> attrDesc,
      String key, String attribute) throws InterruptedException {

    OnlineAttributeWriter writer = repo.getWriter(attrDesc)
        .orElseThrow(() -> new IllegalArgumentException(
            "Missing writer for " + attrDesc));
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> exc = new AtomicReference<>();
    writer.write(StreamElement.update(
        entityDesc, attrDesc, UUID.randomUUID().toString(),
        key, attribute, System.currentTimeMillis(), null), (success, ex) -> {
          if (!success) {
            exc.set(ex);
          }
          latch.countDown();
        });
    latch.await();
    if (exc.get() != null) {
      throw new RuntimeException(exc.get());
    }
  }

  public EntityDescriptor findEntityDescriptor(String entity) {
    return repo.findEntity(entity).orElseThrow(
        () -> new IllegalArgumentException("Entity " + entity + " not found"));
  }


  public Rpc.ListResponse rpcList(EntityDescriptor entity,
      String key, AttributeDescriptor wildcard, String offset, int limit,
      String host, int port) {

    Channel channel = ManagedChannelBuilder
        .forAddress(host, port)
        .directExecutor()
        .usePlaintext()
        .build();

    RetrieveServiceBlockingStub stub = RetrieveServiceGrpc.newBlockingStub(channel);
    return stub.listAttributes(Rpc.ListRequest.newBuilder()
        .setEntity(entity.getName())
        .setKey(key)
        .setWildcardPrefix(wildcard.toAttributePrefix(false))
        .setOffset(offset)
        .setLimit(limit)
        .build());
  }

  public Rpc.GetResponse rpcGet(EntityDescriptor entity,
      String key, String attr, String host, int port) {

    Channel channel = ManagedChannelBuilder
        .forAddress(host, port)
        .directExecutor()
        .usePlaintext()
        .build();

    RetrieveServiceBlockingStub stub = RetrieveServiceGrpc.newBlockingStub(channel);
    return stub.get(Rpc.GetRequest.newBuilder()
        .setEntity(entity.getName())
        .setAttribute(attr)
        .setKey(key)
        .build());
  }

  private void close() {
    readers.forEach(ConsoleRandomReader::close);
  }

  private boolean unboundedStreamInterrupt() {
    try {
      return takeInputChar() == 'q';
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return true;
    }
  }

  private void runInputForwarding() {
    executor.execute(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          byte next = (byte) System.in.read();
          while (!input.offer(next)) {
            input.remove();
          }
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    });
  }

  private InputStream getInputStream() {
    return new InputStream() {

      @Override
      public int read() throws IOException {
        try {
          return input.take();
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          return -1;
        }
      }

    };
  }

  private int takeInputChar() throws InterruptedException {
    return input.take();
  }

}
