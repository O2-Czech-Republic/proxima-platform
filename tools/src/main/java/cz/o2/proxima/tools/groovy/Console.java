/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.CommitLogReader.Position;
import cz.o2.proxima.tools.io.BatchSource;
import cz.o2.proxima.tools.io.ConsoleRandomReader;
import cz.o2.proxima.tools.io.StreamSource;
import cz.o2.proxima.tools.io.TypedIngest;
import cz.o2.proxima.util.Classpath;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.inmem.InMemExecutor;
import cz.seznam.euphoria.inmem.ProcessingTimeTriggerScheduler;
import cz.seznam.euphoria.inmem.WatermarkEmitStrategy;
import cz.seznam.euphoria.inmem.WatermarkTriggerScheduler;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.codehaus.groovy.tools.shell.Groovysh;

/**
 * This is the groovysh based console.
 */
public class Console {

  private static volatile Console INSTANCE = null;

  /**
   * This is supposed to be called only from the groovysh initialized in this
   * main method.
   */
  public static final Console get() { return INSTANCE; }

  public static Console get(String[] args) throws Exception {
    if (INSTANCE == null) {
      synchronized (Console.class) {
        if (INSTANCE == null) {
          INSTANCE = new Console(args);
        }
      }
    }
    return INSTANCE;
  }

  AtomicReference<Flow> flow = new AtomicReference<>(createFlow());

  public static void main(String[] args) throws Exception {
    Console console = Console.get(args);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      console.close();
    }));

    Groovysh shell = new Groovysh();
    shell.run("env = " + Console.class.getName() + ".get().getEnv()");
    System.out.println();
    console.close();
  }

  Repository repo;
  List<ConsoleRandomReader> readers = new ArrayList<>();
  final Configuration conf;

  Console(String[] paths) throws Exception {
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

  private static void usage() {
    System.err.println(String.format("Usage %s <config> [<config>]", Console.class.getName()));
    System.exit(1);
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
  public <T> Stream<TypedIngest<T>> getStream(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      Position position,
      boolean stopAtCurrent) {

    return getStream(
        entityDesc, attrDesc, position, stopAtCurrent, false);
  }


  @SuppressWarnings("unchecked")
  public <T> Stream<TypedIngest<T>> getStream(
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

    DatasetBuilder<TypedIngest<?>> builder = () -> {
      Dataset<TypedIngest<?>> input = flow.get().createInput(StreamSource.of(
          reader,
          position,
          stopAtCurrent,
          TypedIngest::of));

      String prefix = attrDesc.toAttributePrefix();
      if (eventTime) {
        input = AssignEventTime.of(input)
            .using(TypedIngest::getStamp)
            .output();
      }
      return Filter.of(input)
          .by(t -> t.getAttrDesc().toAttributePrefix().equals(prefix))
          .output();
    };

    return Stream.wrap(
        new InMemExecutor()
            .setTriggeringSchedulerSupplier(() -> {
              return eventTime
                  ? new WatermarkTriggerScheduler(500)
                  : new ProcessingTimeTriggerScheduler();
            })
            .setWatermarkEmitStrategySupplier(() -> new WatermarkEmitStrategy.Default()),
        (DatasetBuilder) builder,
        this::resetFlow);
  }

  public <T> WindowedStream<TypedIngest<T>> getBatchSnapshot(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc) {

    return getBatchSnapshot(entityDesc, attrDesc, Long.MIN_VALUE, Long.MAX_VALUE);
  }


  @SuppressWarnings("unchecked")
  public <T> WindowedStream<TypedIngest<T>> getBatchSnapshot(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      long fromStamp,
      long toStamp) {

    DatasetBuilder<TypedIngest<Object>> builder = () -> {
      final Dataset<TypedIngest<Object>> input;
      AttributeFamilyDescriptor<? extends AttributeWriterBase> family = repo.getFamiliesForAttribute(attrDesc)
          .stream()
          .filter(af -> af.getAccess().canReadBatchSnapshot())
          .filter(af -> af.getBatchObservable().isPresent())
          .findAny()
          .orElse(null);

      if (family == null || fromStamp > Long.MIN_VALUE || toStamp < Long.MAX_VALUE) {
        // create the data by reducing stream updates
        CommitLogReader reader = repo.getFamiliesForAttribute(attrDesc)
            .stream()
            .filter(af -> af.getAccess().isStateCommitLog())
            .map(af -> af.getCommitLogReader().get())
            .findAny()
            .orElseThrow(() -> new IllegalStateException(
                "Cannot create batch snapshot, missing random access family and state commit log for " + attrDesc));
        Dataset<TypedIngest<Object>> stream = flow.get().createInput(
            StreamSource.of(reader, Position.OLDEST, true, TypedIngest::of));

        // filter by stamp
        stream = Filter.of(stream)
            .by(i -> i.getStamp() >= fromStamp && i.getStamp() < toStamp)
            .output();

        Dataset<Pair<Pair<String, String>, TypedIngest<Object>>> reduced = ReduceByKey.of(stream)
            .keyBy(i -> Pair.of(i.getKey(), i.getAttribute()))
            .combineBy(values -> {
              TypedIngest<Object> res = null;
              for (TypedIngest<Object> v : values) {
                if (res == null || v.getStamp() > res.getStamp()) {
                  res = v;
                }
              }
              return res;
            })
            .output();

        input = FlatMap.of(reduced)
            .using((Pair<Pair<String, String>, TypedIngest<Object>> e, Collector<TypedIngest<Object>> ctx) -> {
              if (e.getSecond().getValue() != null) {
                ctx.collect(e.getSecond());
              }
            })
            .output();

      } else {
        Dataset<TypedIngest<Object>> raw = flow.get().createInput(BatchSource.of(
            family.getBatchObservable().get(),
            family,
            fromStamp,
            toStamp));
        input = Filter.of(raw)
            .by(i -> i.getStamp() >= fromStamp && i.getStamp() < toStamp)
            .output();
      }

      String prefix = attrDesc.toAttributePrefix();
      return Filter.of(input)
          .by(t -> t.getAttrDesc().toAttributePrefix().equals(prefix))
          .output();
    };

    return Stream.wrap(
        new InMemExecutor()
            .setTriggeringSchedulerSupplier(() -> {
              return new ProcessingTimeTriggerScheduler();
            })
            .setWatermarkEmitStrategySupplier(() -> new WatermarkEmitStrategy.Default()),
        (DatasetBuilder) builder,
        this::resetFlow).windowAll();

  }


  @SuppressWarnings("unchecked")
  public <T> WindowedStream<TypedIngest<T>> getBatchUpdates(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      long startStamp,
      long endStamp) {

    AttributeFamilyDescriptor<? extends AttributeWriterBase> family = repo.getFamiliesForAttribute(attrDesc)
        .stream()
        .filter(af -> af.getAccess().canReadBatchUpdates())
        .filter(af -> af.getBatchObservable().isPresent())
        .findAny()
        .orElseThrow(() -> new IllegalStateException("Attribute "
            + attrDesc.getName() + " has no random access reader"));

    DatasetBuilder<TypedIngest<Object>> builder = () -> {
      Dataset<TypedIngest<Object>> input = flow.get().createInput(BatchSource.of(
          family.getBatchObservable().get(),
          family,
          startStamp, endStamp));

      input = Filter.of(input)
          .by(i -> i.getStamp() >= startStamp && i.getStamp() < endStamp)
          .output();

      String prefix = attrDesc.toAttributePrefix();
      Dataset<TypedIngest<Object>> filtered = Filter.of(input)
          .by(t -> t.getAttrDesc().toAttributePrefix().equals(prefix))
          .output();
      return AssignEventTime.of(filtered)
          .using(TypedIngest::getStamp)
          .output();
    };

    return Stream.wrap(
        new InMemExecutor()
            .setTriggeringSchedulerSupplier(() -> {
              return new ProcessingTimeTriggerScheduler();
            })
            .setWatermarkEmitStrategySupplier(() -> new WatermarkEmitStrategy.Default()),
        (DatasetBuilder) builder,
        this::resetFlow).windowAll();

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
      throws NoSuchMethodException, IllegalAccessException,
          IllegalArgumentException, InvocationTargetException,
          ClassNotFoundException, InvalidProtocolBufferException, InterruptedException,
          TextFormat.ParseException {

    if (attrDesc.getSchemeURI().getScheme().equals("proto")) {
      String protoClass = attrDesc.getSchemeURI().getSchemeSpecificPart();
      Class<AbstractMessage> cls = Classpath.findClass(protoClass, AbstractMessage.class);
      byte[] payload = null;
      if (textFormat != null) {
        Method newBuilder = cls.getDeclaredMethod("newBuilder");
        Builder builder = (Builder) newBuilder.invoke(null);
        TextFormat.merge(textFormat, builder);
        payload = builder.build().toByteArray();
      }
      Set<AttributeFamilyDescriptor<?>> families = repo.getFamiliesForAttribute(attrDesc);
      OnlineAttributeWriter writer = families.stream()
          .filter(af -> af.getType() == StorageType.PRIMARY)
          .findAny()
          .orElse(families.stream().findAny().get())
          .getWriter()
          .get()
          .online();
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
          + attrDesc.getSchemeURI());
    }

  }

  public void delete(
      EntityDescriptor entityDesc, AttributeDescriptor<?> attrDesc,
      String key, String attribute) throws InterruptedException {

      Set<AttributeFamilyDescriptor<?>> families = repo.getFamiliesForAttribute(attrDesc);
      OnlineAttributeWriter writer = families.stream()
          .filter(af -> af.getType() == StorageType.PRIMARY)
          .findAny()
          .orElse(families.stream().findAny().get())
          .getWriter()
          .get()
          .online();
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
        .usePlaintext(true)
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
        .usePlaintext(true)
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

}
