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
package cz.o2.proxima.tools.groovy;

import com.google.common.collect.Streams;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc.RetrieveServiceBlockingStub;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.tools.groovy.internal.ProximaInterpreter;
import cz.o2.proxima.tools.io.ConsoleRandomReader;
import cz.o2.proxima.util.Classpath;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.Binding;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

/** This is the groovysh based console. */
@Slf4j
public class Console implements AutoCloseable {

  private static AtomicReference<Console> INSTANCE = new AtomicReference<>();

  public static final String INITIAL_STATEMENT = "def env = new Environment()";

  /**
   * This is supposed to be called only from the groovysh initialized in this main method.
   *
   * @return the singleton instance
   */
  public static final Console get() {
    return INSTANCE.get();
  }

  public static Console get(String[] args) {
    if (INSTANCE.get() == null) {
      synchronized (Console.class) {
        if (INSTANCE.get() == null) {
          INSTANCE.set(new Console(args));
        }
      }
    }
    return INSTANCE.get();
  }

  public static Console create(Config config, Repository repo) {
    INSTANCE.set(new Console(config, repo, new String[] {}));
    return INSTANCE.get();
  }

  public static Console create(Config config, Repository repo, String[] args) {
    INSTANCE.set(new Console(config, repo, args));
    return INSTANCE.get();
  }

  public static void main(String[] args) throws Exception {
    ClassLoader loader = new ToolsClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    Console console = Console.get(args);
    Binding binding = new Binding();
    console.runInputForwarding();
    console.setShell(
        new Groovysh(
            loader,
            binding,
            new IO(console.getInputStream(), System.out, System.err),
            null,
            null,
            new ProximaInterpreter(loader, binding)));
    Runtime.getRuntime().addShutdownHook(new Thread(console::close));
    console.createWrapperClass();
    console.runShell(INITIAL_STATEMENT);
    System.out.println();
    console.close();
  }

  final String[] args;
  final BlockingQueue<Byte> input = new LinkedBlockingDeque<>();
  @Getter final Repository repo;
  final List<ConsoleRandomReader> readers = new ArrayList<>();
  final Configuration conf;
  final Config config;
  final ExecutorService executor =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r);
            t.setName("input-forwarder");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(
                (thrd, err) -> log.error("Error in thread {}", thrd.getName(), err));
            return t;
          });
  StreamProvider streamProvider;
  @Getter final Optional<DirectDataOperator> direct;
  Groovysh shell;

  Console(String[] args) {
    this(getConfig(), args);
  }

  Console(Config config, String[] args) {
    this(config, Repository.of(config), args);
  }

  private Console(Config config, Repository repo, String[] args) {
    this.args = args;
    this.config = config;
    this.repo = repo;
    this.direct =
        repo.hasOperator("direct")
            ? Optional.of(repo.getOrCreateOperator(DirectDataOperator.class))
            : Optional.empty();
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);

    initializeStreamProvider();
    updateClassLoader();
  }

  private void setShell(Groovysh shell) {
    this.shell = shell;
  }

  public void createWrapperClass() throws Exception {
    updateClassLoader();
    ToolsClassLoader classLoader =
        (ToolsClassLoader) Thread.currentThread().getContextClassLoader();
    log.debug("Creating Environment class in classloader {}", classLoader);
    GroovyEnv.createWrapperInLoader(conf, repo, classLoader);
  }

  private void initializeStreamProvider() {
    ServiceLoader<StreamProvider> loader = ServiceLoader.load(StreamProvider.class);
    // sort possible test implementations on top
    streamProvider =
        Streams.stream(loader)
            .min(
                (a, b) -> {
                  String cls1 = a.getClass().getSimpleName();
                  String cls2 = b.getClass().getSimpleName();
                  if (cls1.startsWith("Test") ^ cls2.startsWith("Test")) {
                    if (cls1.startsWith("Test")) {
                      return -1;
                    }
                    return 1;
                  }
                  return cls1.compareTo(cls2);
                })
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Unable to find any StreamProvider in classpath. Please check dependencies. Looking for service implements '%s' interface.",
                            StreamProvider.class.getName())));
    log.info("Using {} as StreamProvider", streamProvider);
    streamProvider.init(repo, args == null ? new String[] {} : args);
  }

  private void updateClassLoader() {
    if (!(Thread.currentThread().getContextClassLoader() instanceof ToolsClassLoader)) {
      Thread.currentThread().setContextClassLoader(new ToolsClassLoader());
    }
  }

  private static Config getConfig() {
    return ConfigFactory.load().resolve();
  }

  @SuppressWarnings("unchecked")
  public <T> Stream<StreamElement> getStream(
      AttributeDescriptor<T> attrDesc, Position position, boolean stopAtCurrent) {

    return getStream(attrDesc, position, stopAtCurrent, false);
  }

  @SuppressWarnings("unchecked")
  public <T> Stream<StreamElement> getStream(
      AttributeDescriptor<T> attrDesc,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime) {

    return streamProvider.getStream(
        position, stopAtCurrent, eventTime, this::unboundedStreamInterrupt, attrDesc);
  }

  @SuppressWarnings("unchecked")
  public Stream<StreamElement> getUnionStream(
      Position position,
      boolean eventTime,
      boolean stopAtCurrent,
      AttributeDescriptorProvider<?>... descriptors) {

    List<AttributeDescriptor<?>> attrs =
        Arrays.stream(descriptors)
            .map(AttributeDescriptorProvider::desc)
            .collect(Collectors.toList());

    return streamProvider.getStream(
        position,
        stopAtCurrent,
        eventTime,
        this::unboundedStreamInterrupt,
        attrs.toArray(new AttributeDescriptor[attrs.size()]));
  }

  public WindowedStream<StreamElement> getBatchSnapshot(AttributeDescriptor<?> attrDesc) {

    return getBatchSnapshot(attrDesc, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<StreamElement> getBatchSnapshot(
      AttributeDescriptor<?> attrDesc, long fromStamp, long toStamp) {

    return streamProvider.getBatchSnapshot(
        fromStamp, toStamp, this::unboundedStreamInterrupt, attrDesc);
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<StreamElement> getBatchUpdates(
      long startStamp, long endStamp, AttributeDescriptorProvider<?>... attrs) {

    List<AttributeDescriptor<?>> attrList =
        Arrays.stream(attrs).map(AttributeDescriptorProvider::desc).collect(Collectors.toList());

    return streamProvider.getBatchUpdates(
        startStamp,
        endStamp,
        this::unboundedStreamInterrupt,
        attrList.toArray(new AttributeDescriptor[attrList.size()]));
  }

  public ConsoleRandomReader getRandomAccessReader(String entity) {
    if (!direct.isPresent()) {
      throw new IllegalStateException(
          "Can create random access reader with direct operator only. "
              + "Add runtime dependency.");
    }
    EntityDescriptor entityDesc = findEntityDescriptor(entity);
    ConsoleRandomReader reader = new ConsoleRandomReader(entityDesc, repo, direct.get());
    readers.add(reader);
    return reader;
  }

  public void put(
      EntityDescriptor entityDesc,
      AttributeDescriptor attrDesc,
      String key,
      String attribute,
      String textFormat)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
          ClassNotFoundException, InvalidProtocolBufferException, InterruptedException,
          TextFormat.ParseException {

    put(entityDesc, attrDesc, key, attribute, System.currentTimeMillis(), textFormat);
  }

  public void put(
      EntityDescriptor entityDesc,
      AttributeDescriptor attrDesc,
      String key,
      String attribute,
      long stamp,
      String textFormat)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
          ClassNotFoundException, InvalidProtocolBufferException, InterruptedException,
          TextFormat.ParseException {

    if (!direct.isPresent()) {
      throw new IllegalStateException(
          "Can write with direct operator only. Add runtime dependecncy");
    }
    if (attrDesc.getSchemeUri().getScheme().equals("proto")) {
      ValueSerializerFactory factory =
          repo.getValueSerializerFactory(attrDesc.getSchemeUri().getScheme())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Unable to get ValueSerializerFactory for attribute "
                              + attrDesc.getName()
                              + " with scheme "
                              + attrDesc.getSchemeUri().toString()
                              + "."));

      String protoClass = factory.getClassName(attrDesc.getSchemeUri());
      Class<? extends AbstractMessage> cls = Classpath.findClass(protoClass, AbstractMessage.class);
      byte[] payload = null;
      if (textFormat != null) {
        Method newBuilder = cls.getDeclaredMethod("newBuilder");
        Builder builder = (Builder) newBuilder.invoke(null);
        TextFormat.merge(textFormat, builder);
        payload = builder.build().toByteArray();
      }
      OnlineAttributeWriter writer =
          direct
              .get()
              .getWriter(attrDesc)
              .orElseThrow(() -> new IllegalArgumentException("Missing writer for " + attrDesc));
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<Throwable> exc = new AtomicReference<>();
      writer.write(
          StreamElement.upsert(
              entityDesc, attrDesc, UUID.randomUUID().toString(), key, attribute, stamp, payload),
          (success, ex) -> {
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
          "Don't know how to make builder for " + attrDesc.getSchemeUri());
    }
  }

  public void delete(
      EntityDescriptor entityDesc, AttributeDescriptor<?> attrDesc, String key, String attribute)
      throws InterruptedException {

    delete(entityDesc, attrDesc, key, attribute, System.currentTimeMillis());
  }

  public void delete(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attrDesc,
      String key,
      String attribute,
      long stamp)
      throws InterruptedException {

    if (!direct.isPresent()) {
      throw new IllegalStateException(
          "Can write with direct operator only. Add runtime dependency");
    }
    OnlineAttributeWriter writer =
        direct
            .get()
            .getWriter(attrDesc)
            .orElseThrow(() -> new IllegalArgumentException("Missing writer for " + attrDesc));
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> exc = new AtomicReference<>();
    final StreamElement delete;
    if (attrDesc.isWildcard() && attribute.equals(attrDesc.getName())) {
      delete =
          StreamElement.deleteWildcard(
              entityDesc, attrDesc, UUID.randomUUID().toString(), key, stamp);
    } else {
      delete =
          StreamElement.delete(
              entityDesc, attrDesc, UUID.randomUUID().toString(), key, attribute, stamp);
    }
    writer.write(
        delete,
        (success, ex) -> {
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
    return repo.findEntity(entity)
        .orElseThrow(() -> new IllegalArgumentException("Entity " + entity + " not found"));
  }

  public Rpc.ListResponse rpcList(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor wildcard,
      String offset,
      int limit,
      String host,
      int port) {

    Channel channel =
        ManagedChannelBuilder.forAddress(host, port).directExecutor().usePlaintext().build();

    RetrieveServiceBlockingStub stub = RetrieveServiceGrpc.newBlockingStub(channel);
    return stub.listAttributes(
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setKey(key)
            .setWildcardPrefix(wildcard.toAttributePrefix(false))
            .setOffset(offset)
            .setLimit(limit)
            .build());
  }

  public Rpc.GetResponse rpcGet(
      EntityDescriptor entity, String key, String attr, String host, int port) {

    Channel channel =
        ManagedChannelBuilder.forAddress(host, port).directExecutor().usePlaintext().build();

    RetrieveServiceBlockingStub stub = RetrieveServiceGrpc.newBlockingStub(channel);
    return stub.get(
        Rpc.GetRequest.newBuilder()
            .setEntity(entity.getName())
            .setAttribute(attr)
            .setKey(key)
            .build());
  }

  @Override
  public void close() {
    readers.forEach(ConsoleRandomReader::close);
    if (streamProvider != null) {
      streamProvider.close();
    }
    executor.shutdownNow();
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
    executor.execute(
        () -> {
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

  private void runShell(String script) {
    this.shell.run(script);
  }
}
