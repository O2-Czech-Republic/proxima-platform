/*
 * Copyright 2017-2026 O2 Czech Republic, a.s.
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

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.scheme.ValueSerializer;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Streams;
import cz.o2.proxima.tools.io.ConsoleRandomReader;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.Closure;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShellRunnable {

  private static ShellRunnable INSTANCE;

  @Getter protected final Repository repo;
  protected final Configuration conf;
  protected final List<ConsoleRandomReader> readers = new ArrayList<>();
  @Nullable protected final DirectDataOperator direct;
  private final String[] args;
  private final ClassLoader previous;

  StreamProvider streamProvider;

  public ShellRunnable(Repository repo, String[] args) {
    Preconditions.checkState(INSTANCE == null, "Do not construct ShellRunnable multiple times");
    this.repo = repo;
    this.args = args;
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
    this.direct =
        repo.hasOperator("direct") ? repo.getOrCreateOperator(DirectDataOperator.class) : null;
    INSTANCE = this;

    this.previous = Thread.currentThread().getContextClassLoader();

    initializeStreamProvider();
    updateClassLoader();
  }

  public Class<? extends RepositoryProvider> createWrapperClass() throws Exception {
    updateClassLoader();
    ToolsClassLoader classLoader =
        (ToolsClassLoader) Thread.currentThread().getContextClassLoader();
    log.debug("Creating Environment class in classloader {}", classLoader);
    return GroovyEnv.createWrapperInLoader(conf, repo, classLoader);
  }

  void initializeStreamProvider() {
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

  void updateClassLoader() {
    if (!(Thread.currentThread().getContextClassLoader() instanceof ToolsClassLoader)) {
      Thread.currentThread().setContextClassLoader(new ToolsClassLoader());
    }
  }

  /**
   * This is supposed to be called only from the groovysh initialized in this main method.
   *
   * @return the singleton instance
   */
  public static ShellRunnable get() {
    return INSTANCE;
  }

  public <T> Stream<StreamElement> getStream(
      AttributeDescriptor<T> attrDesc, Position position, boolean stopAtCurrent) {

    return getStream(attrDesc, position, stopAtCurrent, false);
  }

  public <T> Stream<StreamElement> getStream(
      AttributeDescriptor<T> attrDesc,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime) {

    return streamProvider.getStream(
        position, stopAtCurrent, eventTime, this::unboundedStreamInterrupt, attrDesc);
  }

  public Stream<StreamElement> getUnionStream(
      Position position,
      boolean eventTime,
      boolean stopAtCurrent,
      AttributeDescriptorProvider<?>... descriptors) {

    return streamProvider.getStream(
        position,
        stopAtCurrent,
        eventTime,
        this::unboundedStreamInterrupt,
        Arrays.stream(descriptors)
            .distinct()
            .map(AttributeDescriptorProvider::desc)
            .toArray(AttributeDescriptor[]::new));
  }

  public WindowedStream<StreamElement> getBatchSnapshot(AttributeDescriptor<?> attrDesc) {
    return getBatchSnapshot(attrDesc, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public WindowedStream<StreamElement> getBatchSnapshot(
      AttributeDescriptor<?> attrDesc, long fromStamp, long toStamp) {

    return streamProvider.getBatchSnapshot(
        fromStamp, toStamp, this::unboundedStreamInterrupt, attrDesc);
  }

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

  public <T> WindowedStream<T> getImpulse(@Nullable String name, Closure<T> factory) {
    return streamProvider.impulse(factory);
  }

  public <T> WindowedStream<T> getPeriodicImpulse(
      @Nullable String name, Closure<T> factory, long durationMs) {
    return streamProvider.periodicImpulse(factory, durationMs);
  }

  public ConsoleRandomReader getRandomAccessReader(String entity) {
    Preconditions.checkState(
        direct != null,
        "Can create random access reader with direct operator only. Add runtime dependency.");
    EntityDescriptor entityDesc = findEntityDescriptor(entity);
    ConsoleRandomReader reader = new ConsoleRandomReader(entityDesc, direct);
    readers.add(reader);
    return reader;
  }

  public void put(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attrDesc,
      String key,
      String attribute,
      String value)
      throws InterruptedException {

    put(entityDesc, attrDesc, key, attribute, System.currentTimeMillis(), value);
  }

  public void put(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attrDesc,
      String key,
      String attribute,
      long stamp,
      String value)
      throws InterruptedException {

    Preconditions.checkState(
        direct != null, "Can write with direct operator only. Add runtime dependency");

    @SuppressWarnings("unchecked")
    ValueSerializer<Object> valueSerializer =
        (ValueSerializer<Object>) attrDesc.getValueSerializer();
    byte[] payload = valueSerializer.serialize(valueSerializer.fromJsonValue(value));
    OnlineAttributeWriter writer =
        Optionals.get(direct.getWriter(attrDesc), "Cannot find writer for attribute %s", attrDesc);
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

    Preconditions.checkState(
        direct != null, "Can write with direct operator only. Add runtime dependency");
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(attrDesc));
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

  public Optional<DirectDataOperator> getDirect() {
    return Optional.ofNullable(direct);
  }

  public EntityDescriptor findEntityDescriptor(String entity) {
    return repo.getEntity(entity);
  }

  void close() {
    Optional.ofNullable(streamProvider).ifPresent(StreamProvider::close);
    readers.forEach(ConsoleRandomReader::close);
    readers.clear();
    Optional.ofNullable(direct).ifPresent(DirectDataOperator::close);
    Thread.currentThread().setContextClassLoader(previous);
    // destroy the instance
    INSTANCE = null;
  }

  boolean unboundedStreamInterrupt() {
    return false;
  }
}
