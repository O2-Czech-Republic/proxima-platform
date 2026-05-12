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
import cz.o2.proxima.direct.server.rpc.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.direct.server.rpc.proto.service.RetrieveServiceGrpc.RetrieveServiceBlockingStub;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.tools.groovy.internal.ProximaInterpreter;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import groovy.lang.Binding;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.extern.slf4j.Slf4j;
import org.apache.groovy.groovysh.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

/** This is the groovysh based console. */
@Slf4j
public class Console extends ShellRunnable implements AutoCloseable {

  public static final String INITIAL_STATEMENT = "env = new Environment()";

  public static Console get(String[] args) {
    if (ShellRunnable.get() == null) {
      return new Console(args);
    }
    Preconditions.checkState(ShellRunnable.get() instanceof Console);
    return (Console) ShellRunnable.get();
  }

  public static Console create(Config config, Repository repo) {
    return new Console(config, repo, new String[] {});
  }

  public static Console create(Config config, Repository repo, String[] args) {
    return new Console(config, repo, args);
  }

  public static void main(String[] args) throws Exception {
    try (Console console = Console.get(args)) {
      console.run();
      System.out.println();
    }
  }

  private final BlockingQueue<Integer> input = new LinkedBlockingDeque<>();
  private final Config config;
  private final ExecutorService executor =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r);
            t.setName("input-forwarder");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(
                (thrd, err) -> log.error("Error in thread {}", thrd.getName(), err));
            return t;
          });
  Groovysh shell;

  Console(String[] args) {
    this(getConfig(), args);
  }

  Console(Config config, String[] args) {
    this(config, Repository.of(config), args);
  }

  @VisibleForTesting
  Console(Config config, Repository repo, String[] args) {
    super(repo, args);
    this.config = config;
  }

  @VisibleForTesting
  void run() throws Exception {
    ToolsClassLoader loader = new ToolsClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    Binding binding = new Binding();
    runInputForwarding();
    setShell(
        new Groovysh(
            loader,
            binding,
            new IO(getInputStream(), System.out, System.err),
            null,
            loader.getConfiguration(),
            new ProximaInterpreter(loader, binding, loader.getConfiguration())));
    Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    createWrapperClass();
    runShell(INITIAL_STATEMENT);
  }

  private void setShell(Groovysh shell) {
    this.shell = shell;
  }

  private static Config getConfig() {
    return ConfigFactory.load().resolve();
  }

  public Rpc.ListResponse rpcList(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> wildcard,
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
    log.debug("Console shutting down.");
    super.close();
    executor.shutdownNow();
    input.clear();
    Preconditions.checkState(input.offer(-1));
  }

  @Override
  boolean unboundedStreamInterrupt() {
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
              int next = nextInputByte();
              Preconditions.checkState(input.offer(next));
              if (next < 0) {
                break;
              }
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
        });
  }

  @VisibleForTesting
  int nextInputByte() throws IOException {
    return System.in.read();
  }

  private InputStream getInputStream() {
    return new InputStream() {

      boolean finished = false;

      @Override
      public int read() {
        try {
          if (finished) {
            return -1;
          }
          int next = input.take();
          if (next < 0) {
            finished = true;
          }
          return next;
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          finished = true;
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

  @VisibleForTesting
  ToolsClassLoader getToolsClassLoader() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Preconditions.checkState(loader instanceof ToolsClassLoader);
    return (ToolsClassLoader) loader;
  }
}
