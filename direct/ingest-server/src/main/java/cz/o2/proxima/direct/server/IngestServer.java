/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.server;

import static cz.o2.proxima.direct.server.Constants.CFG_NUM_THREADS;
import static cz.o2.proxima.direct.server.Constants.DEFAULT_NUM_THREADS;

import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.ConfigConstants;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.server.metrics.Metrics;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc;
import cz.o2.proxima.direct.server.transaction.TransactionContext;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import dev.failsafe.RetryPolicy;
import io.grpc.BindableService;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** The ingestion server. */
@Slf4j
public class IngestServer {

  /**
   * Run the server.
   *
   * @param args command line arguments
   * @throws Throwable on error
   */
  public static void main(String[] args) throws Throwable {
    runWithServerFactory(() -> new IngestServer(getCfgFromArgs(args)));
  }

  protected static Config getCfgFromArgs(String[] args) {
    return args.length == 0
        ? ConfigFactory.load().resolve()
        : ConfigFactory.parseFile(new File(args[0])).resolve();
  }

  protected static void runWithServerFactory(Factory<? extends IngestServer> serverFactory) {
    serverFactory.apply().run();
  }

  @Getter final Executor executor;

  @Getter final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(5);

  @Getter final Repository repo;
  @Getter final DirectDataOperator direct;
  @Getter final Config cfg;
  @Getter final boolean ignoreErrors;
  @Getter @Nullable final TransactionContext transactionContext;

  @Getter
  RetryPolicy<Void> retryPolicy =
      RetryPolicy.<Void>builder()
          .withMaxRetries(3)
          .withBackoff(Duration.ofSeconds(3), Duration.ofSeconds(20), 2.0)
          .build();

  protected IngestServer(Config cfg) {
    this(cfg, false);
  }

  @VisibleForTesting
  IngestServer(Config cfg, boolean test) {
    this(cfg, test ? Repository.ofTest(cfg) : Repository.of(cfg));
  }

  protected IngestServer(Config cfg, Repository repo) {
    // this will force load of class from different module layer, if needed
    Preconditions.checkState(repo.hasOperator("direct"), "Missing direct operator");
    this.cfg = cfg;
    this.repo = repo;
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    if (log.isDebugEnabled()) {
      repo.getAllEntities()
          .forEach(
              e -> e.getAllAttributes(true).forEach(a -> log.debug("Configured attribute {}", a)));
    }
    if (repo.isEmpty()) {
      throw new IllegalArgumentException("No valid entities found in provided config!");
    }
    this.ignoreErrors =
        cfg.hasPath(Constants.CFG_IGNORE_ERRORS) && cfg.getBoolean(Constants.CFG_IGNORE_ERRORS);
    this.transactionContext =
        direct.getRepository().findEntity(ConfigConstants.TRANSACTION_ENTITY).isPresent()
            ? new TransactionContext(direct)
            : null;
    executor = createExecutor(cfg);
  }

  static boolean ingestRequest(
      DirectDataOperator direct,
      StreamElement ingest,
      String uuid,
      Consumer<Rpc.Status> responseConsumer) {

    long start = System.currentTimeMillis();
    AttributeDescriptor<?> attributeDesc = ingest.getAttributeDescriptor();

    OnlineAttributeWriter writer = getWriterForAttributeInTransform(direct, attributeDesc);

    if (writer == null) {
      log.warn("Missing writer for request {}", ingest);
      responseConsumer.accept(
          status(uuid, 503, "No writer for attribute " + attributeDesc.getName()));
      return false;
    }

    if (ingest.isDelete()) {
      if (ingest.isDeleteWildcard()) {
        Metrics.DELETE_WILDCARD_REQUESTS.increment();
      } else {
        Metrics.DELETE_REQUESTS.increment();
      }
    } else {
      Metrics.UPDATE_REQUESTS.increment();
    }

    Metrics.COMMIT_LOG_APPEND.increment();
    // write the element into the commit log and confirm to the client
    log.debug("Writing {} to commit log {}", ingest, writer.getUri());
    writer.write(
        ingest,
        (s, exc) -> {
          Metrics.INGEST_LATENCY.increment(System.currentTimeMillis() - start);
          if (s) {
            responseConsumer.accept(ok(uuid));
          } else {
            log.warn("Failed to write {}", ingest, exc);
            responseConsumer.accept(status(uuid, 500, exc.toString()));
          }
        });
    return true;
  }

  private static OnlineAttributeWriter getWriterForAttributeInTransform(
      DirectDataOperator direct, AttributeDescriptor<?> attributeDesc) {

    return direct
        .getWriter(attributeDesc)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Writer for attribute " + attributeDesc.getName() + " not found"));
  }

  static Rpc.Status notFound(String uuid, String what) {
    return Rpc.Status.newBuilder().setUuid(uuid).setStatus(404).setStatusMessage(what).build();
  }

  static Rpc.Status ok(String uuid) {
    return Rpc.Status.newBuilder().setStatus(200).setUuid(uuid).build();
  }

  static Rpc.Status status(String uuid, int status, String message) {
    return Rpc.Status.newBuilder()
        .setUuid(uuid)
        .setStatus(status)
        .setStatusMessage(message)
        .build();
  }

  protected Executor createExecutor(Config cfg) {
    int numThreads =
        cfg.hasPath(CFG_NUM_THREADS) ? cfg.getInt(CFG_NUM_THREADS) : DEFAULT_NUM_THREADS;
    return new ThreadPoolExecutor(
        numThreads, numThreads, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
  }

  /** Run the server. */
  protected void run() {
    final int port =
        cfg.hasPath(Constants.CFG_PORT) ? cfg.getInt(Constants.CFG_PORT) : Constants.DEFAULT_PORT;

    io.grpc.Server server = createServer(port);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Gracefully shutting server down.");
                  server.shutdown();
                }));

    try {
      server.start();
      Optional.ofNullable(transactionContext).ifPresent(TransactionContext::run);
      log.info("Successfully started server 0.0.0.0:{}", server.getPort());
      Metrics.LIVENESS.increment(1.0);
      server.awaitTermination();
      log.info("Server shutdown.");
    } catch (Exception ex) {
      log.error("Failed to start the server", ex);
    } finally {
      Optional.ofNullable(transactionContext).ifPresent(TransactionContext::close);
    }
    Metrics.LIVENESS.reset();
  }

  private Server createServer(int port) {
    return createServer(port, log.isDebugEnabled());
  }

  protected Server createServer(int port, boolean debug) {
    ServerBuilder<?> builder = ServerBuilder.forPort(port).executor(executor);
    getServices().forEach(builder::addService);
    if (debug) {
      builder = builder.intercept(new IngestServerInterceptor());
    }
    return builder.build();
  }

  protected Iterable<BindableService> getServices() {
    return Arrays.asList(
        new IngestService(repo, direct, transactionContext, scheduler),
        new RetrieveService(repo, direct, transactionContext));
  }

  @VisibleForTesting
  void runReplications() {
    final ReplicationController replicationController = ReplicationController.of(repo);
    replicationController
        .runReplicationThreads()
        .whenComplete(
            (success, error) -> {
              if (error != null) {
                Utils.die(error.getMessage(), error);
              }
            });
  }

  private static class IngestServerInterceptor implements io.grpc.ServerInterceptor {

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> serverCall,
        Metadata metadata,
        ServerCallHandler<ReqT, RespT> serverCallHandler) {

      log.debug(
          "Received call {} with attributes {} and metadata {}",
          serverCall.getMethodDescriptor(),
          serverCall.getAttributes(),
          metadata);
      return serverCallHandler.startCall(serverCall, metadata);
    }
  }
}
