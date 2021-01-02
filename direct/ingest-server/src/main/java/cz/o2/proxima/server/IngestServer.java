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
package cz.o2.proxima.server;

import static cz.o2.proxima.server.Constants.CFG_NUM_THREADS;
import static cz.o2.proxima.server.Constants.DEFAULT_NUM_THREADS;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.StreamElement;
import io.grpc.ServerBuilder;
import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

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
    final IngestServer server;

    if (args.length == 0) {
      server = new IngestServer(ConfigFactory.load().resolve());
    } else {
      server = new IngestServer(ConfigFactory.parseFile(new File(args[0])).resolve());
    }
    server.run();
  }

  @Getter final Executor executor;

  @Getter final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(5);

  @Getter final Repository repo;
  @Getter final DirectDataOperator direct;
  @Getter final Config cfg;
  @Getter final boolean ignoreErrors;

  @Getter
  RetryPolicy retryPolicy =
      new RetryPolicy().withMaxRetries(3).withBackoff(3000, 20000, TimeUnit.MILLISECONDS, 2.0);

  protected IngestServer(Config cfg) {
    this(cfg, false);
  }

  @VisibleForTesting
  IngestServer(Config cfg, boolean test) {
    this.cfg = cfg;
    repo = test ? Repository.ofTest(cfg) : Repository.of(cfg);
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
    executor = createExecutor(cfg);
  }

  static boolean ingestRequest(
      DirectDataOperator direct,
      StreamElement ingest,
      String uuid,
      Consumer<Rpc.Status> responseConsumer) {

    EntityDescriptor entityDesc = ingest.getEntityDescriptor();
    AttributeDescriptor<?> attributeDesc = ingest.getAttributeDescriptor();

    OnlineAttributeWriter writer =
        direct
            .getWriter(attributeDesc)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Writer for attribute " + attributeDesc.getName() + " not found"));

    if (writer == null) {
      log.warn("Missing writer for request {}", ingest);
      responseConsumer.accept(
          status(uuid, 503, "No writer for attribute " + attributeDesc.getName()));
      return false;
    }

    boolean valid =
        ingest.isDelete() /* delete is always valid */
            || attributeDesc.getValueSerializer().isValid(ingest.getValue());

    if (!valid) {
      log.info("Request {} is not valid", ingest);
      responseConsumer.accept(
          status(
              uuid,
              412,
              "Invalid scheme for " + entityDesc.getName() + "." + attributeDesc.getName()));
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
    // write the ingest into the commit log and confirm to the client
    log.debug("Writing {} to commit log {}", ingest, writer.getUri());
    writer.write(
        ingest,
        (s, exc) -> {
          if (s) {
            responseConsumer.accept(ok(uuid));
          } else {
            log.warn("Failed to write {}", ingest, exc);
            responseConsumer.accept(status(uuid, 500, exc.toString()));
          }
        });
    return true;
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
  private void run() {
    final int port =
        cfg.hasPath(Constants.CFG_PORT) ? cfg.getInt(Constants.CFG_PORT) : Constants.DEFAULT_PORT;
    io.grpc.Server server =
        ServerBuilder.forPort(port)
            .executor(executor)
            .addService(new IngestService(repo, direct, scheduler))
            .addService(new RetrieveService(repo, direct))
            .build();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Gracefully shutting server down.");
                  server.shutdown();
                }));

    try {
      server.start();
      log.info("Successfully started server 0.0.0.0:{}", server.getPort());
      Metrics.LIVENESS.increment(1.0);
      server.awaitTermination();
      log.info("Server shutdown.");
    } catch (Exception ex) {
      Utils.die("Failed to start the server", ex);
    }
    Metrics.LIVENESS.reset();
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
}
