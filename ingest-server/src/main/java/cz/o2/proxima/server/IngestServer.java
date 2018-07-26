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
package cz.o2.proxima.server;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.transform.Transformation;
import cz.o2.proxima.repository.TransformationDescriptor;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.BulkAttributeWriter;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.AbstractRetryableLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.RetryableBulkObserver;
import cz.o2.proxima.storage.commitlog.RetryableLogObserver;
import cz.o2.proxima.util.Pair;
import io.grpc.ServerBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import javax.annotation.Nullable;
import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The ingestion server.
 */
@Slf4j
public class IngestServer {

  /**
   * Run the server.
   * @param args command line arguments
   * @throws Throwable on error
   */
  public static void main(String[] args) throws Throwable {
    final IngestServer server;

    if (args.length == 0) {
      server = new IngestServer(ConfigFactory.load().resolve());
    } else {
      server = new IngestServer(ConfigFactory.parseFile(
          new File(args[0])).resolve());
    }
    server.run();
  }


  @Getter
  static final int MIN_CORES = 2;
  @Getter
  final Executor executor = new ThreadPoolExecutor(
      MIN_CORES,
            10 * MIN_CORES,
            10, TimeUnit.SECONDS,
            new SynchronousQueue<>());

  @Getter
  final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(5);

  @Getter
  final Repository repo;
  @Getter
  final Config cfg;
  @Getter
  final boolean ignoreErrors;

  @Getter
  RetryPolicy retryPolicy = new RetryPolicy()
      .withMaxRetries(3)
      .withBackoff(3000, 20000, TimeUnit.MILLISECONDS, 2.0);

  protected IngestServer(Config cfg) {
    this.cfg = cfg;
    repo = Repository.of(cfg);
    if (log.isDebugEnabled()) {
      repo.getAllEntities()
          .forEach(e -> e.getAllAttributes(true)
              .stream()
              .forEach(a -> log.debug("Configured attribute {}", a)));
    }
    if (repo.isEmpty()) {
      throw new IllegalArgumentException(
          "No valid entities found in provided config!");
    }
    this.ignoreErrors =
        cfg.hasPath(Constants.CFG_IGNORE_ERRORS)
            && cfg.getBoolean(Constants.CFG_IGNORE_ERRORS);
  }


  static boolean ingestRequest(
      Repository repo,
      StreamElement ingest, String uuid,
      Consumer<Rpc.Status> responseConsumer) {

    EntityDescriptor entityDesc = ingest.getEntityDescriptor();
    AttributeDescriptor attributeDesc = ingest.getAttributeDescriptor();

    OnlineAttributeWriter writer = repo.getWriter(attributeDesc)
        .orElseThrow(() ->
            new IllegalStateException(
                "Writer for attribute " + attributeDesc.getName() + " not found"));

    if (writer == null) {
      log.warn("Missing writer for request {}", ingest);
      responseConsumer.accept(
          status(uuid, 503, "No writer for attribute "
              + attributeDesc.getName()));
      return false;
    }

    boolean valid = ingest.isDelete() /* delete is always valid */
        || attributeDesc.getValueSerializer().isValid(ingest.getValue());

    if (!valid) {
      log.info("Request {} is not valid", ingest);
      responseConsumer.accept(status(uuid, 412, "Invalid scheme for "
          + entityDesc.getName() + "." + attributeDesc.getName()));
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
    writer.write(ingest, (s, exc) -> {
      if (s) {
        responseConsumer.accept(ok(uuid));
      } else {
        responseConsumer.accept(status(uuid, 500, exc.getMessage()));
      }
    });
    return true;
  }

  static Rpc.Status notFound(String uuid, String what) {
    return Rpc.Status.newBuilder()
        .setUuid(uuid)
        .setStatus(404)
        .setStatusMessage(what)
        .build();
  }

  static Rpc.Status ok(String uuid) {
    return Rpc.Status.newBuilder()
        .setStatus(200)
        .setUuid(uuid)
        .build();
  }

  static Rpc.Status status(String uuid, int status, String message) {
    return Rpc.Status.newBuilder()
        .setUuid(uuid)
        .setStatus(status)
        .setStatusMessage(message)
        .build();
  }

  /** Run the server. */
  private void run() {
    final int port = cfg.hasPath(Constants.CFG_PORT)
        ? cfg.getInt(Constants.CFG_PORT)
        : Constants.DEFALT_PORT;
    io.grpc.Server server = ServerBuilder.forPort(port)
        .executor(executor)
        .addService(new IngestService(repo, scheduler))
        .addService(new RetrieveService(repo))
        .build();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Gracefully shuting down server.");
      server.shutdown();
    }));
    Metrics.register();
    startConsumerThreads();
    try {
      server.start();
      log.info("Successfully started server 0.0.0.0:{}", server.getPort());
      server.awaitTermination();
      log.info("Server shutdown.");
    } catch (Exception ex) {
      die("Failed to start the server", ex);
    }
  }


  /**
   * Start all threads that will be consuming the commit log and write to the output.
   **/
  protected void startConsumerThreads() {

    // index the repository
    Map<AttributeFamilyDescriptor, Set<AttributeFamilyDescriptor>> familyToCommitLog;
    familyToCommitLog = indexFamilyToCommitLogs();

    log.info("Starting consumer threads for familyToCommitLog {}", familyToCommitLog);
    // execute threads to consume the commit log
    familyToCommitLog.forEach((family, logs) -> {
      for (AttributeFamilyDescriptor commitLogFamily : logs) {
        if (!family.getAccess().isReadonly()) {
          CommitLogReader commitLog = commitLogFamily.getCommitLogReader()
              .orElseThrow(() -> new IllegalStateException(
                  "Failed to find commit-log reader in family " + commitLogFamily));
          AttributeWriterBase writer = family.getWriter()
              .orElseThrow(() ->
                  new IllegalStateException(
                      "Unable to get writer for family " + family.getName() + "."));
          StorageFilter filter = family.getFilter();
          Set<AttributeDescriptor<?>> allowedAttributes =
              new HashSet<>(family.getAttributes());
          final String name = "consumer-" + family.getName();
          registerWriterTo(name, commitLog, allowedAttributes, filter,
              writer, retryPolicy);
          log.info(
              "Started consumer {} consuming from log {} with URI {} into {} "
                  + "attributes {}",
              name, commitLog, commitLog.getUri(), writer.getUri(), allowedAttributes);
        } else {
          log.debug("Not starting thread for read-only family {}", family);
        }
      }
    });

    // execute transformer threads
    repo.getTransformations().forEach(this::runTransformer);
  }

  private void runTransformer(String name, TransformationDescriptor transform) {
    AttributeFamilyDescriptor family = transform.getAttributes()
        .stream()
        .map(a -> this.repo.getFamiliesForAttribute(a)
            .stream().filter(af -> af.getAccess().canReadCommitLog())
            .collect(Collectors.toSet()))
        .reduce(Sets::intersection)
        .filter(s -> !s.isEmpty())
        .map(s -> s.stream()
            .filter(f -> f.getCommitLogReader().isPresent())
            .findAny()
            .orElse(null))
        .filter(Objects::nonNull)
        .orElseThrow(() ->
            new IllegalArgumentException(
            "Cannot obtain attribute family for " + transform.getAttributes()));

    Transformation t = transform.getTransformation();
    StorageFilter f = transform.getFilter();
    final String consumer = "transformer-" + name;
    CommitLogReader reader = family.getCommitLogReader()
        .orElseThrow(() ->
            new IllegalStateException(
                "Unable to get reader for family " + family.getName() + "."));

    startTransformationObserver(consumer, reader, t, f, name);
    log.info(
        "Started transformer {} reading from {} using {}",
        consumer, reader.getUri(), t.getClass());
  }

  private void startTransformationObserver(
      String consumer, CommitLogReader reader,
      Transformation transformation, StorageFilter filter, String name) {

    new TransformationObserver(
        3, consumer, reader, repo,
        name, transformation, filter).start();
  }

  /**
   * Retrieve attribute family and it's associated commit log(s).
   * The families returned are only those which are not used as commit log
   * themselves.
   */
  @SuppressWarnings("unchecked")
  private Map<AttributeFamilyDescriptor, Set<AttributeFamilyDescriptor>>
      indexFamilyToCommitLogs() {

    // each attribute and its associated primary family
    final Map<AttributeDescriptor, AttributeFamilyDescriptor> attrToCommitLog;
    attrToCommitLog = repo.getAllFamilies()
        .filter(af -> af.getType() == StorageType.PRIMARY)
        // take pair of attribute to associated commit log
        .flatMap(af -> af.getAttributes()
            .stream()
            .map(attr -> Pair.of(attr, af)))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    return (Map) repo.getAllFamilies()
        .filter(af -> af.getType() == StorageType.REPLICA)
        // map to pair of attribute family and associated commit log(s) via attributes
        .map(af -> {
          if (af.getSource().isPresent()) {
            String source = af.getSource().get();
            return Pair.of(af, Collections.singleton(repo
                .getAllFamilies()
                .filter(af2 -> af2.getName().equals(source))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(
                    "Unknown family " + source))));
          }
          return Pair.of(af,
            af.getAttributes()
                .stream()
                .map(attr -> {
                  AttributeFamilyDescriptor commitFamily = attrToCommitLog.get(attr);
                  Optional<OnlineAttributeWriter> writer = repo.getWriter(attr);
                  if (commitFamily == null && writer.isPresent()) {
                    throw new IllegalStateException(
                        "Missing source commit log family for " + attr);
                  }
                  return commitFamily;
                })
              .filter(Objects::nonNull)
              .collect(Collectors.toSet()));
        })
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private void registerWriterTo(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      AttributeWriterBase writerBase,
      RetryPolicy retry) {

    AbstractRetryableLogObserver observer;
    log.info(
        "Registering {} writer to {} from commit log {}",
        writerBase.getType(), writerBase.getUri(), commitLog.getUri());

    if (writerBase.getType() == AttributeWriterBase.Type.ONLINE) {
      OnlineAttributeWriter writer = writerBase.online();
      observer = getOnlineObserver(
          consumerName, commitLog, allowedAttributes, filter, writer);
    } else {
      BulkAttributeWriter writer = writerBase.bulk();
      observer = getBulkObserver(
          consumerName, commitLog, allowedAttributes, filter, writer, retry);
    }

    observer.start();

  }

  private AbstractRetryableLogObserver getBulkObserver(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      BulkAttributeWriter writer,
      RetryPolicy retry) {

    return new RetryableBulkObserver<Serializable>(3, consumerName, commitLog) {

      @Override
      public boolean onNextInternal(
          StreamElement ingest,
          OffsetCommitter committer) {

        final boolean allowed = allowedAttributes.contains(
            ingest.getAttributeDescriptor());
        log.debug(
            "Consumer {}: received new ingest element {}", consumerName, ingest);
        if (allowed && filter.apply(ingest)) {
          Failsafe.with(retry).run(() -> ingestBulkInternal(ingest, committer));
        } else {
          Metrics.COMMIT_UPDATE_DISCARDED.increment();
          log.debug(
              "Consumer {]: discarding write of {} to {} because of {}, "
                  + "with allowedAttributes {} and filter class {}",
              consumerName, ingest, writer.getUri(),
              allowed ? "applied filter" : "invalid attribute",
              allowedAttributes, filter.getClass());
        }
        return true;
      }

      @Override
      protected void failure() {
        die(String.format(
            "Consumer %s: too many errors retrying the consumption of "
                + "commit log %s. Killing self.",
            consumerName, commitLog.getUri()));
      }

      @Override
      public void onRestart(List<Offset> offsets) {
        log.info(
            "Consumer {}: restarting bulk processing of {} from {}, "
                + "rollbacking the writer",
            consumerName, writer.getUri(), offsets);
        writer.rollback();
      }

      private void ingestBulkInternal(
          StreamElement ingest,
          OffsetCommitter committer) {

        log.debug(
            "Consumer {}: writing element {} into {}",
            consumerName, ingest, writer);

        writer.write(ingest, (succ, exc) -> confirmWrite(
            consumerName, ingest, writer, succ, exc,
            committer::confirm, committer::fail));
      }

    };
  }

  private AbstractRetryableLogObserver getOnlineObserver(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      OnlineAttributeWriter writer) {

    return new RetryableLogObserver(3, consumerName, commitLog) {

      @Override
      public boolean onNextInternal(
          StreamElement ingest, OffsetCommitter committer) {

        final boolean allowed = allowedAttributes.contains(
            ingest.getAttributeDescriptor());
        log.debug(
            "Consumer {}: received new stream element {}", consumerName, ingest);
        if (allowed && filter.apply(ingest)) {
          Failsafe.with(retryPolicy).run(
              () -> ingestOnlineInternal(ingest, committer));
        } else {
          Metrics.COMMIT_UPDATE_DISCARDED.increment();
          log.debug(
              "Consumer {}: discarding write of {} to {} because of {}, "
                  + "with allowedAttributes {} and filter class {}",
              consumerName, ingest, writer.getUri(),
              allowed ? "applied filter" : "invalid attribute",
              allowedAttributes, filter.getClass());
          committer.confirm();
        }
        return true;
      }

      @Override
      protected void failure() {
        die(String.format(
            "Consumer %s: too many errors retrying the consumption of commit "
                + "log %s. Killing self.",
            consumerName, commitLog.getUri()));
      }

      private void ingestOnlineInternal(
          StreamElement ingest, OffsetCommitter committer) {

        log.debug(
            "Consumer {}: writing element {} into {}",
            consumerName, ingest, writer);
        writer.write(ingest, (success, exc) -> confirmWrite(
            consumerName, ingest, writer, success, exc,
            committer::confirm, committer::fail));
      }

    };
  }

  private void confirmWrite(
      String consumerName,
      StreamElement ingest,
      AttributeWriterBase writer,
      boolean success, Throwable exc,
      Runnable onSuccess,
      Consumer<Throwable> onError) {

    if (!success) {
      log.error(
          "Consumer {}: failed to write ingest {} to {}",
          consumerName, ingest, writer.getUri(), exc);
      Metrics.NON_COMMIT_WRITES_RETRIES.increment();
      if (ignoreErrors) {
        log.error(
            "Consumer {}: retries exhausted trying to ingest {} to {}. "
                + "Configured to ignore. Skipping.",
            consumerName, ingest, writer.getUri());
        onSuccess.run();
      } else {
        onError.accept(exc);
      }
    } else {
      if (ingest.isDelete()) {
        Metrics.NON_COMMIT_LOG_DELETES.increment();
      } else {
        Metrics.NON_COMMIT_LOG_UPDATES.increment();
      }
      onSuccess.run();
    }
  }

  static void die(String message) {
    die(message, null);
  }

  static void die(String message, @Nullable Throwable error) {
    try {
      // sleep random time between zero to 10 seconds to break ties
      Thread.sleep((long) (Math.random() * 10000));
    } catch (InterruptedException ex) {
      // just for making sonar happy :-)
      Thread.currentThread().interrupt();
    }
    if (error == null) {
      log.error(message);
    } else {
      log.error(message, error);
    }
    System.exit(1);
  }

}
