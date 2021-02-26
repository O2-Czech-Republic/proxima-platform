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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.RetryableLogObserver;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.TransformationDescriptor;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.ElementWiseTransformation;
import cz.o2.proxima.util.Pair;
import java.io.File;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

/** Server that controls replications of primary commit logs to replica attribute families. */
@Slf4j
public class ReplicationController {

  /**
   * Run the controller.
   *
   * @param args command line arguments
   * @throws Throwable on error
   */
  public static void main(String[] args) throws Throwable {
    final Repository repo;
    if (args.length == 0) {
      repo = Repository.of(ConfigFactory.load().resolve());
    } else {
      repo = Repository.of(ConfigFactory.parseFile(new File(args[0])).resolve());
    }
    ReplicationController.of(repo).runReplicationThreads().get();
  }

  /**
   * Constructs a new {@link ReplicationController}.
   *
   * @param repository Repository to use for replication.
   * @return New replication controller.
   */
  public static ReplicationController of(Repository repository) {
    return new ReplicationController(repository);
  }

  @AllArgsConstructor
  private abstract class ReplicationLogObserver implements LogObserver {

    private final String consumerName;
    private final CommitLogReader commitLog;
    private final Set<AttributeDescriptor<?>> allowedAttributes;
    private final StorageFilter filter;
    private final AttributeWriterBase writer;

    @Override
    public boolean onNext(StreamElement ingest, OnNextContext context) {
      final boolean allowed = allowedAttributes.contains(ingest.getAttributeDescriptor());
      log.debug("Consumer {}: received new ingest element {}", consumerName, ingest);
      if (allowed && filter.apply(ingest)) {
        Metrics.ingestsForAttribute(ingest.getAttributeDescriptor()).increment();
        if (!ingest.isDelete()) {
          Metrics.sizeForAttribute(ingest.getAttributeDescriptor())
              .increment(ingest.getValue().length);
        }
        Failsafe.with(retryPolicy).run(() -> ingestElement(ingest, context));
      } else {
        Metrics.COMMIT_UPDATE_DISCARDED.increment();
        log.debug(
            "Consumer {}: discarding write of {} to {} because of {}, "
                + "with allowedAttributes {} and filter class {}",
            consumerName,
            ingest,
            writer.getUri(),
            allowed ? "applied filter" : "invalid attribute",
            allowedAttributes,
            filter.getClass());
        maybeCommitInvalidWrite(context);
      }
      return true;
    }

    void maybeCommitInvalidWrite(OnNextContext context) {}

    @Override
    public boolean onError(Throwable error) {
      onReplicationError(
          new IllegalStateException(
              String.format(
                  "Consumer %s: too many errors retrying the consumption of commit log %s.",
                  consumerName, commitLog.getUri())));
      return false;
    }

    @Override
    public void onRepartition(OnRepartitionContext context) {
      log.info(
          "Consumer {}: restarting bulk processing of {} from {}, rollbacking the writer",
          consumerName,
          writer.getUri(),
          context.partitions());
      writer.rollback();
    }

    @Override
    public void onIdle(OnIdleContext context) {
      Metrics.reportConsumerWatermark(consumerName, context.getWatermark(), -1);
    }

    abstract void ingestElement(StreamElement ingest, OnNextContext context);
  }

  @Getter
  RetryPolicy retryPolicy =
      new RetryPolicy().withMaxRetries(3).withBackoff(3000, 20000, TimeUnit.MILLISECONDS, 2.0);

  private final Repository repository;
  private final DirectDataOperator dataOperator;
  private final ScheduledExecutorService scheduler =
      new ScheduledThreadPoolExecutor(
          1,
          runnable -> {
            Thread ret = new Thread(runnable);
            ret.setName("replication-scheduler");
            return ret;
          });
  private static final boolean ignoreErrors = false;

  private final List<CompletableFuture<Void>> replications = new CopyOnWriteArrayList<>();

  ReplicationController(Repository repository) {
    this.repository = repository;
    this.dataOperator = repository.getOrCreateOperator(DirectDataOperator.class);
  }

  public CompletableFuture<Void> runReplicationThreads() {

    final CompletableFuture<Void> completed = new CompletableFuture<>();
    replications.add(completed);

    // index the repository
    Map<DirectAttributeFamilyDescriptor, Set<DirectAttributeFamilyDescriptor>> familyToCommitLog;
    familyToCommitLog = indexFamilyToCommitLogs();

    log.info("Starting consumer threads for familyToCommitLog {}", familyToCommitLog);

    // execute threads to consume the commit log
    familyToCommitLog.forEach(
        (replicaFamily, primaryFamilies) -> {
          for (DirectAttributeFamilyDescriptor primaryFamily : primaryFamilies) {
            if (!replicaFamily.getDesc().getAccess().isReadonly()) {
              consumeLog(primaryFamily, replicaFamily);
            } else {
              log.debug("Not starting thread for read-only family {}", replicaFamily);
            }
          }
        });

    // execute transformer threads
    repository.getTransformations().forEach(this::runTransformer);

    scheduler.scheduleAtFixedRate(this::checkLiveness, 0, 1, TimeUnit.SECONDS);

    return completed;
  }

  @VisibleForTesting
  boolean checkLiveness() {
    long minWatermark = Metrics.minWatermarkOfConsumers();
    boolean isLive = minWatermark > System.currentTimeMillis() - 10_000;
    if (log.isDebugEnabled()) {
      log.debug("Min watermark of consumers calculated as {}", Instant.ofEpochMilli(minWatermark));
    }
    Metrics.LIVENESS.increment(isLive ? 1 : 0);
    return isLive;
  }

  private void consumeLog(
      DirectAttributeFamilyDescriptor primaryFamily,
      DirectAttributeFamilyDescriptor replicaFamily) {

    final CommitLogReader commitLog =
        primaryFamily
            .getCommitLogReader()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Failed to find commit-log reader in family %s.", primaryFamily)));

    final AttributeWriterBase writer =
        replicaFamily
            .getWriter()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to get writer for family %s.",
                            replicaFamily.getDesc().getName())));

    final StorageFilter filter = replicaFamily.getDesc().getFilter();

    final Set<AttributeDescriptor<?>> allowedAttributes =
        new HashSet<>(replicaFamily.getAttributes());

    final String name = replicaFamily.getDesc().getReplicationConsumerNameFactory().apply();
    log.info(
        "Using consumer name {} for replicate family {}", name, replicaFamily.getDesc().getName());

    registerWriterTo(name, commitLog, allowedAttributes, filter, writer);

    log.info(
        "Started consumer {} consuming from log {} with URI {} into {} attributes {}",
        name,
        commitLog,
        commitLog.getUri(),
        writer.getUri(),
        allowedAttributes);
  }

  /**
   * Retrieve attribute family and it's associated commit log(s). The families returned are only
   * those which are not used as commit log themselves.
   */
  private Map<DirectAttributeFamilyDescriptor, Set<DirectAttributeFamilyDescriptor>>
      indexFamilyToCommitLogs() {

    // each attribute and its associated primary family
    final Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor> primaryFamilies =
        dataOperator
            .getAllFamilies()
            .filter(family -> family.getDesc().getType() == StorageType.PRIMARY)
            // take pair of attribute to associated commit log
            .flatMap(
                primaryFamily ->
                    primaryFamily
                        .getAttributes()
                        .stream()
                        .map(attribute -> Pair.of(attribute, primaryFamily)))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    return dataOperator
        .getAllFamilies()
        .filter(family -> family.getDesc().getType() == StorageType.REPLICA)
        // map to pair of attribute family and associated commit log(s) via attributes
        .map(
            replicaFamily -> {
              if (replicaFamily.getSource().isPresent()) {
                final String source = replicaFamily.getSource().get();
                return Pair.of(
                    replicaFamily,
                    Collections.singleton(
                        dataOperator
                            .getAllFamilies()
                            .filter(af2 -> af2.getDesc().getName().equals(source))
                            .findAny()
                            .orElseThrow(
                                () ->
                                    new IllegalArgumentException(
                                        String.format("Unknown family %s.", source)))));
              }
              return Pair.of(
                  replicaFamily,
                  replicaFamily
                      .getAttributes()
                      .stream()
                      .map(
                          attr -> {
                            final DirectAttributeFamilyDescriptor primaryFamily =
                                primaryFamilies.get(attr);
                            final Optional<OnlineAttributeWriter> maybeWriter =
                                dataOperator.getWriter(attr);
                            if (primaryFamily == null && maybeWriter.isPresent()) {
                              throw new IllegalStateException(
                                  String.format("Missing source commit log family for %s.", attr));
                            }
                            return primaryFamily;
                          })
                      .filter(Objects::nonNull)
                      .collect(Collectors.toSet()));
            })
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private void runTransformer(String name, TransformationDescriptor transform) {
    DirectAttributeFamilyDescriptor family =
        transform
            .getAttributes()
            .stream()
            .map(
                a ->
                    dataOperator
                        .getFamiliesForAttribute(a)
                        .stream()
                        .filter(af -> af.getDesc().getAccess().canReadCommitLog())
                        .collect(Collectors.toSet()))
            .reduce(Sets::intersection)
            .filter(s -> !s.isEmpty())
            .flatMap(s -> s.stream().filter(f -> f.getCommitLogReader().isPresent()).findAny())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Cannot obtain attribute family for " + transform.getAttributes()));

    final ElementWiseTransformation transformation =
        transform.getTransformation().asElementWiseTransform();
    final StorageFilter filter = transform.getFilter();
    final String consumer = transform.getConsumerNameFactory().apply();

    final CommitLogReader reader =
        family
            .getCommitLogReader()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Unable to get reader for family " + family.getDesc().getName() + "."));

    startTransformationObserver(consumer, reader, transformation, filter, name);
    log.info(
        "Started transformer {} reading from {} using {}",
        consumer,
        reader.getUri(),
        transformation.getClass());
  }

  private void startTransformationObserver(
      String consumer,
      CommitLogReader reader,
      ElementWiseTransformation transformation,
      StorageFilter filter,
      String name) {
    RetryableLogObserver.online(
            3,
            consumer,
            reader,
            new TransformationObserver(dataOperator, name, transformation, filter))
        .start();
  }

  private void registerWriterTo(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      AttributeWriterBase writerBase) {
    log.info(
        "Registering {} writer to {} from commit log {}",
        writerBase.getType(),
        writerBase.getUri(),
        commitLog.getUri());
    final RetryableLogObserver observer;
    switch (writerBase.getType()) {
      case ONLINE:
        observer =
            createOnlineObserver(
                consumerName, commitLog, allowedAttributes, filter, writerBase.online());
        break;
      case BULK:
        observer =
            createBulkObserver(
                consumerName, commitLog, allowedAttributes, filter, writerBase.bulk());
        break;
      default:
        throw new IllegalStateException(
            String.format("Unknown writer type %s.", writerBase.getType()));
    }
    observer.start();
  }

  /**
   * Get observer for that replicates data using {@link BulkAttributeWriter}.
   *
   * @param consumerName Name of the observer.
   * @param commitLog Commit log to observe.
   * @param allowedAttributes Attributes to replicate.
   * @param filter Filter for elements that we don't want to replicate.
   * @param writer Writer for replica.
   * @return Log observer.
   */
  @VisibleForTesting
  RetryableLogObserver createBulkObserver(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      BulkAttributeWriter writer) {

    final LogObserver logObserver =
        new ReplicationLogObserver(consumerName, commitLog, allowedAttributes, filter, writer) {

          @Override
          void ingestElement(StreamElement ingest, OnNextContext context) {
            final long watermark = context.getWatermark();
            Metrics.reportConsumerWatermark(consumerName, watermark, ingest.getStamp());
            log.debug(
                "Consumer {}: writing element {} into {} at watermark {}",
                consumerName,
                ingest,
                writer,
                watermark);
            writer.write(
                ingest,
                watermark,
                (success, error) ->
                    confirmWrite(
                        consumerName,
                        ingest,
                        writer,
                        success,
                        error,
                        context::confirm,
                        context::fail));
          }

          @Override
          public void onIdle(OnIdleContext context) {
            writer.updateWatermark(context.getWatermark());
          }
        };
    return RetryableLogObserver.bulk(3, consumerName, commitLog, logObserver);
  }

  /**
   * Get observer for that replicates data using {@link OnlineAttributeWriter}.
   *
   * @param consumerName Name of the observer.
   * @param commitLog Commit log to observe.
   * @param allowedAttributes Attributes to replicate.
   * @param filter Filter for elements that we don't want to replicate.
   * @param writer Writer for replica.
   * @return Log observer.
   */
  @VisibleForTesting
  RetryableLogObserver createOnlineObserver(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      OnlineAttributeWriter writer) {
    final LogObserver logObserver =
        new ReplicationLogObserver(consumerName, commitLog, allowedAttributes, filter, writer) {

          @Override
          void ingestElement(StreamElement ingest, OnNextContext context) {
            Metrics.reportConsumerWatermark(
                consumerName, context.getWatermark(), ingest.getStamp());
            log.debug("Consumer {}: writing element {} into {}", consumerName, ingest, writer);
            writer.write(
                ingest,
                (success, exc) ->
                    confirmWrite(
                        consumerName,
                        ingest,
                        writer,
                        success,
                        exc,
                        context::confirm,
                        context::fail));
          }

          @Override
          void maybeCommitInvalidWrite(OnNextContext context) {
            context.confirm();
          }
        };
    return RetryableLogObserver.online(3, consumerName, commitLog, logObserver);
  }

  private void confirmWrite(
      String consumerName,
      StreamElement ingest,
      AttributeWriterBase writer,
      boolean success,
      Throwable exc,
      Runnable onSuccess,
      Consumer<Throwable> onError) {

    if (!success) {
      log.error(
          "Consumer {}: failed to write ingest {} to {}",
          consumerName,
          ingest,
          writer.getUri(),
          exc);
      Metrics.NON_COMMIT_WRITES_RETRIES.increment();
      if (ignoreErrors) {
        log.error(
            "Consumer {}: retries exhausted trying to ingest {} to {}. "
                + "Configured to ignore. Skipping.",
            consumerName,
            ingest,
            writer.getUri());
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

  private static void onReplicationError(Throwable t) {
    Utils.die(t.getMessage(), t);
  }
}
