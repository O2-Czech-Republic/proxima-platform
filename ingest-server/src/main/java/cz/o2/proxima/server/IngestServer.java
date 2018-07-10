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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.proto.service.IngestServiceGrpc.IngestServiceImplBase;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc.RetrieveServiceImplBase;
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
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.RetryableBulkObserver;
import cz.o2.proxima.storage.commitlog.RetryableLogObserver;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.util.Pair;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
   * @throws Exception on error
   */
  public static void main(String[] args) throws Exception {
    final IngestServer server;

    if (args.length == 0) {
      server = new IngestServer(ConfigFactory.load().resolve());
    } else {
      server = new IngestServer(ConfigFactory.parseFile(
          new File(args[0])).resolve());
    }
    server.run();
  }

  /**
   * The ingestion service.
   **/
  public class IngestService extends IngestServiceImplBase {

    @Override
    public void ingest(
        Rpc.Ingest request, StreamObserver<Rpc.Status> responseObserver) {
      Metrics.INGEST_SINGLE.increment();
      processSingleIngest(request, status -> {
        synchronized (responseObserver) {
          responseObserver.onNext(status);
          responseObserver.onCompleted();
        }
      });
    }


    @Override
    public StreamObserver<Rpc.Ingest> ingestSingle(
        StreamObserver<Rpc.Status> responseObserver) {

      AtomicInteger inflightRequest = new AtomicInteger(0);

      return new StreamObserver<Rpc.Ingest>() {
        @Override
        public void onNext(Rpc.Ingest request) {
          Metrics.INGEST_SINGLE.increment();
          inflightRequest.incrementAndGet();
          processSingleIngest(request, status -> {
            synchronized (responseObserver) {
              responseObserver.onNext(status);
            }
            if (inflightRequest.decrementAndGet() == 0) {
              synchronized (inflightRequest) {
                inflightRequest.notify();
              }
            }
          });
        }

        @Override
        public void onError(Throwable thrwbl) {
          log.error("Error on channel", thrwbl);
          synchronized (responseObserver) {
            responseObserver.onError(thrwbl);
          }
        }

        @Override
        public void onCompleted() {
          inflightRequest.accumulateAndGet(0, (a, b) -> {
            int res = a + b;
            if (res > 0) {
              synchronized (inflightRequest) {
                try {
                  inflightRequest.wait();
                } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
                }
              }
            }
            synchronized (responseObserver) {
              responseObserver.onCompleted();
            }
            return res;
          });
        }

      };
    }


    @Override
    public StreamObserver<Rpc.IngestBulk> ingestBulk(
        StreamObserver<Rpc.StatusBulk> responseObserver) {

      // the responseObserver doesn't have to be synchronized in this
      // case, because the communication with the observer is done
      // in single flush thread

      return new StreamObserver<Rpc.IngestBulk>() {

        final Queue<Rpc.Status> statusQueue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicInteger inflightRequests = new AtomicInteger();
        final AtomicLong lastFlushNanos = new AtomicLong(System.nanoTime());
        final Rpc.StatusBulk.Builder builder = Rpc.StatusBulk.newBuilder();
        final long maxSleepNanos = 100000000L;
        final int maxQueuedStatuses = 500;

        Runnable flushTask = () -> {
          try {
            synchronized (builder) {
              while (statusQueue.size() > maxQueuedStatuses) {
                peekQueueToBuilderAndFlush();
              }
              long now = System.nanoTime();
              if (now - lastFlushNanos.get() >= maxSleepNanos) {
                while (!statusQueue.isEmpty()) {
                  peekQueueToBuilderAndFlush();
                }
              }
              if (builder.getStatusCount() > 0) {
                responseObserver.onNext(builder.build());
                builder.clear();
              }
              if (completed.get() && inflightRequests.get() == 0 && statusQueue.isEmpty()) {
                responseObserver.onCompleted();
              }
            }
          } catch (Exception ex) {
            log.error("Failed to send bulk status", ex);
          }
        };

        // schedule the flush periodically
        ScheduledFuture<?> flushFuture = scheduler.scheduleAtFixedRate(
            flushTask, maxSleepNanos, maxSleepNanos, TimeUnit.NANOSECONDS);

        private void peekQueueToBuilderAndFlush() {
          synchronized (builder) {
            builder.addStatus(statusQueue.poll());
            if (builder.getStatusCount() >= 1000) {
              flush();
            }
          }
        }

        /** Flush response(s) to the observer. */
        private void flush() {
          synchronized (builder) {
            lastFlushNanos.set(System.nanoTime());
            Rpc.StatusBulk bulk = builder.build();
            if (bulk.getStatusCount() > 0) {
              responseObserver.onNext(bulk);
            }
            builder.clear();
          }
        }

        @Override
        public void onNext(Rpc.IngestBulk bulk) {
          Metrics.INGEST_BULK.increment();
          Metrics.BULK_SIZE.increment(bulk.getIngestCount());
          inflightRequests.addAndGet(bulk.getIngestCount());
          bulk.getIngestList().stream()
              .forEach(r -> processSingleIngest(r, status -> {
                statusQueue.add(status);
                if (statusQueue.size() >= maxQueuedStatuses) {
                  // enqueue flush
                  scheduler.execute(flushTask);
                }
                if (inflightRequests.decrementAndGet() == 0) {
                  // there is no more infligt requests
                  synchronized (inflightRequests) {
                    inflightRequests.notify();
                  }
                }
              }));
        }

        @Override
        public void onError(Throwable error) {
          log.error("Error from client", error);
          // close the connection
          responseObserver.onError(error);
          flushFuture.cancel(true);
        }

        @SuppressFBWarnings(
            value = "JLM_JSR166_UTILCONCURRENT_MONITORENTER",
            justification = "The synchronization on `inflighRequests` is used only for "
                + "waiting before the flush thread finishes (wait() - notify())")
        @Override
        public void onCompleted() {
          completed.set(true);
          flushFuture.cancel(true);
          // flush all responses to the observer
          synchronized (inflightRequests) {
            while (inflightRequests.get() != 0) {
              try {
                inflightRequests.wait(100);
              } catch (InterruptedException ex) {
                log.warn("Interrupted while waiting to send responses to client");
              }
            }
          }
          while (!statusQueue.isEmpty()) {
            peekQueueToBuilderAndFlush();
          }
          flush();
          responseObserver.onCompleted();
        }

      };
    }

  }

  public class RetrieveService extends RetrieveServiceImplBase {

    private class Status extends Exception {
      final int status;
      final String message;
      Status(int status, String message) {
        this.status = status;
        this.message = message;
      }
    };

    @Override
    public void listAttributes(
        Rpc.ListRequest request,
        StreamObserver<Rpc.ListResponse> responseObserver) {

      try {
        Metrics.LIST_REQUESTS.increment();
        log.info("Processing listAttributes {}", TextFormat.shortDebugString(request));
        if (request.getEntity().isEmpty() || request.getKey().isEmpty()
            || request.getWildcardPrefix().isEmpty()) {
          throw new Status(400, "Missing some required fields");
        }

        EntityDescriptor entity = repo.findEntity(request.getEntity())
            .orElseThrow(() -> new Status(
                404, "Entity " + request.getEntity() + " not found"));

        AttributeDescriptor<Object> wildcard = entity.findAttribute(
            request.getWildcardPrefix() + ".*").orElseThrow(
                () -> new Status(404, "Entity " + request.getEntity()
                    + " does not have wildcard attribute "
                    + request.getWildcardPrefix()));

        RandomAccessReader reader = repo.getFamiliesForAttribute(wildcard).stream()
            .filter(af -> af.getRandomAccessReader().isPresent())
            .map(af -> af.getRandomAccessReader().get())
            .findAny()
            .orElseThrow(() -> new Status(400, "Attribute " + wildcard
                + " has no random reader"));

        Rpc.ListResponse.Builder response = Rpc.ListResponse.newBuilder()
            .setStatus(200);

        reader.scanWildcard(
            request.getKey(), wildcard,
            reader.fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, request.getOffset()),
            request.getLimit() > 0 ? request.getLimit() : -1,
            kv -> response.addValue(
                Rpc.ListResponse.AttrValue.newBuilder()
                    .setAttribute(kv.getAttribute())
                    .setValue(ByteString.copyFrom(kv.getValueBytes()))));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
      } catch (Status s) {
        responseObserver.onNext(Rpc.ListResponse.newBuilder()
              .setStatus(s.status)
              .setStatusMessage(s.message)
              .build());
        responseObserver.onCompleted();
      } catch (Exception ex) {
        log.error("Failed to process request {}", request, ex);
        responseObserver.onNext(Rpc.ListResponse.newBuilder()
            .setStatus(500)
            .setStatusMessage(ex.getMessage())
            .build());
        responseObserver.onCompleted();
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void get(
        Rpc.GetRequest request,
        StreamObserver<Rpc.GetResponse> responseObserver) {

      Metrics.GET_REQUESTS.increment();
      log.info("Processing get {}", TextFormat.shortDebugString(request));
      try {
        if (request.getEntity().isEmpty() || request.getKey().isEmpty()
            || request.getAttribute().isEmpty()) {
          throw new Status(400, "Missing some required fields");
        }

        EntityDescriptor entity = repo.findEntity(request.getEntity())
            .orElseThrow(() -> new Status(
                404, "Entity " + request.getEntity() + " not found"));

        AttributeDescriptor<Object> attribute = entity.findAttribute(
            request.getAttribute()).orElseThrow(
                () -> new Status(404, "Entity " + request.getEntity()
                    + " does not have attribute "
                    + request.getAttribute()));

        RandomAccessReader reader = repo.getFamiliesForAttribute(attribute).stream()
            .filter(af -> af.getRandomAccessReader().isPresent())
            .map(af -> af.getRandomAccessReader().get())
            .findAny()
            .orElseThrow(() -> new Status(400, "Attribute " + attribute
                + " has no random reader"));

        KeyValue<Object> kv = reader.get(request.getKey(), request.getAttribute(), attribute)
            .orElseThrow(() -> new Status(404, "Key " + request.getKey() + " and/or attribute "
                + request.getAttribute() + " not found"));

        responseObserver.onNext(Rpc.GetResponse.newBuilder()
            .setStatus(200)
            .setValue(ByteString.copyFrom(kv.getValueBytes()))
            .build());

        responseObserver.onCompleted();
      } catch (Status s) {
        responseObserver.onNext(Rpc.GetResponse.newBuilder()
              .setStatus(s.status)
              .setStatusMessage(s.message)
              .build());
        responseObserver.onCompleted();
      } catch (Exception ex) {
        log.error("Failed to process request {}", request, ex);
        responseObserver.onNext(Rpc.GetResponse.newBuilder()
            .setStatus(500)
            .setStatusMessage(ex.getMessage())
            .build());
        responseObserver.onCompleted();
      }

    }

  }

  @Getter
  final int minCores = 2;
  @Getter
  final Executor executor = new ThreadPoolExecutor(
            minCores,
            10 * minCores,
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
    this.ignoreErrors = cfg.hasPath(Constants.CFG_IGNORE_ERRORS)
        ? cfg.getBoolean(Constants.CFG_IGNORE_ERRORS)
        : false;
  }



  private void processSingleIngest(
      Rpc.Ingest request,
      Consumer<Rpc.Status> consumer) {

    if (log.isDebugEnabled()) {
      log.debug("Processing input ingest {}", TextFormat.shortDebugString(request));
    }
    Consumer<Rpc.Status> loggingConsumer = rpc -> {
      log.info(
          "Input ingest {}: {}, {}",
          TextFormat.shortDebugString(request),
          rpc.getStatus(),
          rpc.getStatus() == 200 ? "OK" : rpc.getStatusMessage());
      consumer.accept(rpc);
    };
    Metrics.INGESTS.increment();
    try {
      if (!writeRequest(request, loggingConsumer)) {
        Metrics.INVALID_REQUEST.increment();
      }
    } catch (Exception err) {
      log.error("Error processing user request {}", request, err);
      loggingConsumer.accept(status(request.getUuid(), 500, err.getMessage()));
    }
  }

  /**
   * Ingest the given request and return {@code true} if successfully
   * ingested and {@code false} if the request is invalid.
   */
  private boolean writeRequest(
      Rpc.Ingest request,
      Consumer<Rpc.Status> consumer) throws IOException {

    if (Strings.isNullOrEmpty(request.getKey())
        || Strings.isNullOrEmpty(request.getEntity())
        || Strings.isNullOrEmpty(request.getAttribute())) {
      consumer.accept(status(request.getUuid(),
          400, "Missing required fields in input message"));
      return false;
    }
    Optional<EntityDescriptor> entity = repo.findEntity(request.getEntity());

    if (!entity.isPresent()) {
      consumer.accept(notFound(request.getUuid(),
          "Entity " + request.getEntity() + " not found"));
      return false;
    }
    Optional<AttributeDescriptor<Object>> attr = entity.get().findAttribute(
        request.getAttribute());
    if (!attr.isPresent()) {
      consumer.accept(notFound(request.getUuid(),
          "Attribute " + request.getAttribute() + " of entity "
              + entity.get().getName() + " not found"));
      return false;
    }
    return ingestRequest(
        toStreamElement(request, entity.get(), attr.get()),
        request.getUuid(), consumer);
  }

  private static StreamElement toStreamElement(
      Rpc.Ingest request, EntityDescriptor entity,
      AttributeDescriptor attr) {

    long stamp = request.getStamp() == 0
        ? System.currentTimeMillis()
        : request.getStamp();

    if (request.getDelete()) {
      return attr.isWildcard() && attr.getName().equals(request.getAttribute())
        ? StreamElement.deleteWildcard(
            entity, attr, request.getUuid(),
            request.getKey(), stamp)
        : StreamElement.delete(
            entity, attr, request.getUuid(),
            request.getKey(), request.getAttribute(), stamp);
    }
    return StreamElement.update(
            entity, attr, request.getUuid(),
            request.getKey(), request.getAttribute(),
            stamp,
            request.getValue().toByteArray());
  }

  private boolean ingestRequest(
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
    log.debug("Writing request {} to commit log {}", ingest, writer.getURI());
    writer.write(ingest, (s, exc) -> {
      if (s) {
        responseConsumer.accept(ok(uuid));
      } else {
        responseConsumer.accept(status(uuid, 500, exc.getMessage()));
      }
    });
    return true;
  }

  private Rpc.Status notFound(String uuid, String what) {
    return Rpc.Status.newBuilder()
        .setUuid(uuid)
        .setStatus(404)
        .setStatusMessage(what)
        .build();
  }

  private Rpc.Status ok(String uuid) {
    return Rpc.Status.newBuilder()
        .setStatus(200)
        .setUuid(uuid)
        .build();
  }

  private Rpc.Status status(String uuid, int status, String message) {
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
        .addService(new IngestService())
        .addService(new RetrieveService())
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
              name, commitLog, commitLog.getURI(), writer.getURI(), allowedAttributes);
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

    new RetryableLogObserver(3, consumer, reader) {

      @Override
      protected void failure() {
        die(String.format("Failed to transform using %s. Bailing out.", t));
      }

      @Override
      public boolean onNextInternal(
          StreamElement ingest, LogObserver.OffsetCommitter committer) {

        if (!f.apply(ingest)) {
          log.debug("Skipping transformation of {} by filter", ingest);
          committer.confirm();
          return true;
        }
        AtomicInteger toConfirm = new AtomicInteger(0);
        try {
          Transformation.Collector<StreamElement> collector = elem -> {
            try {
              log.debug("Writing transformed element {}", elem);
              ingestRequest(
                  elem, elem.getUuid(), rpc -> {
                    if (rpc.getStatus() == 200) {
                      if (toConfirm.decrementAndGet() == 0) {
                        committer.confirm();
                      }
                    } else {
                      toConfirm.set(-1);
                      committer.fail(new RuntimeException(
                          String.format("Received invalid status %d:%s",
                              rpc.getStatus(), rpc.getStatusMessage())));
                    }
                  });
            } catch (Exception ex) {
              toConfirm.set(-1);
              committer.fail(ex);
            }
          };

          if (toConfirm.addAndGet(t.apply(ingest, collector)) == 0) {
            committer.confirm();
          }
        } catch (Exception ex) {
          toConfirm.set(-1);
          committer.fail(ex);
        }
        return true;
      }


    }.start();
    log.info(
        "Started transformer {} reading from {} using {}",
        consumer, reader.getURI(), t.getClass());
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
    Map<AttributeDescriptor, AttributeFamilyDescriptor> attrToCommitLog = repo.getAllFamilies()
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
                    throw new IllegalStateException("Missing source commit log family for " + attr);
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
        writerBase.getType(), writerBase.getURI(), commitLog.getURI());

    if (writerBase.getType() == AttributeWriterBase.Type.ONLINE) {
      OnlineAttributeWriter writer = writerBase.online();
      observer = getOnlineWriter(
          consumerName, commitLog, allowedAttributes, filter, writer);
    } else {
      BulkAttributeWriter writer = writerBase.bulk();
      observer = getBulkWriter(
          consumerName, commitLog, allowedAttributes, filter, writer, retry);
    }

    observer.start();

  }

  private AbstractRetryableLogObserver getBulkWriter(
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
          BulkLogObserver.OffsetCommitter committer) {

        return writeInternal(ingest, committer);
      }

      private boolean writeInternal(
          StreamElement ingest, BulkLogObserver.OffsetCommitter committer) {

        final boolean allowed = allowedAttributes.contains(ingest.getAttributeDescriptor());
        log.debug("Received new ingest element {}", ingest);
        if (allowed && filter.apply(ingest)) {
          Failsafe.with(retry).run(() -> {
            log.debug("Writing element {} into {}", ingest, writer);
            writer.write(ingest, (success, exc) -> {
              if (!success) {
                log.error(
                    "Failed to write ingest {} to {}", ingest, writer.getURI(),
                    exc);
                Metrics.NON_COMMIT_WRITES_RETRIES.increment();
                if (ignoreErrors) {
                  log.error(
                      "Retries exhausted trying to ingest {} to {}. Configured to ignore. Skipping.",
                      ingest, writer.getURI());
                  committer.confirm();
                } else {
                  committer.fail(exc);
                }
              } else {
                if (ingest.isDelete()) {
                  Metrics.NON_COMMIT_LOG_DELETES.increment();
                } else {
                  Metrics.NON_COMMIT_LOG_UPDATES.increment();
                }
                committer.confirm();
              }
            });
          });
        } else {
          Metrics.COMMIT_UPDATE_DISCARDED.increment();
          log.debug(
              "Discarding write of {} to {} because of {}, "
                  + "with allowedAttributes {} and filter class {}",
              ingest, writer.getURI(),
              allowed ? "applied filter" : "invalid attribute",
              allowedAttributes,
              filter.getClass());
        }
        return true;
      }

      @Override
      protected void failure() {
        die(String.format(
            "Too many errors retrying the consumption of commit log %s. Killing self.",
            commitLog.getURI()));
      }

      @Override
      public void onRestart(List<Offset> offsets) {
        log.info(
            "Restarting bulk processing of {} from {}, rollbacking the writer",
            writer.getURI(), offsets);
        writer.rollback();
      }

    };
  }

  private AbstractRetryableLogObserver getOnlineWriter(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      OnlineAttributeWriter writer) {

    return new RetryableLogObserver(3, consumerName, commitLog) {

      @Override
      public boolean onNextInternal(
          StreamElement ingest,
          LogObserver.OffsetCommitter committer) {

        final boolean allowed = allowedAttributes.contains(ingest.getAttributeDescriptor());
        log.debug("Received new ingest element {}", ingest);
        if (allowed && filter.apply(ingest)) {
          Failsafe.with(retryPolicy).run(() -> {
            log.debug("Writing element {} into {}", ingest, writer);
            writer.write(ingest, (success, exc) -> {
              if (!success) {
                log.error(
                    "Failed to write ingest {} to {}", ingest, writer.getURI(),
                    exc);
                Metrics.NON_COMMIT_WRITES_RETRIES.increment();
                if (ignoreErrors) {
                  log.error(
                      "Retries exhausted trying to ingest {} to {}. Configured to ignore. Skipping.",
                      ingest, writer.getURI());
                  committer.confirm();
                } else {
                  committer.fail(exc);
                }
              } else {
                if (ingest.isDelete()) {
                  Metrics.NON_COMMIT_LOG_DELETES.increment();
                } else {
                  Metrics.NON_COMMIT_LOG_UPDATES.increment();
                }
                committer.confirm();
              }
            });
          });
        } else {
          Metrics.COMMIT_UPDATE_DISCARDED.increment();
          log.debug(
              "Discarding write of {} to {} because of {}, "
                  + "with allowedAttributes {} and filter class {}",
              ingest, writer.getURI(),
              allowed ? "applied filter" : "invalid attribute",
              allowedAttributes,
              filter.getClass());
          committer.confirm();
        }
        return true;
      }

      @Override
      protected void failure() {
        die(String.format(
            "Too many errors retrying the consumption of commit log %s. Killing self.",
            commitLog.getURI()));
      }

    };
  }

  private void die(String message) {
    die(message, null);
  }

  private void die(String message, @Nullable Throwable error) {
    try {
      // sleep random time between zero to 10 seconds to break ties
      Thread.sleep((long) (Math.random() * 10000));
    } catch (InterruptedException ex) {
      // nop, already going to die
    }
    if (error == null) {
      log.error(message);
    } else {
      log.error(message, error);
    }
    System.exit(1);
  }

}
