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

import static cz.o2.proxima.server.IngestServer.ingestRequest;
import static cz.o2.proxima.server.IngestServer.notFound;
import static cz.o2.proxima.server.IngestServer.status;

import com.google.common.base.Strings;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.proto.service.IngestServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.StreamElement;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/** The ingestion service. */
@Slf4j
public class IngestService extends IngestServiceGrpc.IngestServiceImplBase {

  private final Repository repo;
  private final DirectDataOperator direct;
  private final ScheduledExecutorService scheduler;

  public IngestService(
      Repository repo, DirectDataOperator direct, ScheduledExecutorService scheduler) {

    this.repo = repo;
    this.direct = direct;
    this.scheduler = scheduler;
  }

  private class IngestObserver implements StreamObserver<Rpc.Ingest> {

    final StreamObserver<Rpc.Status> responseObserver;
    final Object inflightRequestsLock = new Object();
    final AtomicInteger inflightRequests = new AtomicInteger(0);
    final Object responseObserverLock = new Object();

    IngestObserver(StreamObserver<Rpc.Status> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(Rpc.Ingest request) {
      Metrics.INGEST_SINGLE.increment();
      inflightRequests.incrementAndGet();
      processSingleIngest(
          request,
          status -> {
            synchronized (responseObserverLock) {
              responseObserver.onNext(status);
            }
            if (inflightRequests.decrementAndGet() == 0) {
              synchronized (inflightRequestsLock) {
                inflightRequestsLock.notifyAll();
              }
            }
          });
    }

    @Override
    public void onError(Throwable thrwbl) {
      log.error("Error on channel", thrwbl);
      synchronized (responseObserverLock) {
        responseObserver.onError(thrwbl);
      }
    }

    @Override
    public void onCompleted() {
      inflightRequests.accumulateAndGet(
          0,
          (a, b) -> {
            int res = a + b;
            if (res > 0) {
              synchronized (inflightRequestsLock) {
                try {
                  while (inflightRequests.get() > 0) {
                    inflightRequestsLock.wait();
                  }
                } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
                }
              }
            }
            synchronized (responseObserverLock) {
              responseObserver.onCompleted();
            }
            return res;
          });
    }
  }

  private class IngestBulkObserver implements StreamObserver<Rpc.IngestBulk> {

    final StreamObserver<Rpc.StatusBulk> responseObserver;
    final Queue<Rpc.Status> statusQueue = new ConcurrentLinkedQueue<>();
    final AtomicBoolean completed = new AtomicBoolean(false);
    final Object inflightRequestsLock = new Object();
    final AtomicInteger inflightRequests = new AtomicInteger();
    final AtomicLong lastFlushNanos = new AtomicLong(System.nanoTime());
    final Rpc.StatusBulk.Builder builder = Rpc.StatusBulk.newBuilder();
    static final long MAX_SLEEP_NANOS = 100000000L;
    static final int MAX_QUEUED_STATUSES = 500;

    Runnable flushTask = createFlushTask();

    // schedule the flush periodically
    ScheduledFuture<?> flushFuture =
        scheduler.scheduleAtFixedRate(
            flushTask, MAX_SLEEP_NANOS, MAX_SLEEP_NANOS, TimeUnit.NANOSECONDS);

    IngestBulkObserver(StreamObserver<Rpc.StatusBulk> responseObserver) {
      this.responseObserver = responseObserver;
    }

    private Runnable createFlushTask() {
      return () -> {
        try {
          synchronized (builder) {
            while (statusQueue.size() > MAX_QUEUED_STATUSES) {
              peekQueueToBuilderAndFlush();
            }
            long now = System.nanoTime();
            if (now - lastFlushNanos.get() >= MAX_SLEEP_NANOS) {
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
        } catch (Throwable ex) {
          log.error("Failed to send bulk status", ex);
        }
      };
    }

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
      bulk.getIngestList()
          .stream()
          .forEach(
              r ->
                  processSingleIngest(
                      r,
                      status -> {
                        statusQueue.add(status);
                        if (statusQueue.size() >= MAX_QUEUED_STATUSES) {
                          // enqueue flush
                          scheduler.execute(flushTask);
                        }
                        if (inflightRequests.decrementAndGet() == 0) {
                          // there is no more inflight requests
                          synchronized (inflightRequestsLock) {
                            inflightRequestsLock.notifyAll();
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

    @Override
    public void onCompleted() {
      completed.set(true);
      flushFuture.cancel(true);
      // flush all responses to the observer
      synchronized (inflightRequests) {
        while (inflightRequests.get() > 0) {
          try {
            inflightRequests.wait(100);
          } catch (InterruptedException ex) {
            log.warn("Interrupted while waiting to send responses to client", ex);
            Thread.currentThread().interrupt();
          }
        }
      }
      while (!statusQueue.isEmpty()) {
        peekQueueToBuilderAndFlush();
      }
      flush();
      responseObserver.onCompleted();
    }
  }

  private void processSingleIngest(Rpc.Ingest request, Consumer<Rpc.Status> consumer) {

    if (log.isDebugEnabled()) {
      log.debug("Processing input ingest {}", TextFormat.shortDebugString(request));
    }
    Consumer<Rpc.Status> loggingConsumer =
        rpc -> {
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
      log.error("Error processing user request {}", TextFormat.shortDebugString(request), err);
      loggingConsumer.accept(status(request.getUuid(), 500, err.getMessage()));
    }
  }

  /**
   * Ingest the given request and return {@code true} if successfully ingested and {@code false} if
   * the request is invalid.
   */
  private boolean writeRequest(Rpc.Ingest request, Consumer<Rpc.Status> consumer) {

    if (Strings.isNullOrEmpty(request.getKey())
        || Strings.isNullOrEmpty(request.getEntity())
        || Strings.isNullOrEmpty(request.getAttribute())) {
      consumer.accept(status(request.getUuid(), 400, "Missing required fields in input message"));
      return false;
    }
    Optional<EntityDescriptor> entity = repo.findEntity(request.getEntity());

    if (!entity.isPresent()) {
      consumer.accept(notFound(request.getUuid(), "Entity " + request.getEntity() + " not found"));
      return false;
    }
    Optional<AttributeDescriptor<Object>> attr = entity.get().findAttribute(request.getAttribute());
    if (!attr.isPresent()) {
      consumer.accept(
          notFound(
              request.getUuid(),
              "Attribute "
                  + request.getAttribute()
                  + " of entity "
                  + entity.get().getName()
                  + " not found"));
      return false;
    }
    return ingestRequest(
        direct, toStreamElement(request, entity.get(), attr.get()), request.getUuid(), consumer);
  }

  @Override
  public void ingest(Rpc.Ingest request, StreamObserver<Rpc.Status> responseObserver) {

    Metrics.INGEST_SINGLE.increment();
    processSingleIngest(
        request,
        status -> {
          responseObserver.onNext(status);
          responseObserver.onCompleted();
        });
  }

  @Override
  public StreamObserver<Rpc.Ingest> ingestSingle(StreamObserver<Rpc.Status> responseObserver) {

    return new IngestObserver(responseObserver);
  }

  @Override
  public StreamObserver<Rpc.IngestBulk> ingestBulk(
      StreamObserver<Rpc.StatusBulk> responseObserver) {

    // the responseObserver doesn't have to be synchronized in this
    // case, because the communication with the observer is done
    // in single flush thread

    return new IngestBulkObserver(responseObserver);
  }

  private static StreamElement toStreamElement(
      Rpc.Ingest request, EntityDescriptor entity, AttributeDescriptor<?> attr) {

    long stamp = request.getStamp() == 0 ? System.currentTimeMillis() : request.getStamp();

    if (request.getDelete()) {
      return attr.isWildcard() && attr.getName().equals(request.getAttribute())
          ? StreamElement.deleteWildcard(entity, attr, request.getUuid(), request.getKey(), stamp)
          : StreamElement.delete(
              entity, attr, request.getUuid(), request.getKey(), request.getAttribute(), stamp);
    }
    return StreamElement.upsert(
        entity,
        attr,
        request.getUuid(),
        request.getKey(),
        request.getAttribute(),
        stamp,
        request.getValue().toByteArray());
  }
}
