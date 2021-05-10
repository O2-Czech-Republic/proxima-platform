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
package cz.o2.proxima.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.proto.service.IngestServiceGrpc;
import cz.o2.proxima.proto.service.IngestServiceGrpc.IngestServiceStub;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** A client being able to connect and write requests to the ingest gateway. */
@Slf4j
public class IngestClient implements AutoCloseable {

  /** Request sent through the channel */
  @Value
  private class Request {

    final Consumer<Rpc.Status> consumer;
    final ScheduledFuture<?> timeoutFuture;
    final Rpc.Ingest payload;

    /**
     * Confirm the status and remove the timeout schedule
     *
     * @param status of the request
     */
    private void setStatus(Rpc.Status status) {
      if (timeoutFuture == null || timeoutFuture.cancel(false)) {
        consumer.accept(status);
      }
    }

    /** Retry to send the request. */
    void retry() {
      // we don't setup any timeout
      sendTry(payload, -1L, TimeUnit.MILLISECONDS, consumer, true);
    }
  }

  /**
   * Create {@link IngestClient} instance
   *
   * @param host of the ingest server
   * @param port of the ingest server
   * @return ingest client
   */
  public static IngestClient create(String host, int port) {
    return create(host, port, new Options());
  }

  /**
   * Create {@link IngestClient} instance
   *
   * @param host of the ingest server
   * @param port of the ingest server
   * @param opts extra settings
   * @return ingest client
   */
  public static IngestClient create(String host, int port, Options opts) {
    return new IngestClient(host, port, opts);
  }

  @Getter private final String host;
  @Getter private final int port;
  @Getter private final Options options;

  /** Map of UUID of message to the consumer of the message status. */
  private final Map<String, Request> inFlightRequests;

  @VisibleForTesting Channel channel = null;

  @VisibleForTesting IngestServiceStub ingestStub = null;

  @VisibleForTesting RetrieveServiceGrpc.RetrieveServiceBlockingStub retrieveStub = null;
  private final Rpc.IngestBulk.Builder bulkBuilder = Rpc.IngestBulk.newBuilder();
  private final CountDownLatch closedLatch = new CountDownLatch(1);

  @VisibleForTesting final StreamObserver<Rpc.StatusBulk> statusObserver = newStatusObserver();

  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private final AtomicReference<Thread> flushThread = new AtomicReference<>();

  private final AtomicReference<Throwable> flushThreadExc = new AtomicReference<>();

  @VisibleForTesting StreamObserver<Rpc.IngestBulk> ingestRequestObserver;

  private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);

  private long lastFlush = System.currentTimeMillis();

  @VisibleForTesting
  IngestClient(String host, int port, Options options) {
    this.host = host;
    this.port = port;
    this.options = options;
    this.inFlightRequests = Collections.synchronizedMap(new HashMap<>());
  }

  private Thread createFlushThread() {
    Thread ret =
        new Thread(
            () -> {
              try {
                long flushTimeMs = options.getFlushUsec() / 1_000L;
                while (!Thread.currentThread().isInterrupted()) {
                  flushLoop(flushTimeMs);
                }
                flushThread.set(null);
              } catch (Throwable thwbl) {
                log.error("Error in flush thread", thwbl);
                flushThread.set(null);
                flushThreadExc.set(thwbl);
              }
            });
    ret.setDaemon(true);
    ret.setName(getClass().getSimpleName() + "-flushThread");
    ret.start();
    return ret;
  }

  private void flushLoop(long flushTimeMs) {
    try {
      long now = System.currentTimeMillis();
      long waitTimeMs = flushTimeMs - now + lastFlush;
      synchronized (this) {
        if (waitTimeMs > 0) {
          wait(waitTimeMs);
        }
      }
      synchronized (IngestClient.this) {
        flush();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  private StreamObserver<Rpc.StatusBulk> newStatusObserver() {

    return new StreamObserver<Rpc.StatusBulk>() {
      @Override
      public void onNext(Rpc.StatusBulk bulk) {
        for (Rpc.Status status : bulk.getStatusList()) {
          final String uuid = status.getUuid();
          final Request request = inFlightRequests.remove(uuid);
          if (request == null) {
            log.warn(
                "Received response for unknown message {}", TextFormat.shortDebugString(status));
          } else {
            synchronized (inFlightRequests) {
              inFlightRequests.notifyAll();
            }
            request.setStatus(status);
          }
        }
      }

      @Override
      public void onError(Throwable thrwbl) {
        IngestClient.this.onError(thrwbl);
      }

      @Override
      public void onCompleted() {
        synchronized (inFlightRequests) {
          inFlightRequests.clear();
        }
        closedLatch.countDown();
      }
    };
  }

  private synchronized void onError(Throwable thrwbl) {
    ingestStub = null;
    try {
      log.warn("Error on channel, closing ingestStub", thrwbl);
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException ex) {
      log.warn("Interrupted while waiting before channel open retry.", ex);
      Thread.currentThread().interrupt();
    }
    createChannelAndStub();
  }

  /**
   * Send the request.
   *
   * @param ingest the data
   * @param statusConsumer callback for receiving status
   */
  public void send(Rpc.Ingest ingest, Consumer<Rpc.Status> statusConsumer) {
    send(ingest, -1, TimeUnit.SECONDS, statusConsumer);
  }

  /**
   * Send the request with timeout.
   *
   * @param ingest the data
   * @param timeout timeout
   * @param unit time unit of timeout
   * @param statusConsumer callback for receiving status
   */
  public void send(
      Rpc.Ingest ingest, long timeout, TimeUnit unit, Consumer<Rpc.Status> statusConsumer) {

    sendTry(ingest, timeout, unit, statusConsumer, false);
  }

  /**
   * Send ingest request.
   *
   * @param key entity key value.
   * @param entity entity name.
   * @param attribute attribute name.
   * @param value ingested value.
   * @param statusConsumer callback for receiving status.
   */
  public void ingest(
      String key,
      String entity,
      String attribute,
      ByteString value,
      Consumer<Rpc.Status> statusConsumer) {
    ingest(
        UUID.randomUUID().toString(),
        key,
        entity,
        attribute,
        value,
        System.currentTimeMillis(),
        statusConsumer);
  }

  /**
   * Send ingest request.
   *
   * @param uuid request UUID.
   * @param key entity key value.
   * @param entity entity name.
   * @param attribute attribute name.
   * @param value ingested value.
   * @param statusConsumer callback for receiving status.
   */
  public void ingest(
      String uuid,
      String key,
      String entity,
      String attribute,
      ByteString value,
      Consumer<Rpc.Status> statusConsumer) {
    ingest(uuid, key, entity, attribute, value, System.currentTimeMillis(), statusConsumer);
  }

  /**
   * Send ingest request.
   *
   * @param uuid request UUID.
   * @param key entity key value.
   * @param entity entity name.
   * @param attribute attribute name.
   * @param value attribute value
   * @param stamp timestamp.
   * @param statusConsumer callback for receiving status.
   */
  public void ingest(
      String uuid,
      String key,
      String entity,
      String attribute,
      @Nullable ByteString value,
      long stamp,
      Consumer<Rpc.Status> statusConsumer) {
    Rpc.Ingest.Builder requestBuilder =
        Rpc.Ingest.newBuilder()
            .setUuid(uuid)
            .setKey(key)
            .setEntity(entity)
            .setAttribute(attribute)
            .setStamp(stamp);
    if (value == null) {
      requestBuilder.setDelete(true);
    } else {
      requestBuilder.setValue(value);
    }
    send(requestBuilder.build(), statusConsumer);
  }

  /**
   * Send delete request.
   *
   * @param uuid request UUID.
   * @param key entity key value.
   * @param entity entity name.
   * @param attribute attribute name.
   * @param stamp timestamp.
   * @param statusConsumer callback for receiving status.
   */
  public void delete(
      String uuid,
      String key,
      String entity,
      String attribute,
      long stamp,
      Consumer<Rpc.Status> statusConsumer) {
    ingest(uuid, key, entity, attribute, null, stamp, statusConsumer);
  }

  /**
   * Send delete request.
   *
   * @param key entity key value.
   * @param entity entity name.
   * @param attribute attribute name.
   * @param statusConsumer callback for receiving status.
   */
  public void delete(
      String key, String entity, String attribute, Consumer<Rpc.Status> statusConsumer) {
    delete(
        UUID.randomUUID().toString(),
        key,
        entity,
        attribute,
        System.currentTimeMillis(),
        statusConsumer);
  }

  /**
   * Send delete request.
   *
   * @param uuid request UUID.
   * @param key entity key value.
   * @param entity entity name.
   * @param attribute attribute name.
   * @param statusConsumer callback for receiving status.
   */
  public void delete(
      String uuid,
      String key,
      String entity,
      String attribute,
      Consumer<Rpc.Status> statusConsumer) {
    delete(uuid, key, entity, attribute, System.currentTimeMillis(), statusConsumer);
  }

  /**
   * Sends synchronously {@link cz.o2.proxima.proto.service.Rpc.GetRequest} to retrieve data from
   * the system.
   *
   * @param request Instance of {@link cz.o2.proxima.proto.service.Rpc.GetRequest}.
   * @return Instance of {@link cz.o2.proxima.proto.service.Rpc.GetResponse}.
   */
  public Rpc.GetResponse get(Rpc.GetRequest request) {
    ensureChannel();
    return retrieveStub.get(request);
  }

  /**
   * Sends synchronously {@link cz.o2.proxima.proto.service.Rpc.GetRequest} to retrieve data from
   * system.
   *
   * @param entity entity name.
   * @param key entity key.
   * @param attribute attribute name.
   * @return Instance of {@link cz.o2.proxima.proto.service.Rpc.GetResponse}.
   */
  public Rpc.GetResponse get(String entity, String key, String attribute) {
    Rpc.GetRequest get =
        Rpc.GetRequest.newBuilder().setEntity(entity).setKey(key).setAttribute(attribute).build();
    return get(get);
  }

  /**
   * Send synchronously {@link cz.o2.proxima.proto.service.Rpc.ListRequest} to retrieve attributes
   * for entity.
   *
   * @param request Instance of {@link cz.o2.proxima.proto.service.Rpc.ListRequest}.
   * @return Instance of {@link cz.o2.proxima.proto.service.Rpc.ListResponse}.
   */
  public Rpc.ListResponse listAttributes(Rpc.ListRequest request) {
    ensureChannel();
    return retrieveStub.listAttributes(request);
  }

  /**
   * Send synchronously {@link cz.o2.proxima.proto.service.Rpc.ListRequest} to retrieve attributes
   * for entity.
   *
   * @param entity entity name
   * @param key entity key value.
   * @return Instance of {@link cz.o2.proxima.proto.service.Rpc.ListResponse}.
   */
  public Rpc.ListResponse listAttributes(String entity, String key) {
    return listAttributes(entity, key, null, -1);
  }

  /**
   * Send synchronously {@link cz.o2.proxima.proto.service.Rpc.ListRequest} to retrieve attributes
   * for entity.
   *
   * @param entity entity name
   * @param key entity key value.
   * @param offset random offset.
   * @param limit limit of values (-1 for all).
   * @return Instance of {@link cz.o2.proxima.proto.service.Rpc.ListResponse}.
   */
  public Rpc.ListResponse listAttributes(
      String entity, String key, @Nullable String offset, int limit) {
    Rpc.ListRequest.Builder list =
        Rpc.ListRequest.newBuilder().setEntity(entity).setKey(key).setLimit(limit);
    if (offset != null) {
      list.setOffset(offset);
    }
    return listAttributes(list.build());
  }

  /** Send the request with timeout. */
  private void sendTry(
      Rpc.Ingest ingest,
      long timeout,
      TimeUnit unit,
      Consumer<Rpc.Status> statusConsumer,
      boolean isRetry) {

    if (Strings.isNullOrEmpty(ingest.getUuid())) {
      throw new IllegalArgumentException(
          "UUID cannot be null, because it is used to confirm messages.");
    }

    synchronized (this) {
      ensureChannel();
      Throwable flushExc = flushThreadExc.getAndSet(null);
      if (flushExc != null) {
        log.warn("Received exception from flush thread. Restarting flush thread.", flushExc);
        onError(flushExc);
        flushThread.set(null);
      }
    }

    flushThread.getAndUpdate(current -> current == null ? createFlushThread() : current);

    ScheduledFuture<?> scheduled = null;
    if (timeout > 0) {
      scheduled =
          timer.schedule(
              () -> {
                inFlightRequests.remove(ingest.getUuid());
                statusConsumer.accept(
                    Rpc.Status.newBuilder()
                        .setStatus(504)
                        .setStatusMessage(
                            "Timeout while waiting for response of request UUID "
                                + ingest.getUuid())
                        .build());
              },
              timeout,
              unit);
    }

    while (!isRetry && inFlightRequests.size() >= options.getMaxInflightRequests()) {
      synchronized (inFlightRequests) {
        try {
          inFlightRequests.wait(100);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          statusConsumer.accept(
              Rpc.Status.newBuilder()
                  .setStatus(417)
                  .setStatusMessage("Interrupted while waiting for the requests to settle")
                  .build());
          return;
        }
      }
    }

    inFlightRequests.putIfAbsent(ingest.getUuid(), new Request(statusConsumer, scheduled, ingest));

    synchronized (this) {
      bulkBuilder.addIngest(ingest);
      if (bulkBuilder.getIngestCount() >= options.getMaxFlushRecords()) {
        flush();
      }
    }
  }

  @VisibleForTesting
  synchronized void createChannelAndStub() {

    if (channel == null) {
      channel =
          ManagedChannelBuilder.forAddress(host, port)
              .usePlaintext()
              .executor(options.getExecutor())
              .build();
    }

    retrieveStub = RetrieveServiceGrpc.newBlockingStub(channel);
    ingestStub = IngestServiceGrpc.newStub(channel);

    ingestRequestObserver = ingestStub.ingestBulk(statusObserver);

    synchronized (inFlightRequests) {
      inFlightRequests.values().forEach(Request::retry);
    }
  }

  private void ensureChannel() {
    if (channel == null) {
      createChannelAndStub();
    }
  }

  @Override
  public void close() {

    final boolean channelNotNull;
    synchronized (this) {
      flush();
      channelNotNull = channel != null;
    }

    if (channelNotNull) {
      while (!inFlightRequests.isEmpty()) {
        synchronized (inFlightRequests) {
          try {
            inFlightRequests.wait(100);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      synchronized (this) {
        ingestRequestObserver.onCompleted();
      }

      Optional.ofNullable(flushThread.get()).ifPresent(Thread::interrupt);
      try {
        if (!closedLatch.await(1, TimeUnit.SECONDS)) {
          log.warn("Unable to await for flushThreads");
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      channel = null;
    }
  }

  private synchronized void flush() {
    if (bulkBuilder.getIngestCount() > 0) {
      if (ingestRequestObserver != null) {
        ingestRequestObserver.onNext(bulkBuilder.build());
      } else {
        log.warn("Cannot send bulk due to null observer. " + "This might suggest bug in code.");
      }
      bulkBuilder.clear();
    }
    lastFlush = System.currentTimeMillis();
  }
}
