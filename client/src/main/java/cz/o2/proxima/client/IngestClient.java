/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

import cz.o2.proxima.proto.service.IngestServiceGrpc;
import cz.o2.proxima.proto.service.IngestServiceGrpc.IngestServiceStub;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import cz.seznam.euphoria.shaded.guava.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Strings;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client being able to connect and write requests to the ingest gateway.
 */
public class IngestClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(IngestClient.class);

  // request sent through the channel
  private class Request {
    @Getter
    final Consumer<Rpc.Status> consumer;
    @Getter
    final ScheduledFuture timeoutFuture;
    @Getter
    final Rpc.Ingest payload;
    Request(
        Consumer<Rpc.Status> consumer,
        ScheduledFuture timeoutFuture,
        Rpc.Ingest payload) {
      this.consumer = consumer;
      this.timeoutFuture = timeoutFuture;
      this.payload = payload;
    }

    // confirm the status and remove the timeout schedule
    private void setStatus(Rpc.Status s) {
      if (timeoutFuture == null || timeoutFuture.cancel(false)) {
        consumer.accept(s);
      }
    }

    // retry to send the request
    void retry() {
      // we don't setup any timeout
      sendTry(payload, -1L, TimeUnit.MILLISECONDS, consumer, true);
    }
  }

  public static IngestClient create(String host, int port) {
    return create(host, port, new Options());
  }

  public static IngestClient create(String host, int port, Options opts) {
    return new IngestClient(host, port, opts);
  }

  @Getter
  private final String host;
  @Getter
  private final int port;
  @Getter
  private final Options options;

  /** Map of UUID of message to the consumer of the message status. */
  private final Map<String, Request> inflightRequests = Collections.synchronizedMap(new HashMap<>());

  Channel channel = null;
  IngestServiceStub stub = null;
  RetrieveServiceGrpc.RetrieveServiceBlockingStub getStub = null;
  final Rpc.IngestBulk.Builder bulkBuilder = Rpc.IngestBulk.newBuilder();
  final CountDownLatch closedLatch = new CountDownLatch(1);
  final StreamObserver<Rpc.StatusBulk> statusObserver = new StreamObserver<Rpc.StatusBulk>() {

    @Override
    public void onNext(Rpc.StatusBulk bulk) {
      for (Rpc.Status status : bulk.getStatusList()) {
        final String uuid = status.getUuid();
        final Request request = inflightRequests.remove(uuid);
        if (request == null) {
          LOG.warn("Received response for unknown message " + status);
        } else {
          synchronized (inflightRequests) {
            inflightRequests.notifyAll();
          }
          request.setStatus(status);
        }
      }
    }

    @Override
    public void onError(Throwable thrwbl) {
      LOG.warn("Error on channel, closing stub", thrwbl);
      synchronized (IngestClient.this) {
        stub = null;
        createChannelAndStub();
      }
    }

    @Override
    public void onCompleted() {
      synchronized (inflightRequests) {
        inflightRequests.clear();
      }
      closedLatch.countDown();
    }

  };

  private final Thread flushThread;

  StreamObserver<Rpc.IngestBulk> requestObserver;

  private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);

  @VisibleForTesting
  IngestClient(String host, int port, Options options) {
    this.host = host;
    this.port = port;
    this.options = options;
    this.flushThread = new Thread(() -> {
      long flushTimeNanos = options.getFlushUsec() * 1_000L;
      long lastFlush = System.nanoTime();
      while (!Thread.currentThread().isInterrupted()) {
        try {
          long nowNanos = System.nanoTime();
          long waitTimeNanos = flushTimeNanos - nowNanos + lastFlush;
          synchronized (this) {
            if (waitTimeNanos > 0) {
              wait(waitTimeNanos / 1_000_000L, (int) (waitTimeNanos % 1_000_000L));
            }
          }
          synchronized (IngestClient.this) {
            if (bulkBuilder.getIngestCount() > 0) {
              flush();
            }
            lastFlush = nowNanos;
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    });

    this.flushThread.setDaemon(true);
    this.flushThread.setName(getClass().getSimpleName() + "-flushThread");
  }

  /** Send the request. */
  public void send(Rpc.Ingest ingest, Consumer<Rpc.Status> statusConsumer) {
    send(ingest, -1, TimeUnit.SECONDS, statusConsumer);
  }

  /** Send the request with timeout. */
  public void send(Rpc.Ingest ingest, long timeout,
      TimeUnit unit, Consumer<Rpc.Status> statusConsumer) {
    sendTry(ingest, timeout, unit, statusConsumer, false);
  }

  /**
   * Sends synchronously {@link cz.o2.proxima.proto.service.Rpc.GetRequest}
   * to retrieve data from the system.
   *
   * @param request Instance of {@link cz.o2.proxima.proto.service.Rpc.GetRequest}.
   * @return Instance of {@link cz.o2.proxima.proto.service.Rpc.GetResponse}.
   */
  public Rpc.GetResponse get(Rpc.GetRequest request) {
     ensureChannel();
     return getStub.get(request);
  }

  /** Send the request with timeout. */
  private void sendTry(
      Rpc.Ingest ingest, long timeout,
      TimeUnit unit, Consumer<Rpc.Status> statusConsumer,
      boolean isRetry) {


    if (Strings.isNullOrEmpty(ingest.getUuid())) {
      throw new IllegalArgumentException(
          "UUID cannot be null, because it is used to confirm messages.");
    }

    synchronized (this) {
      if (!flushThread.isAlive()) {
        flushThread.start();
      }
      ensureChannel();
    }

    while (!isRetry && inflightRequests.size() >= options.getMaxInflightRequests()) {
      synchronized (inflightRequests) {
        try {
          inflightRequests.wait(100);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          statusConsumer.accept(Rpc.Status.newBuilder()
              .setStatus(417)
              .setStatusMessage("Interrupted while waiting for the requests to settle")
              .build());
          return;
        }
      }
    }

    ScheduledFuture<?> scheduled = null;
    if (timeout > 0) {
      scheduled = timer.schedule(() -> {
        inflightRequests.remove(ingest.getUuid());
        statusConsumer.accept(Rpc.Status.newBuilder()
            .setStatus(504)
            .setStatusMessage(
                "Timeout while waiting for response of request UUID " + ingest.getUuid())
            .build());
      }, timeout, unit);
    }

    inflightRequests.putIfAbsent(ingest.getUuid(),
        new Request(statusConsumer, scheduled, ingest));

    synchronized (this) {
      bulkBuilder.addIngest(ingest);
      if (bulkBuilder.getIngestCount() >= options.getMaxFlushRecords()) {
        synchronized (flushThread) {
          flushThread.notify();
        }
      }
    }

  }

  @VisibleForTesting
  void createChannelAndStub() {

    if (channel == null) {
      channel = ManagedChannelBuilder
          .forAddress(host, port)
          .usePlaintext(true)
          .executor(options.getExecutor())
          .build();
    }

    getStub = RetrieveServiceGrpc.newBlockingStub(channel);
    stub = IngestServiceGrpc.newStub(channel);

    requestObserver = stub.ingestBulk(statusObserver);

    synchronized (inflightRequests) {
      inflightRequests.values().forEach(Request::retry);
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
      while (!inflightRequests.isEmpty()) {
        synchronized (inflightRequests) {
          try {
            inflightRequests.wait(100);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      synchronized (this) {
        requestObserver.onCompleted();
      }

      flushThread.interrupt();
      try {
        closedLatch.await(1, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      channel = null;
    }
  }

  private void flush() {
    if (requestObserver != null) {
      requestObserver.onNext(bulkBuilder.build());
    }
    bulkBuilder.clear();
  }

}
