/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import com.google.protobuf.ByteString;
import cz.o2.proxima.beam.tools.proto.service.Collect.Item;
import cz.o2.proxima.beam.tools.proto.service.Collect.Response;
import cz.o2.proxima.beam.tools.proto.service.CollectServiceGrpc;
import cz.o2.proxima.beam.tools.proto.service.CollectServiceGrpc.CollectServiceStub;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.util.ExceptionUtils;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;

// iterable that collects elements using HTTP
// this is first shot implementation with no optimizations
@Slf4j
class RemoteConsumer<T> implements AutoCloseable, Consumer<T> {

  private static final int CONTINUE = 100;
  private static final int OK = 200;

  static <T> RemoteConsumer<T> create(
      Object seed, String hostname, int preferredPort, Consumer<T> consumer, Coder<T> coder) {

    int retries = 3;
    while (retries > 0) {
      retries--;
      int port = getPort(preferredPort, System.identityHashCode(seed));
      RemoteConsumer<T> ret = null;
      try {
        // start HTTP server and store host and port
        ret = new RemoteConsumer<>(hostname, port, consumer, coder);
        ret.start();
        return ret;
      } catch (Exception ex) {
        if (ret != null) {
          ret.close();
        }
        if (isBindException(ex)) {
          log.debug("Failed to bind on port {}", port, ex);
        } else {
          throw new RuntimeException(ex);
        }
      }
    }
    throw new RuntimeException("Retries exhausted trying to start server");
  }

  private static boolean isBindException(Throwable ex) {
    return ex.getMessage().equals("Failed to bind")
        || ex.getCause() != null && isBindException(ex.getCause());
  }

  static int getPort(int preferredPort, int seed) {
    return preferredPort > 0
        ? preferredPort
        : ((ThreadLocalRandom.current().nextInt() ^ seed) & Integer.MAX_VALUE) % 50000 + 10000;
  }

  private class CollectService extends CollectServiceGrpc.CollectServiceImplBase {

    private final List<CompletableFuture<Void>> unterminatedCalls =
        Collections.synchronizedList(new ArrayList<>());

    @Override
    public StreamObserver<Item> collect(StreamObserver<Response> responseObserver) {
      CompletableFuture<Void> terminateFuture = new CompletableFuture<>();
      unterminatedCalls.add(terminateFuture);
      // send initial status to client
      responseObserver.onNext(Response.newBuilder().setStatus(CONTINUE).build());
      return new StreamObserver<Item>() {

        List<Item> received = new ArrayList<>();

        @Override
        public void onNext(Item item) {
          received.add(item);
          if (received.size() > 10) {
            flush();
          }
        }

        @Override
        public void onError(Throwable throwable) {
          log.warn("Error in collect()", throwable);
          terminateFuture.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
          flush();
          responseObserver.onNext(Response.newBuilder().setStatus(OK).setStatusCode("OK").build());
          responseObserver.onCompleted();
          terminateFuture.complete(null);
        }

        private void flush() {
          synchronized (RemoteConsumer.this) {
            received.forEach(
                i ->
                    consumer.accept(
                        ExceptionUtils.uncheckedFactory(
                            () -> deserialize(i.getSerialized().newInput()))));
            received.clear();
          }
        }
      };
    }

    public void awaitAllClosed() {
      final CountDownLatch latch;
      synchronized (unterminatedCalls) {
        latch = new CountDownLatch(unterminatedCalls.size());
        unterminatedCalls.forEach(
            f ->
                f.whenComplete(
                    (ign, exc) -> {
                      if (exc != null) {
                        log.warn("Error waiting for termination of ongoing call. Ignored.", exc);
                      }
                      latch.countDown();
                    }));
      }
      ExceptionUtils.ignoringInterrupted(latch::await);
    }
  }

  private class Server implements AutoCloseable {

    private final io.grpc.Server server;
    private final CollectService service = new CollectService();

    private Server(int port) {
      server =
          ServerBuilder.forPort(port)
              .executor(Executors.newCachedThreadPool())
              .addService(service)
              .build();
    }

    void run() throws IOException {
      server.start();
    }

    @Override
    public void close() {
      service.awaitAllClosed();
      server.shutdown();
    }
  }

  private final Coder<T> coder;
  private final String hostname;
  private final int port;
  private final transient Consumer<T> consumer;
  private final transient Server server;
  private transient CollectServiceStub stub;
  private transient ManagedChannel channel;
  private transient StreamObserver<Item> observer;
  private transient CompletableFuture<Void> terminateFuture;

  private RemoteConsumer(String hostname, int port, Consumer<T> consumer, Coder<T> coder) {
    this.server = new Server(port);
    this.hostname = hostname;
    this.port = port;
    this.consumer = consumer;
    this.coder = coder;
  }

  StreamObserver<Item> observer() {
    if (channel == null) {
      channel = ManagedChannelBuilder.forAddress(hostname, port).usePlaintext().build();
      CountDownLatch connected = new CountDownLatch(1);
      channel.notifyWhenStateChanged(ConnectivityState.READY, connected::countDown);
      ExceptionUtils.ignoringInterrupted(
          () -> {
            if (!connected.await(1, TimeUnit.SECONDS)) {
              log.warn("Timeout waiting for channel to become connected. Skipping.");
            }
          });
    }
    if (stub == null) {
      stub = CollectServiceGrpc.newStub(channel);
    }
    if (observer == null) {
      terminateFuture = new CompletableFuture<>();
      CountDownLatch initLatch = new CountDownLatch(1);
      observer =
          stub.collect(
              new StreamObserver<Response>() {
                @Override
                public void onNext(Response response) {
                  if (response.getStatus() == CONTINUE) {
                    initLatch.countDown();
                  } else if (response.getStatus() == OK) {
                    terminateFuture.complete(null);
                  }
                }

                @Override
                public void onError(Throwable throwable) {
                  terminateFuture.completeExceptionally(throwable);
                  observer = null;
                }

                @Override
                public void onCompleted() {
                  // nop
                }
              });
      initLatch.countDown();
    }
    return observer;
  }

  @Override
  public void accept(T what) {
    try {
      Item item = Item.newBuilder().setSerialized(ByteString.readFrom(serialize(what))).build();
      observer().onNext(item);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  void stop() {
    if (channel != null) {
      Throwable errCaught = null;
      if (observer != null) {
        observer.onCompleted();
        try {
          terminateFuture.get();
        } catch (Exception ex) {
          log.error("Error waiting for observer to terminate.", ex);
          errCaught = ex;
        }
        observer = null;
      }
      channel.shutdown();
      channel = null;
      if (errCaught != null) {
        throw new RuntimeException(errCaught);
      }
    }
    if (server != null) {
      server.close();
    }
  }

  void start() throws IOException {
    server.run();
  }

  InputStream serialize(T what) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode(what, baos);
      return new ByteArrayInputStream(baos.toByteArray());
    }
  }

  T deserialize(InputStream in) throws IOException {
    return coder.decode(in);
  }

  @Override
  public void close() {
    stop();
  }
}
