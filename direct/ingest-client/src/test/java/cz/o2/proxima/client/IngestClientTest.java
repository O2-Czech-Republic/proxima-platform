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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ByteString;
import cz.o2.proxima.proto.service.IngestServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

/** Test {@code IngestClient} functionality. */
public class IngestClientTest {

  private final String host = "localhost";
  private final int port = 4001;
  private List<Rpc.Ingest> ingested;
  private Deque<Rpc.Status> statuses;

  @Before
  public void setUp() {
    ingested = new ArrayList<>();
    statuses = new LinkedList<>();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingUuid() {
    IngestClient client = create(new Options());
    client.send(
        Rpc.Ingest.newBuilder()
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {});
  }

  @Test(timeout = 10000)
  public void testSingleRequest() throws InterruptedException {
    IngestClient client = create(new Options());
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Rpc.Status> status = new AtomicReference<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {
          status.set(s);
          latch.countDown();
        });
    latch.await();
    assertNotNull(status.get());
    assertEquals(200, status.get().getStatus());
  }

  @Test(timeout = 10000)
  public void testSingleSimpleIngest() throws InterruptedException {
    IngestClient client = create(new Options());
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Rpc.Status> status = new AtomicReference<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    client.ingest(
        "gw1",
        "gateway",
        "armed",
        ByteString.EMPTY,
        s -> {
          status.set(s);
          latch.countDown();
        });
    latch.await();
    assertNotNull(status.get());
    assertEquals(200, status.get().getStatus());
  }

  @Test(timeout = 10000)
  public void testSingleSimpleDelete() throws InterruptedException {
    IngestClient client = create(new Options());
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Rpc.Status> status = new AtomicReference<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    client.delete(
        "gw1",
        "gateway",
        "armed",
        s -> {
          status.set(s);
          latch.countDown();
        });
    latch.await();
    assertNotNull(status.get());
    assertEquals(200, status.get().getStatus());
  }

  @Test(timeout = 10000)
  public void testSingleDelete() throws InterruptedException {
    IngestClient client = create(new Options());
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Rpc.Status> status = new AtomicReference<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    client.delete(
        UUID.randomUUID().toString(),
        "gw1",
        "gateway",
        "armed",
        System.currentTimeMillis(),
        s -> {
          status.set(s);
          latch.countDown();
        });
    latch.await();
    assertNotNull(status.get());
    assertEquals(200, status.get().getStatus());
  }

  @Test(timeout = 10000)
  public void testMultiDelete() throws InterruptedException {
    IngestClient client = create(new Options());
    CountDownLatch latch = new CountDownLatch(2);
    List<Rpc.Status> received = new ArrayList<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    client.delete(
        UUID.randomUUID().toString(),
        "gw1",
        "gateway",
        "armed",
        System.currentTimeMillis(),
        s -> {
          received.add(s);
          latch.countDown();
        });
    client.delete(
        UUID.randomUUID().toString(),
        "gw1",
        "gateway",
        "armed",
        System.currentTimeMillis(),
        s -> {
          received.add(s);
          latch.countDown();
        });
    latch.await();
    assertEquals(2, received.size());
  }

  @Test(timeout = 10000)
  public void testMultiRequests() throws InterruptedException {
    IngestClient client = create(new Options());
    CountDownLatch latch = new CountDownLatch(2);
    List<Rpc.Status> received = new ArrayList<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {
          received.add(s);
          latch.countDown();
        });
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {
          received.add(s);
          latch.countDown();
        });

    latch.await();
    assertEquals(2, received.size());
  }

  @Test(timeout = 10000)
  public void testMultiRequestsInterruptFlushThread() throws InterruptedException {
    IngestClient client = create(new Options());
    AtomicReference<Thread> flushThread = client.getFlushThread();
    CountDownLatch latch = new CountDownLatch(2);
    List<Rpc.Status> received = new ArrayList<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {
          received.add(s);
          latch.countDown();
        });
    Thread flush = flushThread.get();
    assertNotNull(flush);
    flush.interrupt();
    while (flush.equals(flushThread.get())) {}
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {
          received.add(s);
          latch.countDown();
        });

    latch.await();
    assertEquals(2, received.size());
  }

  @Test(timeout = 10000)
  public void testMultiRequestsWithConfirmError() throws InterruptedException {
    IngestClient client = create(new Options());
    AtomicReference<Thread> flushThread = client.getFlushThread();
    CountDownLatch latch = new CountDownLatch(2);
    List<Rpc.Status> received = new ArrayList<>();
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    statuses.add(Rpc.Status.newBuilder().setStatus(200).build());
    AtomicInteger retries = new AtomicInteger();
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {
          if (retries.incrementAndGet() == 0) {
            throw new RuntimeException("Fail");
          }
          received.add(s);
          latch.countDown();
        });
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        s -> {
          received.add(s);
          latch.countDown();
        });

    latch.await();
    assertEquals(2, received.size());
  }

  @Test(timeout = 10000)
  public void testTimeoutRequest() throws InterruptedException {
    IngestClient client = create(new Options());
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Rpc.Status> status = new AtomicReference<>();
    client.send(
        Rpc.Ingest.newBuilder()
            .setUuid(UUID.randomUUID().toString())
            .setKey("gw1")
            .setEntity("gateway")
            .setAttribute("armed")
            .setValue(ByteString.EMPTY)
            .build(),
        500,
        TimeUnit.MILLISECONDS,
        s -> {
          status.set(s);
          latch.countDown();
        });
    latch.await();
    assertNotNull(status.get());
    assertEquals(504, status.get().getStatus());
  }

  private IngestClient create(Options opts) {
    return new IngestClient(host, port, opts) {

      @Override
      void createChannelAndStub() {
        this.channel = mockChannel();
        this.ingestStub = IngestServiceGrpc.newStub(channel);
        this.ingestRequestObserver =
            new StreamObserver<Rpc.IngestBulk>() {

              @Override
              public void onNext(Rpc.IngestBulk bulk) {
                ingested.addAll(bulk.getIngestList());
                Rpc.StatusBulk.Builder responses = Rpc.StatusBulk.newBuilder();
                bulk.getIngestList()
                    .forEach(
                        i -> {
                          if (!statuses.isEmpty()) {
                            responses.addStatus(statuses.pop().toBuilder().setUuid(i.getUuid()));
                          }
                        });
                statusObserver.onNext(responses.build());
              }

              @Override
              public void onError(Throwable thrwbl) {}

              @Override
              public void onCompleted() {}
            };
      }
    };
  }

  private Channel mockChannel() {
    return mock(Channel.class);
  }
}
