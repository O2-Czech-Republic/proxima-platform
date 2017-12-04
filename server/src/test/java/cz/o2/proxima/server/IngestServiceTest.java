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

package cz.o2.proxima.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.server.IngestServer;
import cz.o2.proxima.server.test.Test.ExtendedMessage;
import cz.o2.proxima.storage.InMemBulkStorage;
import cz.o2.proxima.storage.InMemStorage;
import cz.o2.proxima.test.Scheme;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test server API.
 */
public class IngestServiceTest {

  IngestServer server;
  IngestServer.IngestService ingest;

  BlockingQueue<Rpc.Status> responses;
  StreamObserver<Rpc.StatusBulk> responseObserver;
  CountDownLatch latch;

  @Before
  public void setup() throws InterruptedException {
    server = new IngestServer(ConfigFactory.load().resolve());
    ingest = server.new IngestService();
    server.startConsumerThreads();
    latch = new CountDownLatch(1);

    responses = new LinkedBlockingQueue<>();

    responseObserver = new StreamObserver<Rpc.StatusBulk>() {

      @Override
      public void onNext(Rpc.StatusBulk status) {
        for (Rpc.Status s : status.getStatusList()) {
          responses.add(s);
        }
      }

      @Override
      public void onError(Throwable thrwbl) {

      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }
    };
  }


  @Test(timeout = 2000)
  public void testIngestBulkInvalidScheme() throws InterruptedException {

    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
    result.onNext(bulk(Rpc.Ingest.newBuilder()
        .setEntity("gateway")
        .setAttribute("fail")
        .setKey("gateway1")
        .setValue(ByteString.EMPTY)
        .build()));

    result.onCompleted();
    latch.await();
    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(412, status.getStatus());
  }

  @Test(timeout = 2000)
  public void testIngestBulkInvalidEntity() throws InterruptedException {

    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
    result.onNext(bulk(Rpc.Ingest.newBuilder()
        .setEntity("gateway-invalid")
        .setAttribute("fail")
        .setKey("gateway1")
        .setValue(ByteString.EMPTY)
        .build()));

    result.onCompleted();
    latch.await();

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(404, status.getStatus());
  }

  @Test(timeout = 2000)
  public void testIngestBulkInvalidEntityAttribute() throws InterruptedException {

    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
    result.onNext(bulk(Rpc.Ingest.newBuilder()
        .setEntity("gateway")
        .setAttribute("fail-invalid")
        .setKey("gateway1")
        .setValue(ByteString.EMPTY)
        .build()));

    result.onCompleted();
    latch.await();

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(404, status.getStatus());
  }

  @Test(timeout = 2000)
  public void testIngestBulkMissingKey() throws InterruptedException {

    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
    result.onNext(bulk(Rpc.Ingest.newBuilder()
        .setEntity("dummy")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setValue(ByteString.EMPTY)
        .build()));

    result.onCompleted();
    latch.await();

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(400, status.getStatus());

  }


  @Test(timeout = 2000)
  public void testIngestBulkValid() throws InterruptedException {

    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
    result.onNext(bulk(Rpc.Ingest.newBuilder()
        .setEntity("dummy")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setKey("my-dummy-entity")
        .setValue(ByteString.EMPTY)
        .build()));

    result.onCompleted();
    latch.await();

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
    Map<String, byte[]> data = storage.getData();
    assertEquals(1, data.size());
    assertArrayEquals(
        new byte[0], data.get("/proxima/dummy/my-dummy-entity#data"));
  }

  @Test(timeout = 5000)
  public void testIngestBulkWildcardEntityAttribute() throws Exception {

    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
    result.onNext(bulk(
        Rpc.Ingest.newBuilder()
          .setEntity("dummy")
          .setAttribute("wildcard.1234")
          .setKey("dummy1")
          .setValue(Scheme.Device.newBuilder()
              .setType("motion")
              .setPayload(ByteString.copyFromUtf8("muhehe"))
              .build()
              .toByteString())
          .build(),
        Rpc.Ingest.newBuilder()
        .setEntity("dummy")
        .setAttribute("wildcard.12345")
        .setKey("dummy2")
        .setValue(Scheme.Device.newBuilder()
            .setType("motion")
            .setPayload(ByteString.copyFromUtf8("muhehe"))
            .build()
            .toByteString())
        .build()));


    result.onCompleted();
    latch.await();

    assertEquals(2, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
    Map<String, byte[]> data = storage.getData();
    assertEquals(2, data.size());
    assertEquals("muhehe", Scheme.Device.parseFrom(
        data.get("/proxima/dummy/dummy1#wildcard.1234"))
        .getPayload().toStringUtf8());
    assertEquals("muhehe", Scheme.Device.parseFrom(
        data.get("/proxima/dummy/dummy2#wildcard.12345"))
        .getPayload().toStringUtf8());
  }

  @Test(timeout = 2000)
  public void testIngestWildcardEntityInvalidScheme() throws InterruptedException {

    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
    result.onNext(bulk(Rpc.Ingest.newBuilder()
        .setEntity("dummy")
        .setAttribute("wildcard.1234")
        .setKey("dummy1")
        .setValue(ByteString.copyFromUtf8("muhehe"))
        .build()));

    result.onCompleted();
    latch.await();

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(412, status.getStatus());
  }

  private Rpc.IngestBulk bulk(Rpc.Ingest... ingests) {
    Rpc.IngestBulk.Builder ret = Rpc.IngestBulk.newBuilder();
    for (Rpc.Ingest ingest : ingests) {
      ret.addIngest(ingest);
    }
    return ret.build();
  }


  @Test(timeout = 2000)
  public void testIngestSingleValid() throws InterruptedException {

    latch = new CountDownLatch(1);

    StreamObserver<Rpc.Ingest> result = ingest.ingestSingle(new StreamObserver<Rpc.Status>() {

      @Override
      public void onNext(Rpc.Status status) {
        responses.add(status);
      }

      @Override
      public void onError(Throwable thrwbl) {
        // nop
      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }

    });
    result.onNext(Rpc.Ingest.newBuilder()
        .setEntity("dummy")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setKey("my-dummy-entity")
        .setValue(ByteString.EMPTY)
        .build());

    result.onCompleted();
    latch.await();

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
    Map<String, byte[]> data = storage.getData();
    assertEquals(1, data.size());
    assertTrue(data.containsKey("/proxima/dummy/my-dummy-entity#data"));
  }

  @Test(timeout = 2000)
  public void testIngestSingleValidButFilteredOut() throws InterruptedException {

    latch = new CountDownLatch(1);

    StreamObserver<Rpc.Ingest> result = ingest.ingestSingle(new StreamObserver<Rpc.Status>() {

      @Override
      public void onNext(Rpc.Status status) {
        responses.add(status);
      }

      @Override
      public void onError(Throwable thrwbl) {
        // nop
      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }

    });
    result.onNext(Rpc.Ingest.newBuilder()
        .setEntity("dummy")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setKey("my-dummy-entity")
        // the filter filters out everything with value length of two
        .setValue(ByteString.copyFrom(new byte[] { 0, 1 }))
        .build());

    result.onCompleted();
    latch.await();

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
    Map<String, byte[]> data = storage.getData();
    // although we have the filter applied here,
    // we cannot filter the ingest out, because it goes directly to the
    // output (without commit log in the middle)
    assertFalse(data.isEmpty());
  }



  @Test(timeout = 2000)
  public void testIngestValid() throws InterruptedException {

    Rpc.Ingest request = Rpc.Ingest.newBuilder()
        .setEntity("dummy")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setKey("my-dummy-entity")
        .setValue(ByteString.EMPTY)
        .build();

    flushToIngest(request);

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
    Map<String, byte[]> data = storage.getData();
    assertEquals(1, data.size());
    assertTrue(data.containsKey("/proxima/dummy/my-dummy-entity#data"));
  }


  @Test(timeout = 2000)
  public void testIngestValidBulk() throws InterruptedException {

    Rpc.Ingest request = Rpc.Ingest.newBuilder()
        .setEntity("event")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setKey("my-dummy-entity")
        .setValue(ByteString.EMPTY)
        .build();

    flushToIngest(request);

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemBulkStorage storage = (InMemBulkStorage) server.repo.getStorageDescriptor("inmem-bulk");
    Map<String, byte[]> data = storage.getData();
    assertEquals(1, data.size());
    assertTrue(data.containsKey("/proxima_events/bulk/my-dummy-entity#data"));
  }

  @Test(timeout = 2000)
  public void testIngestValidExtendedScheme()
      throws InterruptedException, InvalidProtocolBufferException {


    ExtendedMessage payload = ExtendedMessage.newBuilder()
        .setFirst(1).setSecond(2).build();

    Rpc.Ingest request = Rpc.Ingest.newBuilder()
        .setEntity("test")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setKey("my-dummy-entity")
        .setValue(payload.toByteString())
        .build();

    flushToIngest(request);

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
    Map<String, byte[]> data = storage.getData();
    assertEquals(2, data.size());
    byte[] value = data.get("/test_inmem/my-dummy-entity#data");
    assertTrue(value != null);
    assertEquals(payload, ExtendedMessage.parseFrom(value));
    value = data.get("/test_inmem/random/my-dummy-entity#data");
    assertTrue(value != null);
    assertEquals(payload, ExtendedMessage.parseFrom(value));
  }

  @Test(timeout = 2000)
  public void testTransform() throws InterruptedException {
    // write event.data and check that we receive write to dummy.wildcard.<stamp>
    long now = System.currentTimeMillis();
    Rpc.Ingest request = Rpc.Ingest.newBuilder()
        .setEntity("event")
        .setAttribute("data")
        .setUuid(UUID.randomUUID().toString())
        .setKey("my-dummy-entity")
        .setValue(ByteString.EMPTY)
        .setStamp(now)
        .build();

    flushToIngest(request);

    assertEquals(1, responses.size());
    Rpc.Status status = responses.poll();
    assertEquals(200, status.getStatus());

    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
    Map<String, byte[]> data = storage.getData();
    assertEquals(2, data.size());
    byte[] value = data.get("/proxima_events/my-dummy-entity#data");
    assertTrue(value != null);
    value = data.get("/proxima/dummy/my-dummy-entity#wildcard." + now);
    assertTrue(value != null);
  }

  private void flushToIngest(Rpc.Ingest request) throws InterruptedException {

    ingest.ingest(request, new StreamObserver<Rpc.Status>() {

      @Override
      public void onNext(Rpc.Status status) {
        responses.add(status);
      }

      @Override
      public void onError(Throwable thrwbl) {
        // nop
      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }

    });

    latch.await();
  }

}
