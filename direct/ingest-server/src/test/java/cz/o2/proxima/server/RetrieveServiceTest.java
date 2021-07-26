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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.server.test.Test.ExtendedMessage;
import cz.o2.proxima.server.transaction.TransactionContext;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.util.Optionals;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;

/** Test server API. */
public class RetrieveServiceTest {

  IngestServer server;
  RetrieveService retrieve;
  Map<String, List<KeyAttribute>> transactionUpdates;

  @Before
  public void setup() {
    server =
        new IngestServer(
            ConfigFactory.load("test-reference.conf")
                .withFallback(ConfigFactory.load("test-ingest-server.conf"))
                .resolve());
    transactionUpdates = new HashMap<>();
    retrieve =
        new RetrieveService(server.repo, server.direct, new TransactionContext(server.direct)) {
          @Override
          void updateTransaction(String transactionId, List<KeyAttribute> keyAttributes)
              throws TransactionRejectedException {
            transactionUpdates.compute(
                transactionId,
                (k, v) -> {
                  if (v == null) {
                    v = new ArrayList<>();
                  }
                  v.addAll(keyAttributes);
                  return v;
                });
          }
        };
    server.runReplications();
  }

  @Test
  public void testGetWithMissingFields() {

    final Rpc.GetRequest request = Rpc.GetRequest.newBuilder().build();
    final List<Rpc.GetResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.GetResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.GetResponse>() {
          @Override
          public void onNext(Rpc.GetResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.get(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.GetResponse response = responses.get(0);
    assertEquals(400, response.getStatus());
  }

  @Test
  public void testListWithMissingFields() {

    final Rpc.ListRequest request = Rpc.ListRequest.newBuilder().build();
    final List<Rpc.ListResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.listAttributes(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(400, response.getStatus());
  }

  @Test
  public void testGetValid() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("data");
    String key = "my-fancy-entity-key";
    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                attribute.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (s, err) -> {});
    Rpc.GetRequest request =
        Rpc.GetRequest.newBuilder()
            .setEntity(entity.getName())
            .setAttribute(attribute.getName())
            .setKey(key)
            .build();

    final List<Rpc.GetResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.GetResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.GetResponse>() {
          @Override
          public void onNext(Rpc.GetResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.get(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.GetResponse response = responses.get(0);
    assertEquals(
        "Error: " + response.getStatus() + ": " + response.getStatusMessage(),
        200,
        response.getStatus());
    assertArrayEquals(new byte[] {1, 2, 3}, response.getValue().toByteArray());
  }

  @Test
  public void testGetValidWithTransaction() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("data");
    String key = "my-fancy-entity-key";
    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                1000L,
                key,
                attribute.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (s, err) -> {});
    String transactionId = UUID.randomUUID().toString();
    Rpc.GetRequest request =
        Rpc.GetRequest.newBuilder()
            .setEntity(entity.getName())
            .setAttribute(attribute.getName())
            .setKey(key)
            .setTransactionId(transactionId)
            .build();

    final List<Rpc.GetResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.GetResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.GetResponse>() {
          @Override
          public void onNext(Rpc.GetResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.get(request, responseObserver);
    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.GetResponse response = responses.get(0);
    assertEquals(
        "Error: " + response.getStatus() + ": " + response.getStatusMessage(),
        200,
        response.getStatus());
    assertArrayEquals(new byte[] {1, 2, 3}, response.getValue().toByteArray());
    assertEquals(1, transactionUpdates.size());
    assertTrue(transactionUpdates.containsKey(transactionId));
    assertEquals(
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(entity, key, attribute, 1000L)),
        transactionUpdates.get(transactionId));
  }

  @Test
  public void testGetNotFound() {
    final Rpc.GetRequest request =
        Rpc.GetRequest.newBuilder()
            .setEntity("dummy")
            .setAttribute("dummy")
            .setKey("some-not-existing-key")
            .build();
    final List<Rpc.GetResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.GetResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.GetResponse>() {
          @Override
          public void onNext(Rpc.GetResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.get(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.GetResponse response = responses.get(0);
    assertEquals(404, response.getStatus());
  }

  @Test
  public void testGetNotFoundWithTransaction() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("data");
    String key = "some-not-existing-key";
    String transactionId = UUID.randomUUID().toString();
    final Rpc.GetRequest request =
        Rpc.GetRequest.newBuilder()
            .setEntity("dummy")
            .setAttribute("data")
            .setKey(key)
            .setTransactionId(transactionId)
            .build();
    final List<Rpc.GetResponse> responses = new ArrayList<>();
    final StreamObserver<Rpc.GetResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.GetResponse>() {
          @Override
          public void onNext(Rpc.GetResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {}
        };

    retrieve.get(request, responseObserver);
    assertEquals(1, responses.size());
    Rpc.GetResponse response = responses.get(0);
    assertEquals(404, response.getStatus());
    assertEquals(1, transactionUpdates.size());
    assertTrue(transactionUpdates.containsKey(transactionId));
    assertEquals(
        Collections.singletonList(KeyAttributes.ofMissingAttribute(entity, key, attribute)),
        transactionUpdates.get(transactionId));
  }

  @Test
  public void testListValid() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("wildcard.*");
    String key = "my-fancy-entity-key";

    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                "wildcard.1",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (s, err) -> {});
    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                "wildcard.2",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3, 4}),
            (s, err) -> {});

    Rpc.ListRequest request =
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setWildcardPrefix("wildcard")
            .setKey(key)
            .build();

    List<Rpc.ListResponse> responses = new ArrayList<>();
    AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.listAttributes(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(200, response.getStatus());
    assertEquals(2, response.getValueCount());
    assertEquals("wildcard.1", response.getValue(0).getAttribute());
    assertArrayEquals(new byte[] {1, 2, 3}, response.getValue(0).getValue().toByteArray());
    assertEquals("wildcard.2", response.getValue(1).getAttribute());
    assertArrayEquals(new byte[] {1, 2, 3, 4}, response.getValue(1).getValue().toByteArray());
  }

  @Test
  public void testListValidWithTransaction() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("wildcard.*");
    String key = "my-fancy-entity-key";

    List<StreamElement> elements =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attribute,
                1000L,
                key,
                "wildcard.1",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            StreamElement.upsert(
                entity,
                attribute,
                1001L,
                key,
                "wildcard.2",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3, 4}));

    elements.forEach(
        el ->
            Optionals.get(server.direct.getWriter(el.getAttributeDescriptor()))
                .write(el, (succ, exc) -> {}));

    String transactionId = UUID.randomUUID().toString();
    Rpc.ListRequest request =
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setWildcardPrefix("wildcard")
            .setKey(key)
            .setTransactionId(transactionId)
            .build();

    List<Rpc.ListResponse> responses = new ArrayList<>();
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {}
        };

    retrieve.listAttributes(request, responseObserver);
    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(200, response.getStatus());
    assertEquals(2, response.getValueCount());
    assertEquals("wildcard.1", response.getValue(0).getAttribute());
    assertArrayEquals(new byte[] {1, 2, 3}, response.getValue(0).getValue().toByteArray());
    assertEquals("wildcard.2", response.getValue(1).getAttribute());
    assertArrayEquals(new byte[] {1, 2, 3, 4}, response.getValue(1).getValue().toByteArray());
    assertEquals(1, transactionUpdates.size());
    assertTrue(transactionUpdates.containsKey(transactionId));
    assertEquals(
        KeyAttributes.ofWildcardQueryElements(entity, key, attribute, elements),
        transactionUpdates.get(transactionId));
  }

  @Test
  public void testListValidWithOffset() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("wildcard.*");
    String key = "my-fancy-entity-key";

    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                "wildcard.1",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (s, err) -> {});
    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                "wildcard.2",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3, 4}),
            (s, err) -> {});

    Rpc.ListRequest request =
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setWildcardPrefix("wildcard")
            .setKey(key)
            .setOffset("wildcard.1")
            .build();

    final List<Rpc.ListResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.listAttributes(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(200, response.getStatus());
    assertEquals(1, response.getValueCount());
    assertEquals("wildcard.2", response.getValue(0).getAttribute());
    assertArrayEquals(new byte[] {1, 2, 3, 4}, response.getValue(0).getValue().toByteArray());
  }

  @Test
  public void testListValidWithOffsetAndTransaction() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("wildcard.*");
    String key = "my-fancy-entity-key";

    Rpc.ListRequest request =
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setWildcardPrefix("wildcard")
            .setKey(key)
            .setOffset("wildcard.1")
            .setTransactionId(UUID.randomUUID().toString())
            .build();

    final List<Rpc.ListResponse> responses = new ArrayList<>();
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {}
        };

    retrieve.listAttributes(request, responseObserver);
    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(400, response.getStatus());
  }

  @Test
  public void testListValidWithLimit() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("wildcard.*");
    String key = "my-fancy-entity-key";

    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                "wildcard.1",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (s, err) -> {});
    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                "wildcard.2",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3, 4}),
            (s, err) -> {});

    Rpc.ListRequest request =
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setWildcardPrefix("wildcard")
            .setKey(key)
            .setLimit(1)
            .build();

    final List<Rpc.ListResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.listAttributes(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(200, response.getStatus());
    assertEquals(1, response.getValueCount());
    assertEquals("wildcard.1", response.getValue(0).getAttribute());
    assertArrayEquals(new byte[] {1, 2, 3}, response.getValue(0).getValue().toByteArray());
  }

  @Test
  public void testListValidWithLimitWithTransaction() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("wildcard.*");
    String key = "my-fancy-entity-key";

    Rpc.ListRequest request =
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setWildcardPrefix("wildcard")
            .setKey(key)
            .setTransactionId(UUID.randomUUID().toString())
            .setLimit(1)
            .build();

    final List<Rpc.ListResponse> responses = new ArrayList<>();
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {}
        };

    retrieve.listAttributes(request, responseObserver);

    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(400, response.getStatus());
  }

  @Test
  public void testGetValidExtendedScheme() throws InvalidProtocolBufferException {
    EntityDescriptor entity = server.repo.getEntity("test");
    AttributeDescriptor<?> attribute = entity.getAttribute("data");
    String key = "my-fancy-entity-key";
    ExtendedMessage payload = ExtendedMessage.newBuilder().setFirst(1).setSecond(2).build();

    Optionals.get(server.direct.getWriter(attribute))
        .write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                attribute.getName(),
                System.currentTimeMillis(),
                payload.toByteArray()),
            (s, err) -> {});
    Rpc.GetRequest request =
        Rpc.GetRequest.newBuilder()
            .setEntity(entity.getName())
            .setAttribute(attribute.getName())
            .setKey(key)
            .build();

    final List<Rpc.GetResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.GetResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.GetResponse>() {
          @Override
          public void onNext(Rpc.GetResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.get(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.GetResponse response = responses.get(0);
    assertEquals(
        "Error: " + response.getStatus() + ": " + response.getStatusMessage(),
        200,
        response.getStatus());
    assertEquals(payload, ExtendedMessage.parseFrom(response.getValue().toByteArray()));
  }

  @Test
  public void testListNotFound() {
    EntityDescriptor entity = server.repo.getEntity("dummy");
    AttributeDescriptor<?> attribute = entity.getAttribute("wildcard.*");
    String key = "my-fancy-entity-key";

    Rpc.ListRequest request =
        Rpc.ListRequest.newBuilder()
            .setEntity(entity.getName())
            .setWildcardPrefix("wildcard")
            .setKey(key)
            .setLimit(1)
            .build();

    final List<Rpc.ListResponse> responses = new ArrayList<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final StreamObserver<Rpc.ListResponse> responseObserver;
    responseObserver =
        new StreamObserver<Rpc.ListResponse>() {
          @Override
          public void onNext(Rpc.ListResponse res) {
            responses.add(res);
          }

          @Override
          public void onError(Throwable thrwbl) {
            throw new RuntimeException(thrwbl);
          }

          @Override
          public void onCompleted() {
            finished.set(true);
          }
        };

    retrieve.listAttributes(request, responseObserver);

    assertTrue(finished.get());
    assertEquals(1, responses.size());
    Rpc.ListResponse response = responses.get(0);
    assertEquals(200, response.getStatus());
    assertTrue(response.getValueList().isEmpty());
  }
}
