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
package cz.o2.proxima.scheme.proto;

import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.AttributeValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.scheme.proto.ProtoSerializerFactory.TransactionProtoSerializer;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Commit;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.Pair;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link ProtoSerializerFactory}. */
public class ProtoSerializerFactoryTest {

  private final ValueSerializerFactory factory = new ProtoSerializerFactory();
  private ValueSerializer<Event> serializer;

  @Before
  public void setup() throws URISyntaxException {
    serializer = factory.getValueSerializer(new URI("proto:" + Event.class.getName()));
  }

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    Event event = Event.newBuilder().setGatewayId("gateway").build();
    byte[] bytes = serializer.serialize(event);
    Optional<Event> deserialized = serializer.deserialize(bytes);
    assertTrue(deserialized.isPresent());
    assertEquals(event, deserialized.get());
    assertEquals(
        event.getClass().getName(),
        factory.getClassName(new URI("proto:" + Event.class.getName())));
  }

  @Test
  public void testToLogString() {
    Event event = Event.newBuilder().setGatewayId("gateway").build();
    // we have single line string
    assertEquals(-1, serializer.getLogString(event).indexOf('\n'));
  }

  @Test
  public void testIsUsable() {
    assertTrue(serializer.isUsable());
  }

  @Test
  public void testJsonValue() {
    Event message =
        Event.newBuilder()
            .setGatewayId("gateway")
            .setPayload(ByteString.copyFrom(new byte[] {0}))
            .build();
    assertEquals(
        "{\n  \"gatewayId\": \"gateway\",\n  \"payload\": \"AA==\"\n}",
        serializer.asJsonValue(message));
    assertEquals(
        "gateway", serializer.fromJsonValue(serializer.asJsonValue(message)).getGatewayId());
  }

  @Test
  public void testGetSchemaDescriptor() {
    SchemaTypeDescriptor<Event> descriptor = serializer.getValueSchemaDescriptor();
    assertEquals(AttributeValueType.STRUCTURE, descriptor.getType());
  }

  @Test
  public void testGetValueAccessor() {
    AttributeValueAccessor<Event, StructureValue> accessor = serializer.getValueAccessor();
    Event created =
        accessor.createFrom(
            StructureValue.of(
                new HashMap<String, Object>() {
                  {
                    put("gatewayId", "gatewayId value");
                    put("payload", "payload value".getBytes(StandardCharsets.UTF_8));
                  }
                }));
    assertEquals("gatewayId value", created.getGatewayId());
    assertEquals("payload value", created.getPayload().toStringUtf8());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTransactionSchemeProvider() {
    try {
      Repository.ofTest(
          ConfigFactory.load("test-transactions-proto.conf")
              .withFallback(ConfigFactory.load("test-transactions.conf"))
              .resolve());
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }

    Repository repo =
        Repository.ofTest(
            ConfigFactory.load("test-transactions-proto.conf")
                .withFallback(ConfigFactory.load("test-transactions.conf"))
                .resolve());
    EntityDescriptor transaction = repo.getEntity("_transaction");
    Wildcard<Request> request = Wildcard.of(transaction, transaction.getAttribute("request.*"));

    KeyAttribute keyAttribute =
        KeyAttributes.ofAttributeDescriptor(transaction, "t", request, 1L, "1");

    List<KeyAttribute> wildcardQuery =
        KeyAttributes.ofWildcardQueryElements(
            transaction,
            "t",
            request,
            Arrays.asList(
                request.upsert(
                    1L, "t", "1", System.currentTimeMillis(), newRequest(Request.Flags.OPEN)),
                request.upsert(
                    2L, "t", "1", System.currentTimeMillis(), newRequest(Request.Flags.OPEN))));

    assertTrue(request.getValueSerializer() instanceof TransactionProtoSerializer);
    assertTrue(request.getValueSerializer().isUsable());

    AttributeDescriptor<Response> response = transaction.getAttribute("response.*");
    assertTrue(response.getValueSerializer() instanceof TransactionProtoSerializer);
    assertTrue(request.getValueSerializer().isUsable());

    AttributeDescriptor<State> state = transaction.getAttribute("state");
    assertTrue(state.getValueSerializer() instanceof TransactionProtoSerializer);
    assertTrue(state.getValueSerializer().isUsable());

    AttributeDescriptor<State> commit = transaction.getAttribute("commit");
    assertTrue(state.getValueSerializer() instanceof TransactionProtoSerializer);
    assertTrue(state.getValueSerializer().isUsable());

    StreamElement update =
        StreamElement.upsert(
            transaction,
            request,
            1L,
            "t",
            request.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {});

    StreamElement delete =
        StreamElement.delete(
            transaction,
            request,
            1L,
            "t",
            request.toAttributePrefix() + "2",
            System.currentTimeMillis());

    KeyAttribute keyAttributeSingleWildcard = KeyAttributes.ofStreamElement(update);
    KeyAttribute keyAttributeDelete = KeyAttributes.ofStreamElement(delete);
    KeyAttribute missingGet = KeyAttributes.ofMissingAttribute(transaction, "t", request, "1");
    long now = System.currentTimeMillis();

    Request someRequest = newRequest(keyAttribute, Request.Flags.OPEN);
    List<Pair<Object, AttributeDescriptor<?>>> toVerify =
        Arrays.asList(
            Pair.of(newRequest(keyAttribute, Request.Flags.OPEN), request),
            Pair.of(newRequest(keyAttributeSingleWildcard, Request.Flags.OPEN), request),
            Pair.of(newRequest(keyAttribute, Request.Flags.COMMIT), request),
            Pair.of(newRequest(keyAttributeSingleWildcard, Request.Flags.COMMIT), request),
            Pair.of(newRequest(keyAttribute, Request.Flags.UPDATE), request),
            Pair.of(newRequest(keyAttributeSingleWildcard, Request.Flags.UPDATE), request),
            Pair.of(newRequest(wildcardQuery, Request.Flags.OPEN), request),
            Pair.of(newRequest(Request.Flags.ROLLBACK), request),
            Pair.of(Response.forRequest(someRequest).open(1L, now), response),
            Pair.of(Response.forRequest(someRequest).updated(), response),
            Pair.of(Response.forRequest(someRequest).committed(), response),
            Pair.of(Response.forRequest(someRequest).aborted(), response),
            Pair.of(Response.forRequest(someRequest).duplicate(), response),
            Pair.of(Response.empty(), response),
            Pair.of(
                Commit.of(1L, System.currentTimeMillis(), Arrays.asList(update, delete)), commit),
            Pair.of(State.open(1L, now, Sets.newHashSet(keyAttribute)), state),
            Pair.of(
                State.open(1L, now, Sets.newHashSet(keyAttribute, keyAttributeSingleWildcard))
                    .committed(Sets.newHashSet(keyAttribute)),
                state),
            Pair.of(State.empty(), state),
            Pair.of(
                State.open(1L, now, Sets.newHashSet(keyAttribute))
                    .update(Collections.singletonList(keyAttributeSingleWildcard)),
                state),
            Pair.of(State.open(1L, now, Sets.newHashSet(keyAttribute)).aborted(), state),
            Pair.of(State.open(1L, now, Sets.newHashSet(missingGet)).aborted(), state),
            Pair.of(
                State.open(1L, now, Sets.newHashSet(keyAttributeSingleWildcard))
                    .committed(Sets.newHashSet(keyAttributeSingleWildcard)),
                state),
            Pair.of(
                State.open(1L, now, Collections.emptyList())
                    .committed(Sets.newHashSet(keyAttributeDelete)),
                state));

    toVerify.forEach(
        p -> {
          ValueSerializer<Object> serializer =
              (ValueSerializer<Object>) p.getSecond().getValueSerializer();
          byte[] bytes = serializer.serialize(p.getFirst());
          assertNotNull(bytes);
          // we do not serialize the target partition for responses
          if (p.getFirst() instanceof Response) {
            compareResponses(
                (Response) p.getFirst(), (Response) Optionals.get(serializer.deserialize(bytes)));
          } else {
            assertEquals(p.getFirst(), Optionals.get(serializer.deserialize(bytes)));
          }
        });
  }

  private void compareResponses(Response first, Response other) {
    assertEquals(
        new Response(
            first.getFlags(),
            first.hasSequenceId() ? first.getSeqId() : 0L,
            first.hasStamp() ? first.getStamp() : 0L,
            -1),
        new Response(
            other.getFlags(),
            other.hasSequenceId() ? other.getSeqId() : 0L,
            other.hasStamp() ? other.getStamp() : 0L,
            -1));
  }

  private Request newRequest(Request.Flags flags) {
    return Request.builder().flags(flags).build();
  }

  private Request newRequest(KeyAttribute keyAttribute, Request.Flags flags) {
    return newRequest(Collections.singletonList(keyAttribute), flags);
  }

  private Request newRequest(List<KeyAttribute> keyAttributes, Request.Flags flags) {
    return Request.builder()
        .inputAttributes(keyAttributes)
        .outputAttributes(keyAttributes)
        .responsePartitionId(1)
        .flags(flags)
        .build();
  }
}
