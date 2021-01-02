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
package cz.o2.proxima.scheme.confluent;

import static org.junit.Assert.*;

import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.scheme.avro.AvroSerializer;
import cz.o2.proxima.scheme.avro.test.Event;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

/** Test {@link SchemaRegistrySerializerFactory}. */
public class SchemaRegistrySerializerFactoryTest {
  private SchemaRegistryClient schemaRegistry;
  private SchemaRegistryValueSerializer<Event> serializer;
  private ValueSerializerFactory factory = new SchemaRegistrySerializerFactory();

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    schemaRegistry = new MockSchemaRegistryClient();
    schemaRegistry.register("event", Event.getClassSchema());

    serializer =
        (SchemaRegistryValueSerializer)
            factory.getValueSerializer(
                new URI("schema-registry:http://schema-registry:8081/event"));
    serializer.setSchemaRegistry(schemaRegistry);
  }

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    Event event =
        Event.newBuilder()
            .setGatewayId("gateway")
            .setPayload(ByteBuffer.wrap("my-payload".getBytes()))
            .build();

    byte[] bytes = serializer.serialize(event);
    Optional<Event> deserialized = serializer.deserialize(bytes);
    assertTrue(deserialized.isPresent());
    assertEquals(event, deserialized.get());
    assertEquals(
        event.getClass().getName(),
        factory.getClassName(new URI("schema-registry:http://schema-registry:8081/event")));
  }

  @Test
  public void testGetClassname() throws Exception {
    assertEquals(
        Event.class.getName(),
        factory.getClassName(new URI("schema-registry:http://schema-registry:8081/event")));
  }

  @Test
  public void testByteSerialization() throws Exception {
    Event event =
        Event.newBuilder()
            .setGatewayId("gateway")
            .setPayload(ByteBuffer.wrap("my-payload".getBytes()))
            .build();

    ByteBuffer buffer = ByteBuffer.wrap(serializer.serialize(event));
    assertEquals(SchemaRegistryValueSerializer.MAGIC_BYTE, buffer.get());
    assertEquals(schemaRegistry.getLatestSchemaMetadata("event").getId(), buffer.getInt());

    AvroSerializer<Event> avroSerializer = new AvroSerializer<>(event.getSchema());

    int len = buffer.limit() - 1 - SchemaRegistryValueSerializer.SCHEMA_ID_SIZE;
    int start = buffer.position() + buffer.arrayOffset();

    Event fromBytes = avroSerializer.deserialize(buffer, start, len);
    assertEquals(event, fromBytes);
  }

  @Test
  public void testWithInvalidUri() throws Exception {
    ValueSerializer s = factory.getValueSerializer(new URI("schema-registry:not-valid"));
    assertFalse(s.isUsable());
  }

  @Test
  public void testDeserializeWithInvalidMagicByte() {
    assertFalse(serializer.deserialize(new byte[] {0x1}).isPresent());
  }

  @Test
  public void testDeserializeNotRegisteredSubject() throws Exception {

    SchemaRegistryValueSerializer s =
        (SchemaRegistryValueSerializer)
            factory.getValueSerializer(
                new URI("schema-registry:http://schema-registry:8081/not-registered"));
    s.setSchemaRegistry(schemaRegistry);
    assertFalse(s.isUsable());
  }

  @Test
  public void testBytesDeserialization() throws Exception {
    int schemaId = schemaRegistry.getLatestSchemaMetadata("event").getId();
    Event event =
        Event.newBuilder()
            .setGatewayId("gateway")
            .setPayload(ByteBuffer.wrap("my-payload".getBytes()))
            .build();

    AvroSerializer<Event> avroSerializer = new AvroSerializer<>(event.getSchema());
    byte[] eventBytes = avroSerializer.serialize(event);
    ByteBuffer buffer =
        ByteBuffer.allocate(1 + SchemaRegistryValueSerializer.SCHEMA_ID_SIZE + eventBytes.length);

    buffer.put(SchemaRegistryValueSerializer.MAGIC_BYTE);
    buffer.putInt(schemaId);
    buffer.put(eventBytes);

    Optional<Event> fromBytes = serializer.deserialize(buffer.array());

    assertTrue(fromBytes.isPresent());
    assertEquals(event, fromBytes.get());
  }

  @Test
  public void testIsUsable() {
    assertTrue(serializer.isUsable());
  }
}
