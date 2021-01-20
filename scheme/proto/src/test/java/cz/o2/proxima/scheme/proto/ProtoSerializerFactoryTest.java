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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import java.net.URI;
import java.net.URISyntaxException;
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
}
