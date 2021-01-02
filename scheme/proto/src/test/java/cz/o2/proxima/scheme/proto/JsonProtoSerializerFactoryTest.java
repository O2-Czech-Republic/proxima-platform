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
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Optional;
import org.junit.Test;

/** Test {@link JsonProtoSerializerFactory}. */
@SuppressWarnings("unchecked")
public class JsonProtoSerializerFactoryTest {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  ValueSerializerFactory factory = new JsonProtoSerializerFactory();

  @Test
  public void testSerialization() throws URISyntaxException {
    Event event =
        Event.newBuilder()
            .setGatewayId("gateway")
            .setPayload(ByteString.copyFrom(new byte[] {1, 2, 3}))
            .build();
    ValueSerializer serializer =
        factory.getValueSerializer(new URI("json-proto:" + Event.class.getName()));
    String json = new String(serializer.serialize(event), UTF8);
    assertEquals("{\n  \"gatewayId\": \"gateway\",\n  \"payload\": \"AQID\"\n}", json);
  }

  @Test
  public void testDeserialization() throws URISyntaxException {
    String json = "{\n  \"gatewayId\": \"gateway\",\n  \"payload\": \"AQID\"\n}";
    Event event =
        Event.newBuilder()
            .setGatewayId("gateway")
            .setPayload(ByteString.copyFrom(new byte[] {1, 2, 3}))
            .build();
    ValueSerializer serializer =
        factory.getValueSerializer(new URI("json-proto:" + Event.class.getName()));
    Optional<Event> deserialized = serializer.deserialize(json.getBytes(UTF8));
    assertTrue(deserialized.isPresent());
    assertEquals(event, deserialized.get());
  }

  @Test
  public void testDeserializationWithMissingField() throws URISyntaxException {
    String json = "{\"gatewayId\": \"gateway\"}";
    Event event = Event.newBuilder().setGatewayId("gateway").build();
    ValueSerializer serializer =
        factory.getValueSerializer(new URI("json-proto:" + Event.class.getName()));
    Optional<Event> deserialized = serializer.deserialize(json.getBytes(UTF8));
    assertTrue(deserialized.isPresent());
    assertEquals(event, deserialized.get());
  }

  @Test
  public void testDeserializationWithUnknownField() throws URISyntaxException {
    String json = "{\"gatewayId\": \"gateway\", \"unknown\": 1}";
    Event event = Event.newBuilder().setGatewayId("gateway").build();
    ValueSerializer serializer =
        factory.getValueSerializer(new URI("json-proto:" + Event.class.getName()));
    Optional<Event> deserialized = serializer.deserialize(json.getBytes(UTF8));
    assertTrue(deserialized.isPresent());
    assertEquals(event, deserialized.get());
  }

  @Test
  public void testIsUsable() throws URISyntaxException {
    ValueSerializer serializer =
        factory.getValueSerializer(new URI("json-proto:" + Event.class.getName()));
    assertTrue(serializer.isUsable());
  }
}
