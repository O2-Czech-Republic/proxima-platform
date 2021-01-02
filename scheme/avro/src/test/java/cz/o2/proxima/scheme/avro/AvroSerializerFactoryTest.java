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
package cz.o2.proxima.scheme.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.scheme.avro.test.Event;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.junit.Test;

/** Test {@link AvroSerializerFactory}. */
public class AvroSerializerFactoryTest {

  private final ValueSerializerFactory factory = new AvroSerializerFactory();

  @Test
  public void testEqualsAfterSerializeAndDeserialize() throws URISyntaxException {
    Event event =
        Event.newBuilder()
            .setGatewayId("gateway")
            .setPayload(ByteBuffer.wrap("my-payload".getBytes()))
            .build();

    ValueSerializer<Event> serializer =
        factory.getValueSerializer(new URI("avro:" + event.getClass().getName()));

    byte[] bytes = serializer.serialize(event);

    Optional<Event> deserialized = serializer.deserialize(bytes);
    assertTrue(deserialized.isPresent());
    assertEquals(event, deserialized.get());
    assertEquals(
        Event.class.getName(), factory.getClassName(new URI("avro:" + Event.class.getName())));
  }

  @Test
  public void testGetDefault() throws URISyntaxException {
    ValueSerializer<Event> serializer =
        factory.getValueSerializer(new URI("avro:" + Event.class.getName()));
    Event object = new Event();
    assertEquals(object, serializer.getDefault());
  }

  @Test
  public void testIsUsable() throws URISyntaxException {
    ValueSerializer<Event> serializer =
        factory.getValueSerializer(new URI("avro:" + Event.class.getName()));
    assertTrue(serializer.isUsable());
  }
}
