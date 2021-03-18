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
package cz.o2.proxima.scheme;

import static org.junit.Assert.*;

import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

/** Simple test for {@link BytesSerializer} */
public class BytesSerializerTest {
  private final BytesSerializer factory = new BytesSerializer();
  private ValueSerializer<byte[]> serializer;

  @Before
  public void setup() throws URISyntaxException {
    serializer = factory.getValueSerializer(new URI("bytes:///"));
  }

  @Test
  public void testSerialize() {
    byte[] value = serializer.serialize("test-value".getBytes());
    assertEquals("test-value", new String(value));
  }

  @Test
  public void testDeserialize() {
    Optional<byte[]> deserialized = serializer.deserialize("test-value".getBytes());
    assertTrue(deserialized.isPresent());
    assertEquals("test-value", new String(deserialized.get()));
  }

  @Test
  public void testGetClass() throws URISyntaxException {
    assertEquals("byte[]", factory.getClassName(new URI("bytes:///")));
  }

  @Test
  public void testIsUsable() {
    assertTrue(serializer.isUsable());
  }

  @Test
  public void testDefaultValue() {
    assertArrayEquals(new byte[] {}, serializer.getDefault());
  }

  @Test
  public void testJsonValue() {
    byte[] value = new byte[] {1, 2};
    assertArrayEquals(value, serializer.fromJsonValue(serializer.asJsonValue(value)));
  }

  @Test
  public void testGetValueSchemaDescriptor() {
    SchemaTypeDescriptor<byte[]> descriptor = serializer.getValueSchemaDescriptor();
    assertEquals(AttributeValueType.BYTE, descriptor.asArrayTypeDescriptor().getValueType());
  }
}
