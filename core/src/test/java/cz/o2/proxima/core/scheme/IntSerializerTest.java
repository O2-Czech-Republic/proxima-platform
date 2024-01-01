/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.scheme;

import static org.junit.Assert.*;

import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import org.junit.Test;

/** Test suite for {@link IntSerializer}. */
public class IntSerializerTest {

  private final IntSerializer serializer = new IntSerializer();

  @Test
  public void testScheme() {
    assertEquals("integer", serializer.getAcceptableScheme());
  }

  @Test
  public void testClassName() {
    assertEquals("Integer", serializer.getClassName(null));
  }

  @Test
  public void testSerializeDeserialize() {
    ValueSerializer<Integer> s = serializer.getValueSerializer(null);
    assertEquals(1, (int) s.deserialize(s.serialize(1)).get());
  }

  @Test
  public void testJsonSerializeDeserialize() {
    ValueSerializer<Integer> s = serializer.getValueSerializer(null);
    assertEquals(1, (int) s.fromJsonValue(s.asJsonValue(1)));
  }

  @Test
  public void testInvalid() {
    ValueSerializer<Integer> s = serializer.getValueSerializer(null);
    assertFalse(s.isValid(new byte[] {0}));
  }

  @Test
  public void testUsable() {
    ValueSerializer<Integer> s = serializer.getValueSerializer(null);
    assertTrue(s.isUsable());
  }

  @Test
  public void testDefaultValue() {
    ValueSerializer<Integer> s = serializer.getValueSerializer(null);
    assertEquals(0, (int) s.getDefault());
  }

  @Test
  public void testValueDescriptor() {
    SchemaTypeDescriptor<Integer> descriptor =
        serializer.<Integer>getValueSerializer(null).getValueSchemaDescriptor();
    assertTrue(descriptor.isPrimitiveType());
  }
}
