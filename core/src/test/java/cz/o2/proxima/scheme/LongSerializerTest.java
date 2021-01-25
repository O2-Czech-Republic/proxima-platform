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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import org.junit.Test;

/** Test suite for {@link LongSerializer}. */
public class LongSerializerTest {

  private final LongSerializer serializer = new LongSerializer();

  @Test
  public void testScheme() {
    assertEquals("long", serializer.getAcceptableScheme());
  }

  @Test
  public void testClassName() {
    assertEquals("Long", serializer.getClassName(null));
  }

  @Test
  public void testSerializeDeserialize() {
    ValueSerializer<Long> s = serializer.getValueSerializer(null);
    assertEquals(1L, (long) s.deserialize(s.serialize(1L)).get());
  }

  @Test
  public void testJsonSerializeDeserialize() {
    ValueSerializer<Long> s = serializer.getValueSerializer(null);
    assertEquals(1L, (long) s.fromJsonValue(s.asJsonValue(1L)));
  }

  @Test
  public void testInvalid() {
    ValueSerializer<Long> s = serializer.getValueSerializer(null);
    assertFalse(s.isValid(new byte[] {0}));
  }

  @Test
  public void testUsable() {
    ValueSerializer<Long> s = serializer.getValueSerializer(null);
    assertTrue(s.isUsable());
  }

  @Test
  public void testDefaultValue() {
    ValueSerializer<Long> s = serializer.getValueSerializer(null);
    assertEquals(0L, (long) s.getDefault());
  }

  @Test
  public void testValueDescriptor() {
    SchemaTypeDescriptor<Long> descriptor =
        serializer.<Long>getValueSerializer(null).getValueSchemaDescriptor();
    assertTrue(descriptor.isPrimitiveType());
  }
}
