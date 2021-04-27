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
import static org.junit.Assert.assertEquals;

import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.util.Optionals;
import org.junit.Test;

public class DoubleSerializerTest {

  private final DoubleSerializer serializer = new DoubleSerializer();

  @Test
  public void testScheme() {
    assertEquals("double", serializer.getAcceptableScheme());
  }

  @Test
  public void testClassName() {
    assertEquals("Double", serializer.getClassName(null));
  }

  @Test
  public void testSerializeDeserialize() {
    ValueSerializer<Double> s = serializer.getValueSerializer(null);
    assertEquals(1.1, Optionals.get(s.deserialize(s.serialize(1.1))), 0.0001);
  }

  @Test
  public void testJsonSerializeDeserialize() {
    ValueSerializer<Double> s = serializer.getValueSerializer(null);
    assertEquals(1.1, s.fromJsonValue(s.asJsonValue(1.1)), 0.0001);
  }

  @Test
  public void testInvalid() {
    ValueSerializer<Double> s = serializer.getValueSerializer(null);
    assertFalse(s.isValid(new byte[] {0}));
  }

  @Test
  public void testUsable() {
    ValueSerializer<Double> s = serializer.getValueSerializer(null);
    assertTrue(s.isUsable());
  }

  @Test
  public void testDefaultValue() {
    ValueSerializer<Double> s = serializer.getValueSerializer(null);
    assertEquals(0.0f, s.getDefault(), 0.0001);
  }

  @Test
  public void testValueDescriptor() {
    SchemaTypeDescriptor<Double> descriptor =
        serializer.<Double>getValueSerializer(null).getValueSchemaDescriptor();
    assertTrue(descriptor.isPrimitiveType());
    assertEquals(AttributeValueType.DOUBLE, descriptor.getType());
  }
}
