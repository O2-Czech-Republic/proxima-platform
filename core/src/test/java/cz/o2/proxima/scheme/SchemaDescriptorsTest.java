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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.EnumTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.PrimitiveTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SchemaDescriptorsTest {

  @Test
  public void assertSerializablePrimitiveType() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(SchemaDescriptors.strings());
  }

  @Test
  public void assertSerializableArrayType() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(SchemaDescriptors.bytes());
  }

  @Test
  public void assertSerializableStructType() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(
        SchemaDescriptors.structures("struct")
            .addField("field1", SchemaDescriptors.longs())
            .addField("field2", SchemaDescriptors.bytes()));
  }

  @Test
  public void checkEquals() {
    assertNotEquals(SchemaDescriptors.strings(), SchemaDescriptors.longs());
    assertEquals(SchemaDescriptors.strings(), SchemaDescriptors.strings());
    assertEquals(
        SchemaDescriptors.arrays(SchemaDescriptors.strings()),
        SchemaDescriptors.arrays(SchemaDescriptors.strings()));
    assertNotEquals(
        SchemaDescriptors.arrays(SchemaDescriptors.strings()),
        SchemaDescriptors.arrays(SchemaDescriptors.integers()));
    assertEquals(SchemaDescriptors.structures("test"), SchemaDescriptors.structures("test"));
    assertNotEquals(SchemaDescriptors.structures("test"), SchemaDescriptors.structures("another"));
    assertNotEquals(
        SchemaDescriptors.structures("test").addField("field", SchemaDescriptors.integers()),
        SchemaDescriptors.structures("test").addField("field", SchemaDescriptors.longs()));
    assertEquals(
        SchemaDescriptors.structures(
            "test", Collections.singletonMap("field", SchemaDescriptors.integers())),
        SchemaDescriptors.structures(
            "test", Collections.singletonMap("field", SchemaDescriptors.integers())));
  }

  @Test
  public void testStringDescriptor() {
    PrimitiveTypeDescriptor<String> string = SchemaDescriptors.strings();
    assertEquals(AttributeValueType.STRING, string.getType());
    SchemaTypeDescriptor<String> descriptor = string.toTypeDescriptor();
    assertEquals(string, descriptor.getPrimitiveTypeDescriptor());
    assertThrows(IllegalStateException.class, descriptor::getArrayTypeDescriptor);
    assertThrows(IllegalStateException.class, descriptor::getStructureTypeDescriptor);
    assertThrows(IllegalStateException.class, descriptor::getEnumTypeDescriptor);
  }

  @Test
  public void testArrayDescriptor() {
    ArrayTypeDescriptor<String> desc = SchemaDescriptors.arrays(SchemaDescriptors.strings());
    assertEquals(AttributeValueType.ARRAY, desc.getType());
    assertEquals(AttributeValueType.STRING, desc.getValueType());
    SchemaTypeDescriptor<String> d = desc.toTypeDescriptor();
    assertThrows(IllegalStateException.class, d::getPrimitiveTypeDescriptor);
    assertThrows(IllegalStateException.class, d::getStructureTypeDescriptor);
    assertThrows(IllegalStateException.class, d::getEnumTypeDescriptor);
  }

  @Test
  public void testStructureDescriptorWithoutFields() {
    StructureTypeDescriptor<Object> s = SchemaDescriptors.structures("structure");
    assertEquals("structure", s.getName());
    assertTrue(s.getFields().isEmpty());
    SchemaTypeDescriptor<Object> desc = s.toTypeDescriptor();
    assertEquals(s, desc.getStructureTypeDescriptor());
    assertThrows(IllegalArgumentException.class, () -> s.getField("something"));
    assertFalse(s.hasField("something"));
    assertFalse(desc.isArrayType());
    assertFalse(desc.isPrimitiveType());
    assertTrue(desc.isStructureType());
    assertThrows(IllegalStateException.class, desc::getArrayTypeDescriptor);
    assertThrows(IllegalStateException.class, desc::getPrimitiveTypeDescriptor);
    assertThrows(IllegalStateException.class, desc::getEnumTypeDescriptor);
  }

  @Test
  public void testStructureDescriptorWithFields() {
    StructureTypeDescriptor<Object> s =
        SchemaDescriptors.structures("structure")
            .addField("string_field", SchemaDescriptors.strings())
            .addField(
                "array_of_string_field", SchemaDescriptors.arrays(SchemaDescriptors.strings()))
            .addField(
                "inner_structure",
                SchemaDescriptors.structures("inner_message")
                    .addField("byte_array", SchemaDescriptors.bytes())
                    .addField("int", SchemaDescriptors.integers())
                    .addField("long", SchemaDescriptors.longs()));
    assertEquals(AttributeValueType.STRUCTURE, s.getType());
    assertFalse(s.getFields().isEmpty());
    assertThrows(IllegalArgumentException.class, () -> s.getField("not-exists-field"));
    PrimitiveTypeDescriptor<String> stringDescriptor = SchemaDescriptors.strings();
    assertThrows(
        IllegalArgumentException.class, () -> s.addField("string_field", stringDescriptor));
    assertEquals(AttributeValueType.STRING, s.getField("string_field").getType());
    assertEquals(AttributeValueType.ARRAY, s.getField("array_of_string_field").getType());
    assertEquals(
        AttributeValueType.STRING,
        s.getField("array_of_string_field").getArrayTypeDescriptor().getValueType());
    assertEquals(AttributeValueType.STRUCTURE, s.getField("inner_structure").getType());
    assertEquals(
        AttributeValueType.STRUCTURE,
        s.getField("inner_structure").toTypeDescriptor().getStructureTypeDescriptor().getType());

    assertEquals(
        "inner_message",
        s.getField("inner_structure").toTypeDescriptor().getStructureTypeDescriptor().getName());

    assertEquals(
        AttributeValueType.ARRAY,
        s.getField("inner_structure")
            .getStructureTypeDescriptor()
            .getField("byte_array")
            .getType());
    assertEquals(
        AttributeValueType.BYTE,
        s.getField("inner_structure")
            .getStructureTypeDescriptor()
            .getField("byte_array")
            .getArrayTypeDescriptor()
            .getValueType());
  }

  @Test
  public void testArrayTypeWithPrimitiveValue() {
    ArrayTypeDescriptor<Long> d = SchemaDescriptors.arrays(SchemaDescriptors.longs());
    assertEquals(AttributeValueType.ARRAY, d.getType());
    assertEquals(AttributeValueType.LONG, d.getValueType());
  }

  @Test
  public void testArrayTypeWithStructValue() {
    ArrayTypeDescriptor<Object> d =
        SchemaDescriptors.arrays(
            SchemaDescriptors.structures("structure")
                .addField("field1", SchemaDescriptors.strings())
                .addField("field2", SchemaDescriptors.longs()));
    assertEquals(AttributeValueType.ARRAY, d.getType());
    assertEquals(AttributeValueType.STRUCTURE, d.getValueType());
    assertEquals(AttributeValueType.ARRAY, d.toTypeDescriptor().getType());
    assertEquals(AttributeValueType.STRUCTURE, d.getValueDescriptor().getType());
    assertEquals("structure", d.getValueDescriptor().getStructureTypeDescriptor().getName());
    assertEquals(2, d.getValueDescriptor().getStructureTypeDescriptor().getFields().size());
  }

  @Test
  public void testDoubleType() {
    PrimitiveTypeDescriptor<Double> descriptor = SchemaDescriptors.doubles();
    assertEquals(AttributeValueType.DOUBLE, descriptor.getType());
  }

  @Test
  public void testFloatType() {
    PrimitiveTypeDescriptor<Float> descriptor = SchemaDescriptors.floats();
    assertEquals(AttributeValueType.FLOAT, descriptor.getType());
  }

  @Test
  public void testBooleanType() {
    PrimitiveTypeDescriptor<Boolean> descriptor = SchemaDescriptors.booleans();
    assertEquals(AttributeValueType.BOOLEAN, descriptor.getType());
  }

  @Test
  public void testEnumType() {
    List<String> values = Arrays.asList("LEFT", "RIGHT");
    EnumTypeDescriptor<String> desc = SchemaDescriptors.enums(values);
    assertEquals(AttributeValueType.ENUM, desc.getType());
    assertEquals(values, desc.getValues());

    assertEquals(desc, desc.toTypeDescriptor().getEnumTypeDescriptor());

    assertEquals(desc, SchemaDescriptors.enums(new String[] {"LEFT", "RIGHT"}));
    assertEquals(desc, SchemaDescriptors.enums(values));
  }
}
