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
import java.util.Map;
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
  public void assertSerializableEnumType() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(SchemaDescriptors.enums(Arrays.asList("FIRST", "SECOND")));
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
    assertEquals("STRING", string.toString());
    assertTrue(string.isPrimitiveType());
    assertEquals(string, string.asPrimitiveTypeDescriptor());
    assertThrows(UnsupportedOperationException.class, string::asArrayTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, string::asStructureTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, string::asEnumTypeDescriptor);
  }

  @Test
  public void testArrayDescriptor() {
    ArrayTypeDescriptor<String> desc = SchemaDescriptors.arrays(SchemaDescriptors.strings());
    assertEquals(AttributeValueType.ARRAY, desc.getType());
    assertEquals(AttributeValueType.STRING, desc.getValueType());
    assertEquals("ARRAY[STRING]", desc.toString());
    assertThrows(UnsupportedOperationException.class, desc::asStructureTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, desc::asEnumTypeDescriptor);
  }

  @Test
  public void testStructureDescriptorWithoutFields() {
    StructureTypeDescriptor<Map<String, Object>> desc = SchemaDescriptors.structures("structure");
    assertEquals("structure", desc.getName());
    Map<String, SchemaTypeDescriptor<?>> fields = desc.getFields();
    assertTrue(fields.isEmpty());
    assertEquals("STRUCTURE structure", desc.toString());
    assertThrows(IllegalArgumentException.class, () -> desc.getField("something"));
    assertFalse(desc.hasField("something"));
    PrimitiveTypeDescriptor<String> stringTypeDescriptor = SchemaDescriptors.strings();
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          // Fields should be immutable
          fields.put("field", stringTypeDescriptor);
        });
    assertFalse(desc.isArrayType());
    assertFalse(desc.isPrimitiveType());
    assertTrue(desc.isStructureType());
    assertThrows(UnsupportedOperationException.class, desc::asArrayTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, desc::asPrimitiveTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, desc::asEnumTypeDescriptor);
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
        s.getField("array_of_string_field").asArrayTypeDescriptor().getValueType());
    assertEquals(AttributeValueType.STRUCTURE, s.getField("inner_structure").getType());
    assertEquals(
        AttributeValueType.STRUCTURE,
        s.getField("inner_structure").asStructureTypeDescriptor().getType());

    assertEquals(
        "inner_message", s.getField("inner_structure").asStructureTypeDescriptor().getName());

    assertEquals(
        AttributeValueType.ARRAY,
        s.getField("inner_structure").asStructureTypeDescriptor().getField("byte_array").getType());
    assertEquals(
        AttributeValueType.BYTE,
        s.getField("inner_structure")
            .asStructureTypeDescriptor()
            .getField("byte_array")
            .asArrayTypeDescriptor()
            .getValueType());
  }

  @Test
  public void testArrayTypeWithPrimitiveValue() {
    final ArrayTypeDescriptor<Long> desc = SchemaDescriptors.arrays(SchemaDescriptors.longs());
    assertEquals(AttributeValueType.ARRAY, desc.getType());
    assertEquals(AttributeValueType.LONG, desc.getValueType());
  }

  @Test
  public void testBytes() {
    final ArrayTypeDescriptor<byte[]> bytes = SchemaDescriptors.bytes();
    assertEquals(AttributeValueType.ARRAY, bytes.getType());
    assertEquals(AttributeValueType.BYTE, bytes.getValueType());
    assertTrue(bytes.isPrimitiveType());
    assertEquals(AttributeValueType.BYTE, bytes.asPrimitiveTypeDescriptor().getType());
  }

  @Test
  public void testArrayTypeWithStructValue() {
    ArrayTypeDescriptor<Object> desc =
        SchemaDescriptors.arrays(
            SchemaDescriptors.structures("structure")
                .addField("field1", SchemaDescriptors.strings())
                .addField("field2", SchemaDescriptors.longs()));
    assertEquals(AttributeValueType.ARRAY, desc.getType());
    assertEquals(AttributeValueType.STRUCTURE, desc.getValueType());
    assertEquals(AttributeValueType.STRUCTURE, desc.getValueDescriptor().getType());
    assertFalse(desc.isPrimitiveType());
    final StructureTypeDescriptor<Object> structureTypeDescriptor =
        desc.getValueDescriptor().asStructureTypeDescriptor();
    assertEquals("structure", structureTypeDescriptor.getName());
    final Map<String, SchemaTypeDescriptor<?>> fields = structureTypeDescriptor.getFields();
    assertEquals(2, fields.size());
    final PrimitiveTypeDescriptor<String> string = SchemaDescriptors.strings();
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          // fields should be always immutable
          fields.put("foo", string);
        });
  }

  @Test
  public void testArrayOfEnum() {
    final List<String> enumValues = Arrays.asList("FIRST", "SECOND");
    final ArrayTypeDescriptor<String> desc =
        SchemaDescriptors.arrays(SchemaDescriptors.enums(enumValues));
    assertEquals(AttributeValueType.ENUM, desc.getValueType());
    assertEquals(enumValues, desc.getValueDescriptor().asEnumTypeDescriptor().getValues());
  }

  @Test
  public void testLongType() {
    PrimitiveTypeDescriptor<Long> desc = SchemaDescriptors.longs();
    assertEquals(AttributeValueType.LONG, desc.getType());
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
  public void testArrayOfBytes() {
    final ArrayTypeDescriptor<byte[]> descriptor =
        SchemaDescriptors.arrays(SchemaDescriptors.bytes());
    assertEquals(AttributeValueType.ARRAY, descriptor.getType());
    assertEquals(AttributeValueType.ARRAY, descriptor.getValueType());
  }

  @Test
  public void testEnumType() {
    final List<String> values = Arrays.asList("LEFT", "RIGHT");
    final EnumTypeDescriptor<String> desc = SchemaDescriptors.enums(values);
    assertTrue(desc.isEnumType());
    assertEquals(desc, desc.asEnumTypeDescriptor());
    assertEquals(AttributeValueType.ENUM, desc.getType());
    assertEquals(values, desc.getValues());
    assertEquals("ENUM[LEFT, RIGHT]", desc.toString());

    assertEquals(desc, SchemaDescriptors.enums(values));
    assertNotEquals(desc, SchemaDescriptors.enums(Collections.emptyList()));
  }
}
