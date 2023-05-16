/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.scheme.proto.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import cz.o2.proxima.core.scheme.AttributeValueType;
import cz.o2.proxima.core.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme;
import cz.o2.proxima.scheme.proto.test.Scheme.Device;
import cz.o2.proxima.scheme.proto.test.Scheme.MessageWithWrappers;
import cz.o2.proxima.scheme.proto.test.Scheme.RecursiveMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class ProtoUtilsTest {

  @Test
  public void testConvertSimpleProtoToSchema() {
    StructureTypeDescriptor<Device> schema =
        ProtoUtils.convertProtoToSchema(Device.getDescriptor());
    assertEquals(AttributeValueType.STRUCTURE, schema.getType());
    assertEquals(2, schema.getFields().size());
    checkIfSchemaContainsAllFields(schema, Device.getDescriptor());
    assertEquals(AttributeValueType.STRING, schema.getField("type").getType());
    assertEquals(AttributeValueType.ARRAY, schema.getField("payload").getType());
    assertEquals(
        AttributeValueType.BYTE, schema.getField("payload").asArrayTypeDescriptor().getValueType());
  }

  @Test
  public void testConvertComplexProtoToSchema() {
    StructureTypeDescriptor<ValueSchemeMessage> descriptor =
        ProtoUtils.convertProtoToSchema(ValueSchemeMessage.getDescriptor());
    log.debug("Schema: {}", descriptor);
    checkIfSchemaContainsAllFields(descriptor, ValueSchemeMessage.getDescriptor());
    assertEquals(AttributeValueType.STRUCTURE, descriptor.getType());
    assertEquals(ValueSchemeMessage.getDescriptor().getName(), descriptor.getName());
    assertTrue(descriptor.hasField("repeated_inner_message"));
    assertEquals(AttributeValueType.ARRAY, descriptor.getField("repeated_inner_message").getType());
    assertEquals(
        AttributeValueType.ENUM,
        descriptor
            .getField("inner_message")
            .asStructureTypeDescriptor()
            .getField("inner_enum")
            .getType());

    assertEquals(
        AttributeValueType.STRUCTURE,
        descriptor.getField("repeated_inner_message").asArrayTypeDescriptor().getValueType());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testConvertMessageWithRecursion() {
    StructureTypeDescriptor<RecursiveMessage> descriptor =
        ProtoUtils.convertProtoToSchema(RecursiveMessage.getDescriptor());
    log.debug("Schema: {}", descriptor);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testConvertMessageWithTwoStepRecursion() {
    StructureTypeDescriptor<RecursiveMessage> descriptor =
        ProtoUtils.convertProtoToSchema(Scheme.TwoStepRecursiveMessage.getDescriptor());
    log.debug("Schema: {}", descriptor);
  }

  @Test
  public void testConvertMessageWithMessageAsScalarConversion() {
    StructureTypeDescriptor<MessageWithWrappers> schema =
        ProtoUtils.convertProtoToSchema(MessageWithWrappers.getDescriptor());
    log.debug("Schema: {}", schema);
    assertEquals(AttributeValueType.STRUCTURE, schema.getType());
    checkIfSchemaContainsAllFields(schema, MessageWithWrappers.getDescriptor());
    assertEquals(AttributeValueType.STRING, schema.getField("string").getType());
    assertEquals(AttributeValueType.BOOLEAN, schema.getField("bool").getType());
    assertEquals(AttributeValueType.INT, schema.getField("int32").getType());
    assertEquals(AttributeValueType.INT, schema.getField("uint32").getType());
    assertEquals(AttributeValueType.LONG, schema.getField("int64").getType());
    assertEquals(AttributeValueType.LONG, schema.getField("uint64").getType());
    assertEquals(AttributeValueType.FLOAT, schema.getField("float").getType());
    assertEquals(AttributeValueType.ARRAY, schema.getField("bytes").getType());
    assertEquals(
        AttributeValueType.BYTE, schema.getField("bytes").asArrayTypeDescriptor().getValueType());
  }

  private void checkIfSchemaContainsAllFields(
      StructureTypeDescriptor<?> schema, Descriptor message) {
    message
        .getFields()
        .forEach(
            fieldDescriptor -> {
              assertTrue(
                  "Missing field " + fieldDescriptor.getName() + "in schema.",
                  schema.hasField(fieldDescriptor.getName()));
            });
  }
}
