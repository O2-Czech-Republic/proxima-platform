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
package cz.o2.proxima.scheme.proto.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme.Device;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import org.junit.Test;

public class ProtoUtilsTest {

  @Test
  public void testConvertSimpleProtoToSchema() {
    SchemaTypeDescriptor<Device> schema = ProtoUtils.convertProtoToSchema(Device.getDescriptor());
    assertEquals(AttributeValueType.STRUCTURE, schema.getType());
    assertEquals(2, schema.getStructureTypeDescriptor().getFields().size());
    assertEquals(
        AttributeValueType.STRING, schema.getStructureTypeDescriptor().getField("type").getType());
    assertEquals(
        AttributeValueType.ARRAY,
        schema.getStructureTypeDescriptor().getField("payload").getType());
    assertEquals(
        AttributeValueType.BYTE,
        schema
            .getStructureTypeDescriptor()
            .getField("payload")
            .getArrayTypeDescriptor()
            .getValueType());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConvertComplexProtoToSchema() {
    SchemaTypeDescriptor<ValueSchemeMessage> schema =
        ProtoUtils.convertProtoToSchema(ValueSchemeMessage.getDescriptor());
    assertEquals(AttributeValueType.STRUCTURE, schema.getType());
    StructureTypeDescriptor<ValueSchemeMessage> descriptor = schema.getStructureTypeDescriptor();
    assertEquals(ValueSchemeMessage.getDescriptor().getName(), descriptor.getName());
    assertTrue(descriptor.hasField("repeated_inner_message"));
    assertEquals(AttributeValueType.ARRAY, descriptor.getField("repeated_inner_message").getType());
    assertEquals(
        AttributeValueType.ENUM,
        descriptor
            .getField("inner_message")
            .getStructureTypeDescriptor()
            .getField("inner_enum")
            .getType());

    assertEquals(
        AttributeValueType.STRUCTURE,
        descriptor.getField("repeated_inner_message").getArrayTypeDescriptor().getValueType());
  }
}
