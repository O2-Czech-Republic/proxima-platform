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
package cz.o2.proxima.direct.bulk.fs.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.utils.ProtoUtils;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;
import org.mockito.Mockito;

@Slf4j
public class ParquetUtilsTest {

  final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  final EntityDescriptor gateway = repo.getEntity("gateway");

  @Test
  public void testCreateParquetSchemaForAttributeDescriptor() {
    AttributeDescriptor<byte[]> attr = gateway.getAttribute("armed");
    String prefix = "prefix_";
    Type parquet = ParquetUtils.createParquetSchema(attr, prefix);
    log.debug("Parquet schema: {}", parquet);
    assertEquals(prefix + attr.getName(), parquet.getName());
    assertTrue(parquet.isPrimitive());
    assertEquals(Repetition.OPTIONAL, parquet.getRepetition());
    assertEquals(PrimitiveTypeName.BINARY, parquet.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test
  public void testCreateParquetSchemaForWildcardAttribute() {
    AttributeDescriptor<byte[]> attr = gateway.getAttribute("device.*");
    Type parquet = ParquetUtils.createParquetSchema(attr, "");
    log.debug("Parquet schema: {}", parquet);
    assertEquals(attr.toAttributePrefix(false), parquet.getName());
    assertTrue(parquet.isPrimitive());
    assertEquals(Repetition.OPTIONAL, parquet.getRepetition());
    assertEquals(PrimitiveTypeName.BINARY, parquet.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test
  public void testCreateParquetSchemaForAttributeFamily() {
    AttributeFamilyDescriptor family =
        repo.getFamiliesForAttribute(gateway.getAttribute("armed"))
            .stream()
            .filter(f -> f.getAccess().canReadBatchSnapshot())
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Unable to find attribute family."));

    MessageType parquet = ParquetUtils.createParquetSchema(family);
    log.debug("Parquet schema: {}", parquet);
    assertProximaFieldsInParquetSchema(parquet, false);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateParquetSchemaForComplexAttributeDescriptor() {
    AttributeDescriptor<Object> attr =
        (AttributeDescriptor<Object>) Mockito.mock(AttributeDescriptor.class);
    Mockito.when(attr.getName()).thenReturn("attribute_name");
    Mockito.when(attr.getSchemaTypeDescriptor())
        .thenReturn(
            SchemaDescriptors.structures("structure")
                .addField("field", SchemaDescriptors.strings()));
    Type parquet = ParquetUtils.createParquetSchema(attr, "");
    log.debug("Parquet schema: {}", parquet);
    assertFalse(parquet.isPrimitive());
    final GroupType group = parquet.asGroupType();
    assertEquals(Repetition.OPTIONAL, group.getRepetition());
    assertEquals("attribute_name", group.getName());
    assertTrue(group.containsField("field"));

    Optional<Type> field =
        group.getFields().stream().filter(f -> f.getName().equals("field")).findFirst();
    assertTrue(field.isPresent());
    assertEquals(Repetition.OPTIONAL, field.get().getRepetition());
    assertEquals(PrimitiveTypeName.BINARY, field.get().asPrimitiveType().getPrimitiveTypeName());
    assertEquals(LogicalTypeAnnotation.stringType(), field.get().getLogicalTypeAnnotation());
  }

  @Test
  public void testCreateParquetTypeFromSchemaTypeDescriptor() {
    Type type =
        ParquetUtils.mapSchemaTypeToParquet(
            SchemaDescriptors.structures("structure")
                .addField("string_field", SchemaDescriptors.strings())
                .addField("long_field", SchemaDescriptors.longs())
                .addField("float_field", SchemaDescriptors.floats())
                .addField("enum_type", SchemaDescriptors.enums(Arrays.asList("LEFT", "RIGHT")))
                .addField("bytes_field", SchemaDescriptors.bytes())
                .addField(
                    "array_of_strings_field", SchemaDescriptors.arrays(SchemaDescriptors.strings()))
                .addField(
                    "array_of_double_field", SchemaDescriptors.arrays(SchemaDescriptors.doubles()))
                .addField(
                    "structure_field_with_primitive",
                    SchemaDescriptors.structures("inner_structure1")
                        .addField("inner_integer", SchemaDescriptors.integers()))
                .addField(
                    "structure_field_with_array",
                    SchemaDescriptors.structures("inner_structure2")
                        .addField(
                            "inner_array", SchemaDescriptors.arrays(SchemaDescriptors.doubles())))
                .addField(
                    "structure_field_with_structure",
                    SchemaDescriptors.structures("inner_structure3")
                        .addField(
                            "inner_structure",
                            SchemaDescriptors.structures("inner_inner_structure")
                                .addField("inner_inner_field1", SchemaDescriptors.bytes())
                                .addField("inner_inner_field2", SchemaDescriptors.integers()))),
            "value");
    log.debug("Parquet schema: {}", type);
    List<String> expectedFields =
        Arrays.asList(
            "optional binary string_field (STRING)",
            "optional int64 long_field",
            "optional float float_field",
            "optional binary enum_type (ENUM)",
            "optional binary bytes_field",
            "optional group array_of_strings_field (LIST) {repeated group list {optional binary element (STRING);}}",
            "optional group array_of_double_field (LIST) {repeated group list {optional double element;}}",
            "optional group structure_field_with_primitive {optional int32 inner_integer;}",
            "optional group structure_field_with_array {optional group inner_array (LIST) {repeated group list {optional double element;}}}",
            "optional group structure_field_with_structure {optional group inner_structure {"
                + "optional binary inner_inner_field1;optional int32 inner_inner_field2;}}");
    List<String> fields = parquetFieldAsString(type.asGroupType().getFields());
    assertEquals(expectedFields.size(), fields.size());
    assertTrue(fields.containsAll(expectedFields));
  }

  @Test
  public void testCreateProximaSchemaForStreamElement() {
    assertProximaFieldsInParquetSchema(
        ParquetUtils.createMessageWithFields(Collections.emptyList()), true);
  }

  @Test
  public void testCreateSchemaWitReservedAttributeShouldThrowsException() {
    AttributeDescriptor<byte[]> key =
        AttributeDescriptor.newBuilder(repo)
            .setEntity(gateway.getName())
            .setName("key")
            .setSchemeUri(URI.create("bytes:///"))
            .build();
    assertThrows(IllegalArgumentException.class, () -> ParquetUtils.createParquetSchema(key, ""));
    assertNotNull(ParquetUtils.createParquetSchema(key, "prefix"));
  }

  @Test
  public void testConvertComplexProtoBufToParquetSchema() {
    GroupType parquetSchema =
        ParquetUtils.mapSchemaTypeToParquet(
                ProtoUtils.convertProtoToSchema(ValueSchemeMessage.getDescriptor()), "complex")
            .asGroupType();
    log.info("Converted complex schema: {}", parquetSchema);
    GroupType repeatedInnerMessage =
        parquetSchema
            .getFields()
            .get(parquetSchema.getFieldIndex("repeated_inner_message"))
            .asGroupType();
    assertNotNull(repeatedInnerMessage);
    assertTrue(repeatedInnerMessage.isRepetition(Repetition.OPTIONAL));
    assertEquals(LogicalTypeAnnotation.listType(), repeatedInnerMessage.getLogicalTypeAnnotation());
    assertEquals("repeated_inner_message", repeatedInnerMessage.getName());
    assertFalse(repeatedInnerMessage.getFields().isEmpty());
  }

  private void assertProximaFieldsInParquetSchema(Type type, boolean checkSize) {
    assertFalse(type.isPrimitive());

    List<String> expectedFields =
        Arrays.asList(
            "required binary key (STRING)",
            "required binary uuid (STRING)",
            "required int64 timestamp (TIMESTAMP(MILLIS,true))",
            "required binary operation (ENUM)",
            "required binary attribute (STRING)",
            "required binary attribute_prefix (STRING)");

    List<String> fields = parquetFieldAsString(type.asGroupType().getFields());
    if (checkSize) {
      assertEquals(expectedFields.size(), fields.size());
    }
    assertTrue(fields.containsAll(expectedFields));
  }

  private List<String> parquetFieldAsString(List<Type> fields) {
    return fields
        .stream()
        .map(f -> f.toString().replaceAll("\n(\\s)*", ""))
        .collect(Collectors.toList());
  }
}
