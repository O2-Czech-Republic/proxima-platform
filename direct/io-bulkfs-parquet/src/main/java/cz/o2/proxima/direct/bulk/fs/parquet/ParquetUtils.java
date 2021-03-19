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

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.fs.parquet.StreamElementWriteSupport.ArrayWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;

@Slf4j
@Internal
class ParquetUtils {

  private ParquetUtils() {
    // no-op
  }

  /**
   * Create parquet schema for given {@link AttributeFamilyDescriptor}.
   *
   * @param family attribute family
   * @return parquet message schema
   */
  static MessageType createParquetSchema(AttributeFamilyDescriptor family) {

    return ParquetUtils.createMessageWithFields(
        family
            .getAttributes()
            .stream()
            .map(
                attribute ->
                    createParquetSchema(
                        attribute,
                        family
                            .getCfg()
                            .getOrDefault(
                                ParquetFileFormat.PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME,
                                ParquetFileFormat.PARQUET_DEFAULT_VALUES_NAME_PREFIX)
                            .toString()))
            .collect(Collectors.toList()));
  }

  /**
   * Convert {@link AttributeDescriptor} to parquet type.
   *
   * @param attribute attribute
   * @param attributeNamesPrefix prefix used for parquet type name
   * @param <T> type
   * @return parquet type
   */
  static <T> Type createParquetSchema(
      AttributeDescriptor<T> attribute, String attributeNamesPrefix) {
    final SchemaTypeDescriptor<T> schema = attribute.getSchemaTypeDescriptor();
    String attributeName = attributeNamesPrefix + attribute.getName();
    if (attribute.isWildcard()) {
      attributeName = attributeNamesPrefix + attribute.toAttributePrefix(false);
    }
    validateAttributeName(attributeName);
    Type parquet;
    if (schema.isStructureType()) {
      parquet =
          Types.optionalGroup()
              .named(attributeName)
              .withNewFields(
                  schema
                      .asStructureTypeDescriptor()
                      .getFields()
                      .entrySet()
                      .stream()
                      .map(e -> mapSchemaTypeToParquet(e.getValue(), e.getKey()))
                      .collect(Collectors.toList()));
    } else {
      parquet = mapSchemaTypeToParquet(schema, attributeName);
    }
    return parquet;
  }

  /**
   * Map proxima {@link SchemaTypeDescriptor} to correspond parquet type.
   *
   * @param descriptor schema descriptor
   * @param name name for parquet type
   * @param <T> descriptor type
   * @return parquet {@link Type}
   */
  static <T> Type mapSchemaTypeToParquet(SchemaTypeDescriptor<T> descriptor, String name) {
    switch (descriptor.getType()) {
      case INT:
        return Types.optional(PrimitiveTypeName.INT32).named(name);
      case LONG:
        return Types.optional(PrimitiveTypeName.INT64).named(name);
      case DOUBLE:
        return Types.optional(PrimitiveTypeName.DOUBLE).named(name);
      case FLOAT:
        return Types.optional(PrimitiveTypeName.FLOAT).named(name);
      case BOOLEAN:
        return Types.optional(PrimitiveTypeName.BOOLEAN).named(name);
      case STRING:
        return Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(name);
      case ENUM:
        return Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.enumType())
            .named(name);
      case BYTE:
        return Types.optional(PrimitiveTypeName.BINARY).named(name);
      case ARRAY:
        SchemaTypeDescriptor<?> valueTypeDescriptor =
            descriptor.asArrayTypeDescriptor().getValueDescriptor();

        // proxima byte array should be encoded as binary
        if (valueTypeDescriptor.isPrimitiveType()
            && valueTypeDescriptor.getType().equals(AttributeValueType.BYTE)) {
          return mapSchemaTypeToParquet(valueTypeDescriptor, name);
        }

        return Types.optionalList()
            .setElementType(
                mapSchemaTypeToParquet(valueTypeDescriptor, ArrayWriter.ELEMENT_FIELD_NAME))
            .named(name);

      case STRUCTURE:
        GroupBuilder<GroupType> structure = Types.optionalGroup();
        descriptor
            .asStructureTypeDescriptor()
            .getFields()
            .forEach(
                (fieldName, des) -> structure.addField(mapSchemaTypeToParquet(des, fieldName)));

        return structure.named(name);
      default:
        throw new IllegalStateException("Unexpected value: " + descriptor.getType());
    }
  }

  /**
   * Create base {@link MessageType} for storing {@link cz.o2.proxima.storage.StreamElement} with
   * additional fields
   *
   * @param fieldsToAdd additional fields
   * @return message type for {@link cz.o2.proxima.storage.StreamElement}.
   */
  static MessageType createMessageWithFields(List<Type> fieldsToAdd) {
    List<Type> fields =
        new ArrayList<>(
            Arrays.asList(
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY),
                Types.required(PrimitiveTypeName.BINARY)
                    // @TODO when parquet 1.12 is released we should be able to use logicalType UUID
                    // More info: https://issues.apache.org/jira/browse/PARQUET-1827
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID),
                Types.required(PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP),
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.enumType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION),
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE),
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX)));
    fields.addAll(fieldsToAdd);
    return new MessageType(ParquetFileFormat.PARQUET_MESSAGE_NAME, fields);
  }

  /**
   * Check name for possible collision with top level fields.
   *
   * @param attributeName attribute name
   * @throws IllegalArgumentException for reserved names
   */
  private static void validateAttributeName(String attributeName) {
    if (Arrays.asList(
            ParquetFileFormat.PARQUET_COLUMN_NAME_KEY,
            ParquetFileFormat.PARQUET_COLUMN_NAME_UUID,
            ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION,
            ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP,
            ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE,
            ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX)
        .contains(attributeName)) {
      throw new IllegalArgumentException(
          String.format(
              "Attribute [%s] collides with reserved keyword. Please specify %s.",
              attributeName, ParquetFileFormat.PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME));
    }
  }
}
