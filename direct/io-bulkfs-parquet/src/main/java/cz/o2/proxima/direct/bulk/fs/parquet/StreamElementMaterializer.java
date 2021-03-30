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

import static java.util.Optional.of;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.Operation;
import cz.o2.proxima.direct.bulk.fs.parquet.StreamElementMaterializer.ParquetColumnGroup.ParquetColumn;
import cz.o2.proxima.direct.bulk.fs.parquet.StreamElementWriteSupport.ArrayWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.scheme.AttributeValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/** Class responsible for converting parquet record into {@link StreamElement}. */
@Slf4j
@Internal
public class StreamElementMaterializer extends RecordMaterializer<StreamElement> {

  private final ParquetColumnRecordConverter root;
  private final MessageType schema;
  private final EntityDescriptor entity;
  private final String attributeNamesPrefix;

  public StreamElementMaterializer(
      MessageType schema, EntityDescriptor entity, String attributeNamesPrefix) {
    this.schema = schema;
    this.entity = entity;
    this.attributeNamesPrefix = attributeNamesPrefix;
    this.root = new ParquetColumnRecordConverter(schema, null, null);
  }

  @Override
  public StreamElement getCurrentRecord() {

    Map<String, Object> record = recordToMap(schema, root.getCurrentRecord(), new HashMap<>());

    final String key = getColumnFromRow(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY, record);
    final String operation =
        getColumnFromRow(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION, record);
    final String attributeName =
        getColumnFromRow(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX, record);
    Optional<AttributeDescriptor<Object>> attribute = entity.findAttribute(attributeName);
    if (!attribute.isPresent()) {
      // current attribute is not in entity -> skip
      log.info(
          "Skipping attribute [{}] which is not in current entity [{}].",
          attributeName,
          entity.getName());
      return null;
    }
    final String uuid = getColumnFromRow(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID, record);
    final long timestamp =
        getColumnFromRow(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP, record);
    switch (Operation.of(operation)) {
      case DELETE:
        return StreamElement.delete(entity, attribute.get(), uuid, key, attributeName, timestamp);
      case DELETE_WILDCARD:
        return StreamElement.deleteWildcard(entity, attribute.get(), uuid, key, timestamp);
      case UPSERT:
        return StreamElement.upsert(
            entity,
            attribute.get(),
            uuid,
            key,
            attributeName,
            timestamp,
            getValueFromCurrentRowData(attribute.get(), record));
      default:
        throw new RecordMaterializationException("Unknown operation " + operation);
    }
  }

  private byte[] getValueFromCurrentRowData(
      AttributeDescriptor<?> attribute, Map<String, Object> record) {
    final String storedAttributeName = attributeNamesPrefix + attribute.toAttributePrefix(false);
    @SuppressWarnings("unchecked")
    final ValueSerializer<Object> valueSerializer =
        (ValueSerializer<Object>) attribute.getValueSerializer();
    final AttributeValueAccessor<Object, Object> valueAccessor = valueSerializer.getValueAccessor();
    final SchemaTypeDescriptor<Object> attributeSchema = valueSerializer.getValueSchemaDescriptor();
    Object attributeValue;
    // Note: There is nothing written into parquet file when we are writing empty element. In that
    // case we need to delegate creation of empty element to serializer.
    // This is reason for that record.getOrDefault below.
    if (attributeSchema.isStructureType()) {
      attributeValue = record.getOrDefault(storedAttributeName, new HashMap<>());
      @SuppressWarnings("unchecked")
      final StructureValue cast = StructureValue.of((Map<String, Object>) attributeValue);
      return valueSerializer.serialize(valueAccessor.createFrom(cast));
    } else {
      attributeValue = record.getOrDefault(storedAttributeName, new Object());
      return valueSerializer.serialize(valueAccessor.createFrom(attributeValue));
    }
  }

  @SuppressWarnings("unchecked")
  private <V> V getColumnFromRow(String key, Map<String, Object> row) {
    return (V)
        Optional.ofNullable(row.getOrDefault(key, null))
            .orElseThrow(
                () ->
                    new IllegalStateException("Unable to get required key [" + key + "] from row"));
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

  private Map<String, Object> recordToMap(
      Type schema, ParquetColumnGroup record, Map<String, Object> current) {
    for (ParquetColumn value : record.getColumns()) {
      Type fieldType = schema.asGroupType().getType(value.getName());

      if (fieldType.isPrimitive()) {
        current.put(value.getName(), value.getValue());
      } else {
        LogicalTypeAnnotation typeAnnotation = fieldType.asGroupType().getLogicalTypeAnnotation();

        if (typeAnnotation != null && typeAnnotation.equals(LogicalTypeAnnotation.listType())) {
          List<Object> listValues = new ArrayList<>();
          for (ParquetColumnGroup listRecord :
              ((ParquetColumnListGroup) value.getValue()).getListElements()) {
            Type listType =
                schema
                    .asGroupType()
                    .getType(value.getName())
                    .asGroupType()
                    .getType(ArrayWriter.LIST_FIELD_NAME);
            Map<String, Object> v = recordToMap(listType, listRecord, new HashMap<>());
            listValues.add(v.get(ArrayWriter.ELEMENT_FIELD_NAME));
          }

          current.put(fieldType.getName(), listValues);

        } else {
          current.put(
              value.getName(),
              recordToMap(
                  schema.asGroupType().getType(value.getName()),
                  (ParquetColumnGroup) value.getValue(),
                  new HashMap<>()));
        }
      }
    }

    return current;
  }

  public static class ParquetColumnListGroup extends ParquetColumnGroup {

    @Override
    List<ParquetColumnGroup> getListElements() {

      List<ParquetColumnGroup> elements = new ArrayList<>();

      for (ParquetColumn value : getColumns()) {
        if (value.getName().equals(ArrayWriter.LIST_FIELD_NAME)) {
          elements.add((ParquetColumnGroup) value.getValue());
        }
      }
      return elements;
    }
  }

  public static class ParquetColumnGroup {

    private final List<ParquetColumn> columns = new ArrayList<>();

    public void add(String name, Object value) {
      columns.add(new ParquetColumn(name, value));
    }

    List<ParquetColumnGroup> getListElements() {
      throw new UnsupportedOperationException("Unable to get list element from not list object");
    }

    public List<ParquetColumn> getColumns() {
      return Collections.unmodifiableList(columns);
    }

    @Value
    public static class ParquetColumn {

      String name;
      Object value;
    }
  }

  public static class ParquetColumnListRecordConverter extends ParquetColumnRecordConverter {

    public ParquetColumnListRecordConverter(
        GroupType schema, String name, ParquetColumnRecordConverter parent) {
      super(schema, name, parent);
    }

    @Override
    public void start() {
      record = new ParquetColumnListGroup();
    }
  }

  public static class ParquetColumnRecordConverter extends GroupConverter {

    private final Converter[] converters;
    @Nullable private final String name;
    @Nullable private final ParquetColumnRecordConverter parent;

    protected ParquetColumnGroup record;

    public ParquetColumnRecordConverter(
        GroupType schema, String name, ParquetColumnRecordConverter parent) {
      this.converters = new Converter[schema.getFieldCount()];
      this.name = name;
      this.parent = parent;

      int i = 0;
      for (Type field : schema.getFields()) {
        converters[i++] = createConverter(field);
      }
    }

    private Converter createConverter(Type field) {
      LogicalTypeAnnotation ltype = field.getLogicalTypeAnnotation();
      if (field.isPrimitive()) {
        if (ltype != null) {
          return ltype
              .accept(
                  new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
                    @Override
                    public Optional<Converter> visit(
                        LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                      return of(new StringConverter(field.getName()));
                    }

                    @Override
                    public Optional<Converter> visit(
                        LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
                      return of(new StringConverter(field.getName()));
                    }
                  })
              .orElse(new NamedPrimitiveConverter(field.getName()));
        }
        return new NamedPrimitiveConverter(field.getName());
      }

      GroupType groupType = field.asGroupType();
      if (ltype != null) {
        return ltype
            .accept(
                new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
                  @Override
                  public Optional<Converter> visit(
                      LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
                    return of(
                        new ParquetColumnListRecordConverter(
                            groupType, field.getName(), ParquetColumnRecordConverter.this));
                  }
                })
            .orElse(new ParquetColumnRecordConverter(groupType, field.getName(), this));
      }
      return new ParquetColumnRecordConverter(groupType, field.getName(), this);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    public ParquetColumnGroup getCurrentRecord() {
      Preconditions.checkState(record != null, "GetCurrentRecord should be called after start.");
      return record;
    }

    @Override
    public void start() {
      record = new ParquetColumnGroup();
    }

    @Override
    public void end() {
      if (parent != null) {
        parent.getCurrentRecord().add(name, record);
      }
    }

    private class NamedPrimitiveConverter extends PrimitiveConverter {

      protected final String name;

      public NamedPrimitiveConverter(String name) {
        this.name = name;
      }

      @Override
      public void addBinary(Binary value) {
        record.add(name, value.getBytes());
      }

      @Override
      public void addBoolean(boolean value) {
        record.add(name, value);
      }

      @Override
      public void addDouble(double value) {
        record.add(name, value);
      }

      @Override
      public void addFloat(float value) {
        record.add(name, value);
      }

      @Override
      public void addInt(int value) {
        record.add(name, value);
      }

      @Override
      public void addLong(long value) {
        record.add(name, value);
      }
    }

    private class StringConverter extends NamedPrimitiveConverter {

      public StringConverter(String name) {
        super(name);
      }

      @Override
      public void addBinary(Binary value) {
        record.add(name, value.toStringUsingUTF8());
      }
    }
  }
}
