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

import static cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE;
import static cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX;
import static cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.PARQUET_COLUMN_NAME_KEY;
import static cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION;
import static cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP;
import static cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.PARQUET_COLUMN_NAME_UUID;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.Operation;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.scheme.AttributeValueAccessor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

@Slf4j
@Internal
public class StreamElementWriteSupport extends WriteSupport<StreamElement> {

  private final MessageType parquetSchema;
  private final String attributeNamesPrefix;
  private final StreamElementWriter streamElementWriter;
  RecordConsumer recordConsumer;

  public StreamElementWriteSupport(MessageType parquetSchema, String attributeNamesPrefix) {
    this.parquetSchema = parquetSchema;
    this.attributeNamesPrefix = attributeNamesPrefix;
    this.streamElementWriter = new StreamElementWriter(parquetSchema);
  }

  @Override
  public WriteContext init(Configuration configuration) {
    final Map<String, String> extraMetadata =
        Collections.singletonMap(
            ParquetFileFormat.PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME, attributeNamesPrefix);
    return new WriteContext(parquetSchema, extraMetadata);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(StreamElement record) {
    Preconditions.checkNotNull(this.recordConsumer, "RecordConsumer can not be null.");
    streamElementWriter.write(record);
  }

  <T> Writer<?> createWriter(
      SchemaTypeDescriptor<T> schema, String attributeName, GroupType parquetSchema) {
    Writer<?> writer;
    switch (schema.getType()) {
      case ARRAY:
        if (schema.asArrayTypeDescriptor().getValueType().equals(AttributeValueType.BYTE)) {
          writer =
              createWriter(
                  schema.asArrayTypeDescriptor().getValueDescriptor(),
                  attributeName,
                  parquetSchema);
        } else {
          writer =
              new ArrayWriter<>(
                  createWriter(
                      schema.asArrayTypeDescriptor().getValueDescriptor(),
                      ArrayWriter.ELEMENT_FIELD_NAME,
                      parquetSchema
                          .getType(attributeName)
                          .asGroupType()
                          .getType(ArrayWriter.LIST_FIELD_NAME)
                          .asGroupType()));
        }
        break;
      case STRUCTURE:
        writer =
            new StructureWriter(
                schema.asStructureTypeDescriptor(),
                parquetSchema.getType(attributeName).asGroupType());
        break;
      case INT:
        writer = new IntWriter();
        break;
      case LONG:
        writer = new LongWriter();
        break;
      case BOOLEAN:
        writer = new BooleanWriter();
        break;
      case ENUM:
      case STRING:
        writer = new StringWriter();
        break;
      case FLOAT:
        writer = new FloatWriter();
        break;
      case DOUBLE:
        writer = new DoubleWriter();
        break;
      case BYTE:
        writer = new BytesWriter();
        break;
      default:
        throw new UnsupportedOperationException("Unknown type " + schema.getType());
    }
    writer.setName(attributeName);
    writer.setIndex(parquetSchema.getFieldIndex(attributeName));
    return writer;
  }

  interface Writer<T> {

    void setName(String name);

    void setIndex(int index);

    default void writeRawValue(T value) {
      throw new UnsupportedOperationException("Method writeRawValue should be overridden.");
    }

    void write(T value);
  }

  class StreamElementWriter extends GenericFieldWriter<StreamElement> {

    final Map<String, Writer<?>> writers = new HashMap<>();

    StreamElementWriter(GroupType schema) {
      final SchemaTypeDescriptor<?> stringType = SchemaDescriptors.strings();
      final SchemaTypeDescriptor<?> longType = SchemaDescriptors.longs();
      final SchemaTypeDescriptor<?> operationType =
          SchemaDescriptors.enums(
              Arrays.stream(Operation.values())
                  .map(Operation::getValue)
                  .collect(Collectors.toList()));
      writers.put(
          PARQUET_COLUMN_NAME_KEY, createWriter(stringType, PARQUET_COLUMN_NAME_KEY, schema));
      writers.put(
          PARQUET_COLUMN_NAME_UUID, createWriter(stringType, PARQUET_COLUMN_NAME_UUID, schema));
      writers.put(
          PARQUET_COLUMN_NAME_TIMESTAMP,
          createWriter(longType, PARQUET_COLUMN_NAME_TIMESTAMP, schema));
      writers.put(
          PARQUET_COLUMN_NAME_OPERATION,
          createWriter(operationType, PARQUET_COLUMN_NAME_OPERATION, schema));
      writers.put(
          PARQUET_COLUMN_NAME_ATTRIBUTE,
          createWriter(stringType, PARQUET_COLUMN_NAME_ATTRIBUTE, schema));
      writers.put(
          PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX,
          createWriter(stringType, PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX, schema));
    }

    @Override
    public void write(StreamElement element) {

      final AttributeDescriptor<?> attributeDescriptor = element.getAttributeDescriptor();
      final String attributePrefix =
          attributeNamesPrefix
              + ((attributeDescriptor.isWildcard())
                  ? attributeDescriptor.toAttributePrefix(true) + "*"
                  : attributeDescriptor.toAttributePrefix());

      final Map<String, Object> row = new HashMap<>();
      row.put(PARQUET_COLUMN_NAME_KEY, element.getKey());
      row.put(PARQUET_COLUMN_NAME_UUID, element.getUuid());
      row.put(PARQUET_COLUMN_NAME_TIMESTAMP, element.getStamp());
      row.put(PARQUET_COLUMN_NAME_OPERATION, Operation.fromElement(element).getValue());
      row.put(PARQUET_COLUMN_NAME_ATTRIBUTE, element.getAttribute());
      row.put(PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX, attributePrefix);

      @SuppressWarnings("unchecked")
      final SchemaTypeDescriptor<Object> attributeSchema =
          (SchemaTypeDescriptor<Object>) element.getAttributeDescriptor().getSchemaTypeDescriptor();
      @SuppressWarnings("unchecked")
      AttributeValueAccessor<Object, Object> valueAccessor =
          (AttributeValueAccessor<Object, Object>)
              element.getAttributeDescriptor().getValueSerializer().getValueAccessor();

      recordConsumer.startMessage();

      Optional<Object> elementValue = element.getParsed();
      if (elementValue.isPresent()) {
        Object value = valueAccessor.valueOf(elementValue.get());
        final String storedAttributeName =
            attributeNamesPrefix + attributeDescriptor.toAttributePrefix(false);
        row.put(storedAttributeName, value);
        writers.computeIfAbsent(
            storedAttributeName,
            name -> createWriter(attributeSchema, storedAttributeName, parquetSchema));
      }
      row.forEach(
          (name, value) -> {
            @SuppressWarnings("unchecked")
            Writer<Object> writer = (Writer<Object>) writers.get(name);
            writer.write(value);
          });
      recordConsumer.endMessage();
    }
  }

  abstract class GenericFieldWriter<T> implements Writer<T> {

    @Setter String name;
    @Setter int index = -1;

    public void write(T value) {
      Preconditions.checkState(index >= 0, "Index can not be negative.");
      Preconditions.checkState(!name.isEmpty(), "Attribute name can not be empty.");
      recordConsumer.startField(name, index);
      writeRawValue(value);
      recordConsumer.endField(name, index);
    }
  }

  class StructureWriter extends GenericFieldWriter<Map<String, Object>> {

    final Map<String, Writer<Object>> fieldWriters = new HashMap<>();

    StructureWriter(StructureTypeDescriptor<?> descriptor, GroupType schema) {
      descriptor
          .getFields()
          .forEach(
              (name, type) -> {
                Writer<?> writer = createWriter(descriptor.getField(name), name, schema);
                @SuppressWarnings("unchecked")
                Writer<Object> cast = (Writer<Object>) writer;
                fieldWriters.put(name, cast);
              });
    }

    @Override
    public void writeRawValue(Map<String, Object> value) {
      recordConsumer.startGroup();
      fieldWriters.forEach(
          (name, writer) -> {
            if (value.containsKey(name)) {
              writer.write(value.get(name));
            }
          });
      recordConsumer.endGroup();
    }

    @Override
    public void write(Map<String, Object> value) {
      if (value.size() > 0) {
        recordConsumer.startField(name, index);
        writeRawValue(value);
        recordConsumer.endField(name, index);
      }
    }
  }

  class StringWriter extends GenericFieldWriter<String> {

    @Override
    public void writeRawValue(String value) {
      recordConsumer.addBinary(Binary.fromString(value));
    }
  }

  class IntWriter extends GenericFieldWriter<Integer> {

    @Override
    public void writeRawValue(Integer value) {
      recordConsumer.addInteger(value);
    }
  }

  class LongWriter extends GenericFieldWriter<Long> {

    @Override
    public void writeRawValue(Long value) {
      recordConsumer.addLong(value);
    }
  }

  class BooleanWriter extends GenericFieldWriter<Boolean> {

    @Override
    public void writeRawValue(Boolean value) {
      recordConsumer.addBoolean(value);
    }
  }

  class FloatWriter extends GenericFieldWriter<Float> {

    @Override
    public void writeRawValue(Float value) {
      recordConsumer.addFloat(value);
    }
  }

  class DoubleWriter extends GenericFieldWriter<Double> {

    @Override
    public void writeRawValue(Double value) {
      recordConsumer.addDouble(value);
    }
  }

  class BytesWriter extends GenericFieldWriter<byte[]> {

    @Override
    public void writeRawValue(byte[] value) {
      recordConsumer.addBinary(Binary.fromReusedByteArray(value));
    }
  }

  class ArrayWriter<T> extends GenericFieldWriter<List<T>> {
    static final String ELEMENT_FIELD_NAME = "element";
    static final String LIST_FIELD_NAME = "list";

    final Writer<T> fieldWriter;

    ArrayWriter(Writer<T> fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    public void write(List<T> value) {
      if (!value.isEmpty()) {
        recordConsumer.startField(name, index);
        recordConsumer.startGroup();
        recordConsumer.startField(
            LIST_FIELD_NAME, 0); // This is the wrapper group for the array field
        for (T val : value) {
          recordConsumer.startGroup();
          recordConsumer.startField(ELEMENT_FIELD_NAME, 0); // This is the mandatory inner field

          fieldWriter.writeRawValue(val);

          recordConsumer.endField(ELEMENT_FIELD_NAME, 0);
          recordConsumer.endGroup();
        }

        recordConsumer.endField(LIST_FIELD_NAME, 0);
        recordConsumer.endGroup();
        recordConsumer.endField(name, index);
      }
    }
  }
}
