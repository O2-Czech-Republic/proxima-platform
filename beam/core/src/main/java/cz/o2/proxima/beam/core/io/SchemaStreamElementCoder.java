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
package cz.o2.proxima.beam.core.io;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.scheme.AttributeValueAccessor;
import cz.o2.proxima.core.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.core.scheme.AttributeValueType;
import cz.o2.proxima.core.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.core.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.core.scheme.ValueSerializer;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Builder;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Row.FieldValueBuilder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class SchemaStreamElementCoder extends SchemaCoder<StreamElement> {

  public static final Map<AttributeValueType, FieldType> FIELD_TYPES =
      ImmutableMap.<AttributeValueType, FieldType>builder()
          .put(AttributeValueType.BOOLEAN, FieldType.BOOLEAN)
          .put(AttributeValueType.BYTE, FieldType.BYTE)
          .put(AttributeValueType.DOUBLE, FieldType.DOUBLE)
          .put(AttributeValueType.FLOAT, FieldType.FLOAT)
          .put(AttributeValueType.INT, FieldType.INT32)
          .put(AttributeValueType.LONG, FieldType.INT64)
          .put(AttributeValueType.STRING, FieldType.STRING)
          .build();
  private static final Map<AttributeDescriptor<?>, EntityAwareAttributeDescriptor<Object>>
      ATTR_CACHE = new HashMap<>();

  private static final Map<String, EntityAwareAttributeDescriptor<Object>> ATTR_NAME_CACHE =
      new HashMap<>();

  public static SchemaStreamElementCoder of(Repository repo) {
    if (ATTR_CACHE.isEmpty()) {
      repo.getAllEntities()
          .flatMap(e -> e.getAllAttributes().stream().map(a -> Pair.of(e, a)))
          .forEach(
              p -> {
                EntityAwareAttributeDescriptor<Object> aware = asAware(p.getFirst(), p.getSecond());
                ATTR_CACHE.put(p.getSecond(), aware);
                ATTR_NAME_CACHE.put(asAttributeFieldName(p.getFirst(), p.getSecond()), aware);
              });
    }
    return new SchemaStreamElementCoder(asSchema(repo));
  }

  private static String asAttributeFieldName(String entity, String attribute) {
    return entity + "_" + attribute;
  }

  private static String asAttributeFieldName(EntityDescriptor entity, AttributeDescriptor<?> attr) {
    return asAttributeFieldName(entity.getName(), attr.toAttributePrefix(false));
  }

  private static String asAttributeFieldName(StreamElement element) {
    return asAttributeFieldName(element.getEntityDescriptor(), element.getAttributeDescriptor());
  }

  @SuppressWarnings("unchecked")
  private static EntityAwareAttributeDescriptor<Object> asAware(
      EntityDescriptor entity, AttributeDescriptor<?> attr) {
    if (attr.isWildcard()) {
      return Wildcard.of(entity, (AttributeDescriptor<Object>) attr);
    }
    return Regular.of(entity, (AttributeDescriptor<Object>) attr);
  }

  private static Schema asSchema(Repository repo) {
    Builder builder =
        Schema.builder()
            .addStringField("key")
            .addInt64Field("stamp")
            // entity.attribute format
            .addStringField("attributeName")
            .addBooleanField("delete")
            .addNullableStringField("attribute")
            .addNullableStringField("uuid")
            .addNullableInt64Field("seqId");
    repo.getAllEntities()
        .flatMap(e -> e.getAllAttributes().stream())
        .forEach(attr -> addToBuilder(attr, builder));
    return builder.build();
  }

  private static void addToBuilder(AttributeDescriptor<?> attr, Builder builder) {
    String entity = attr.getEntity();
    String name = attr.toAttributePrefix(false);
    addFieldToBuilder(
        asAttributeFieldName(entity, name),
        attr.getValueSerializer().getValueSchemaDescriptor(),
        builder);
  }

  private static void addFieldToBuilder(
      String name, SchemaTypeDescriptor<?> valueSchemaDescriptor, Builder builder) {

    builder.addNullableField(name, getRequiredField(valueSchemaDescriptor));
  }

  private static @Nonnull FieldType getRequiredField(SchemaTypeDescriptor<?> schema) {
    AttributeValueType type = schema.getType();
    if (type.equals(AttributeValueType.STRUCTURE)) {
      StructureTypeDescriptor<?> structure = schema.asStructureTypeDescriptor();
      Schema.Builder builder = Schema.builder();
      structure.getFields().forEach((n, f) -> addFieldToBuilder(n, f, builder));
      return FieldType.row(builder.build());
    } else if (type.equals(AttributeValueType.ARRAY)) {
      ArrayTypeDescriptor<?> arrayDesc = schema.asArrayTypeDescriptor();
      if (arrayDesc.getValueType().equals(AttributeValueType.BYTE)) {
        return FieldType.BYTES;
      }
      return FieldType.array(getRequiredField(arrayDesc.getValueDescriptor()));
    } else {
      return Objects.requireNonNull(
          FIELD_TYPES.get(type), () -> String.format("Unknown type %s", type));
    }
  }

  private static StreamElement fromRow(Row row) {
    String key = row.getString("key");
    long stamp = Objects.requireNonNull(row.getInt64("stamp"));
    @Nullable String attribute = row.getString("attribute");
    String attributeName = row.getString("attributeName");
    boolean delete = Objects.requireNonNull(row.getBoolean("delete"));
    String uuid = row.getString("uuid");
    Long seqId = row.getInt64("seqId");
    EntityAwareAttributeDescriptor<Object> entityAware =
        Objects.requireNonNull(
            ATTR_NAME_CACHE.get(attributeName),
            () -> String.format("Missing attribute %s", attributeName));
    if (entityAware.isWildcard()) {
      Preconditions.checkArgumentNotNull(attribute);
      Wildcard<Object> wildcard = (Wildcard<Object>) entityAware;
      if (delete) {
        if (attribute.endsWith(".*")) {
          if (seqId == null) {
            return wildcard.deleteWildcard(uuid, key, stamp);
          }
          return wildcard.deleteWildcard(seqId, key, stamp);
        }
        if (seqId == null) {
          return wildcard.delete(uuid, key, attribute, stamp);
        }
        return wildcard.delete(seqId, key, attribute, stamp);
      }
      Object value =
          entityAware
              .getValueSerializer()
              .getValueAccessor()
              .createFrom(row.getBaseValue(attributeName));
      if (seqId == null) {
        return wildcard.upsert(uuid, key, attribute, stamp, value);
      }
      return wildcard.upsert(seqId, key, attribute, stamp, value);
    }
    Regular<Object> regular = (Regular<Object>) entityAware;
    if (delete) {
      if (seqId == null) {
        return regular.delete(uuid, key, stamp);
      }
      return regular.delete(seqId, key, stamp);
    }
    Object value =
        entityAware
            .getValueSerializer()
            .getValueAccessor()
            .createFrom(
                fromFieldType(
                    row.getSchema().getField(attributeName).getType(),
                    row.getValue(attributeName)));
    if (seqId == null) {
      return regular.upsert(uuid, key, stamp, value);
    }
    return regular.upsert(seqId, key, stamp, value);
  }

  private static Row toRow(Schema schema, StreamElement element) {
    Objects.requireNonNull(
        ATTR_CACHE.get(element.getAttributeDescriptor()),
        () -> String.format("Missing attribute %s", element.getAttributeDescriptor()));
    String attributeName = asAttributeFieldName(element);
    FieldValueBuilder builder =
        Row.withSchema(schema)
            .withFieldValue("key", element.getKey())
            .withFieldValue("stamp", element.getStamp())
            .withFieldValue("attributeName", attributeName)
            .withFieldValue("delete", element.isDelete());
    if (element.getAttributeDescriptor().isWildcard()) {
      builder =
          builder.withFieldValue(
              "attribute",
              element
                  .getAttribute()
                  .substring(element.getAttributeDescriptor().toAttributePrefix().length()));
    }
    if (element.hasSequentialId()) {
      builder = builder.withFieldValue("seqId", element.getSequentialId());
    } else {
      builder = builder.withFieldValue("uuid", element.getUuid());
    }
    if (!element.isDelete()) {
      Object parsed = Optionals.get(element.getParsed());
      @SuppressWarnings("unchecked")
      ValueSerializer<Object> valueSerializer =
          (ValueSerializer<Object>) element.getAttributeDescriptor().getValueSerializer();
      Object mapped =
          intoFieldType(
              valueSerializer.getValueAccessor().valueOf(parsed),
              schema.getField(attributeName).getType());
      builder = builder.withFieldValue(attributeName, mapped);
    }
    return builder.build();
  }

  /**
   * Convert the given accessor to object that is compatible with Beam Schema.
   *
   * @param accessor the object returned by Proxima {@link AttributeValueAccessor}
   * @param field the type to convert the object to
   * @return object compatible with equal Beam {@link FieldType}
   */
  private static Object intoFieldType(Object accessor, FieldType field) {
    if (field.getRowSchema() != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) accessor;
      return asRow(map, field);
    }
    return accessor;
  }

  private static Object fromFieldType(FieldType type, Object value) {
    if (type.getRowSchema() != null) {
      return StructureValue.of(asStructureValue((Row) value));
    }
    return value;
  }

  private static Row asRow(Map<String, Object> map, FieldType field) {
    Schema schema = Objects.requireNonNull(field.getRowSchema());
    return Row.withSchema(schema).withFieldValues(map).build();
  }

  private static Map<String, Object> asStructureValue(Row value) {
    Map<String, Object> res = new HashMap<>();
    for (Field f : value.getSchema().getFields()) {
      Object fieldValue = value.getValue(f.getName());
      if (fieldValue != null) {
        if (f.getType().getRowSchema() != null) {
          res.put(f.getName(), asStructureValue((Row) fieldValue));
        } else {
          res.put(f.getName(), fieldValue);
        }
      }
    }
    return res;
  }

  private SchemaStreamElementCoder(Schema schema) {
    super(
        schema,
        TypeDescriptor.of(StreamElement.class),
        elem -> toRow(schema, elem),
        SchemaStreamElementCoder::fromRow);
  }
}
