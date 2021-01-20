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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

/** SchemaDescriptors for types. */
public class SchemaDescriptors {

  private SchemaDescriptors() {
    // no-op
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for specific {@link AttributeValueType}.
   *
   * @param type primitive type
   * @param <T> descriptor type
   * @return primitive type descriptor.
   */
  public static <T> PrimitiveTypeDescriptor<T> primitives(AttributeValueType type) {
    return new PrimitiveTypeDescriptor<>(type);
  }

  /**
   * Create {@link EnumTypeDescriptor} for {@link AttributeValueType#ENUM} type.
   *
   * @param values possible values
   * @param <T> type of descriptor
   * @return enum type descriptor
   */
  public static <T extends Serializable> EnumTypeDescriptor<T> enums(T[] values) {
    return enums(Arrays.asList(values));
  }

  /**
   * Create {@link EnumTypeDescriptor} for {@link AttributeValueType#ENUM} type.
   *
   * @param values possible values
   * @param <T> type of descriptor
   * @return enum type descriptor
   */
  public static <T extends Serializable> EnumTypeDescriptor<T> enums(List<T> values) {
    return new EnumTypeDescriptor<>(values);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code String}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<String> strings() {
    return primitives(AttributeValueType.STRING);
  }

  /**
   * Create {@link ArrayTypeDescriptor} for byte array.
   *
   * @return Array type descriptor
   */
  public static ArrayTypeDescriptor<byte[]> bytes() {
    return arrays(primitives(AttributeValueType.BYTE));
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Integer}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Integer> integers() {
    return primitives(AttributeValueType.INT);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Long}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Long> longs() {
    return primitives(AttributeValueType.LONG);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Double}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Double> doubles() {
    return primitives(AttributeValueType.DOUBLE);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Float}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Float> floats() {
    return primitives(AttributeValueType.FLOAT);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Boolean}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Boolean> booleans() {
    return primitives(AttributeValueType.BOOLEAN);
  }

  /**
   * Create {@link ArrayTypeDescriptor}.
   *
   * @param descriptor primitive type
   * @param <T> value type
   * @return Array type descriptor
   */
  public static <T> ArrayTypeDescriptor<T> arrays(TypeDescriptor<T> descriptor) {
    return new ArrayTypeDescriptor<>(descriptor.toTypeDescriptor());
  }

  /**
   * Create {@link StructureTypeDescriptor} with name
   *
   * @param name structure name
   * @param <T> structure type
   * @return Structure type descriptor
   */
  public static <T> StructureTypeDescriptor<T> structures(String name) {
    return structures(name, Collections.emptyMap());
  }

  public static <T> StructureTypeDescriptor<T> structures(
      String name, Map<String, TypeDescriptor<?>> fields) {
    Map<String, SchemaTypeDescriptor<?>> f =
        fields
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Entry::getKey, v -> v.getValue().toTypeDescriptor()));

    return new StructureTypeDescriptor<>(name, f);
  }

  /**
   * Interface describing TypeDescriptors
   *
   * @param <T> descriptor type
   */
  public interface TypeDescriptor<T> extends Serializable {

    /**
     * Get descriptor type
     *
     * @return descriptor type
     */
    AttributeValueType getType();

    /**
     * Convert descriptor to {@link SchemaTypeDescriptor}.
     *
     * @return schema type descriptor
     */
    SchemaTypeDescriptor<T> toTypeDescriptor();
  }

  /**
   * Generic type descriptor. Parent class for other types
   *
   * @param <T> value type
   */
  public abstract static class GenericTypeDescriptor<T> implements TypeDescriptor<T> {

    private static final long serialVersionUID = 1L;

    /** Value type */
    @Getter protected final AttributeValueType type;

    protected GenericTypeDescriptor(AttributeValueType type) {
      this.type = type;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof GenericTypeDescriptor) {
        return type.equals(((GenericTypeDescriptor<?>) o).getType());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type);
    }
  }

  /**
   * SchemaTypeDescriptor wrapper
   *
   * @param <T> value type
   */
  public static class SchemaTypeDescriptor<T> implements TypeDescriptor<T> {

    @Getter final AttributeValueType type;

    final @Nullable PrimitiveTypeDescriptor<T> primitiveTypeDescriptor;
    final @Nullable ArrayTypeDescriptor<T> arrayTypeDescriptor;
    final @Nullable StructureTypeDescriptor<T> structureTypeDescriptor;

    @SuppressWarnings("squid:S1452")
    final @Nullable EnumTypeDescriptor<?> enumTypeDescriptor;

    @Builder
    public SchemaTypeDescriptor(
        AttributeValueType type,
        @Nullable PrimitiveTypeDescriptor<T> primitiveTypeDescriptor,
        @Nullable ArrayTypeDescriptor<T> arrayTypeDescriptor,
        @Nullable StructureTypeDescriptor<T> structureTypeDescriptor,
        @Nullable EnumTypeDescriptor<?> enumTypeDescriptor) {
      this.type = type;
      this.primitiveTypeDescriptor = primitiveTypeDescriptor;
      this.arrayTypeDescriptor = arrayTypeDescriptor;
      this.structureTypeDescriptor = structureTypeDescriptor;
      this.enumTypeDescriptor = enumTypeDescriptor;
    }

    public PrimitiveTypeDescriptor<T> getPrimitiveTypeDescriptor() {
      Preconditions.checkState(
          isPrimitiveType(), "SchemaTypeDescriptor is not primitive type. Type: " + getType());
      return primitiveTypeDescriptor;
    }

    public ArrayTypeDescriptor<T> getArrayTypeDescriptor() {
      Preconditions.checkState(
          isArrayType(), "SchemaTypeDescriptor is not array type. Type: " + getType());
      return arrayTypeDescriptor;
    }

    public StructureTypeDescriptor<T> getStructureTypeDescriptor() {
      Preconditions.checkState(
          isStructureType(), "SchemaTypeDescriptor is not structure type. Type: " + getType());
      return structureTypeDescriptor;
    }

    @SuppressWarnings("squid:S1452")
    public EnumTypeDescriptor<?> getEnumTypeDescriptor() {
      Preconditions.checkState(
          isEnumType(), "SchemaTypeDescriptor is not enum type. Type: " + getType());
      return enumTypeDescriptor;
    }

    public boolean isStructureType() {
      return type.equals(AttributeValueType.STRUCTURE);
    }

    public boolean isArrayType() {
      return type.equals(AttributeValueType.ARRAY);
    }

    public boolean isEnumType() {
      return type.equals(AttributeValueType.ENUM);
    }

    public boolean isPrimitiveType() {
      return !(isStructureType() || isArrayType() || isEnumType());
    }

    @Override
    public SchemaTypeDescriptor<T> toTypeDescriptor() {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof SchemaTypeDescriptor) {
        @SuppressWarnings("unchecked")
        SchemaTypeDescriptor<?> other = (SchemaTypeDescriptor<?>) o;
        if (!type.equals(other.getType())) {
          return false;
        }
        switch (getType()) {
          case STRUCTURE:
            return structureTypeDescriptor != null
                && structureTypeDescriptor.equals(other.getStructureTypeDescriptor());
          case ARRAY:
            return arrayTypeDescriptor != null
                && arrayTypeDescriptor.equals(other.getArrayTypeDescriptor());
          default:
            return primitiveTypeDescriptor != null
                && primitiveTypeDescriptor.equals(other.getPrimitiveTypeDescriptor());
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  /**
   * Primitive type descriptor with simple type (eq String, Long, Integer, etc).
   *
   * @param <T> value type
   */
  public static class PrimitiveTypeDescriptor<T> extends GenericTypeDescriptor<T> {

    public PrimitiveTypeDescriptor(AttributeValueType type) {
      super(type);
    }

    @Override
    public SchemaTypeDescriptor<T> toTypeDescriptor() {
      return SchemaTypeDescriptor.<T>builder()
          .type(getType())
          .primitiveTypeDescriptor(this)
          .build();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type.name());
    }
  }

  /**
   * Array type descriptor allows to use other descriptor as value.
   *
   * @param <T> value type
   */
  public static class ArrayTypeDescriptor<T> extends GenericTypeDescriptor<T> {

    @Getter final SchemaTypeDescriptor<T> valueDescriptor;

    public ArrayTypeDescriptor(SchemaTypeDescriptor<T> valueDescriptor) {
      super(AttributeValueType.ARRAY);
      this.valueDescriptor = valueDescriptor;
    }

    /**
     * Get value type in Array.
     *
     * @return value type
     */
    public AttributeValueType getValueType() {
      return valueDescriptor.getType();
    }

    @Override
    public SchemaTypeDescriptor<T> toTypeDescriptor() {
      return SchemaTypeDescriptor.<T>builder().type(getType()).arrayTypeDescriptor(this).build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof ArrayTypeDescriptor) {
        return super.equals(o)
            && getValueType().equals(((ArrayTypeDescriptor<?>) o).getValueType());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 + valueDescriptor.hashCode();
    }
  }

  /**
   * Structure type descriptor allows to have fields with type as another descriptor.
   *
   * @param <T> structure type
   */
  public static class StructureTypeDescriptor<T> extends GenericTypeDescriptor<T> {

    @Getter final String name;
    @Getter private Map<String, TypeDescriptor<?>> fields = new HashMap<>();

    public StructureTypeDescriptor(String name, Map<String, SchemaTypeDescriptor<?>> fields) {
      super(AttributeValueType.STRUCTURE);
      this.name = name;
      fields.forEach(this::addField);
    }

    /**
     * Add Field into structure type.
     *
     * @param name field name
     * @param descriptor value descriptor
     * @return this
     */
    public StructureTypeDescriptor<T> addField(String name, TypeDescriptor<?> descriptor) {
      Preconditions.checkArgument(!fields.containsKey(name), "Duplicate field " + name);
      fields.put(name, descriptor);
      return this;
    }

    /**
     * Return {@code true} if field with given name exists in structure.
     *
     * @param name field name
     * @return boolean
     */
    public boolean hasField(String name) {
      return fields.containsKey(name);
    }

    @SuppressWarnings("squid:S1452")
    public SchemaTypeDescriptor<?> getField(String name) {
      return Optional.ofNullable(fields.getOrDefault(name, null))
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "Field " + name + " not found in structure " + getName()))
          .toTypeDescriptor();
    }

    @Override
    public SchemaTypeDescriptor<T> toTypeDescriptor() {
      return SchemaTypeDescriptor.<T>builder()
          .type(getType())
          .structureTypeDescriptor(this)
          .build();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      StructureTypeDescriptor<?> that = (StructureTypeDescriptor<?>) o;
      return name.equals(that.name) && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), name, fields);
    }
  }

  /**
   * Enum type descriptor.
   *
   * @param <T> value type
   */
  public static class EnumTypeDescriptor<T extends Serializable> extends GenericTypeDescriptor<T> {

    @Getter private final List<T> values;

    public EnumTypeDescriptor(List<T> values) {
      super(AttributeValueType.ENUM);
      this.values = values;
    }

    @Override
    public SchemaTypeDescriptor<T> toTypeDescriptor() {
      return SchemaTypeDescriptor.<T>builder().type(getType()).enumTypeDescriptor(this).build();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      EnumTypeDescriptor<?> that = (EnumTypeDescriptor<?>) o;
      return values.equals(that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), values);
    }
  }
}
