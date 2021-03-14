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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
   * @return enum type descriptor
   */
  public static EnumTypeDescriptor<String> enums(List<String> values) {
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
   * @param valueDescriptor primitive type
   * @param <T> value type
   * @return Array type descriptor
   */
  public static <T> ArrayTypeDescriptor<T> arrays(SchemaTypeDescriptor<T> valueDescriptor) {
    return new ArrayTypeDescriptor<>(valueDescriptor);
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

  /**
   * Create {@link StructureTypeDescriptor} with fields.
   *
   * @param name structure name
   * @param fields fields
   * @param <T> structure type
   * @return Structure type descriptor
   */
  public static <T> StructureTypeDescriptor<T> structures(
      String name, Map<String, SchemaTypeDescriptor<?>> fields) {
    return new StructureTypeDescriptor<>(name, fields);
  }

  /**
   * Generic type descriptor. Parent class for other types.
   *
   * @param <T> value type
   */
  public interface SchemaTypeDescriptor<T> extends Serializable {

    String TYPE_CHECK_ERROR_MESSAGE_TEMPLATE =
        "Conversion to type %s is not supported. Given type: %s";

    /**
     * Return attribute value type
     *
     * @return type
     */
    AttributeValueType getType();

    /**
     * Return {@code true} if type is primitive.
     *
     * @return boolean
     */
    default boolean isPrimitiveType() {
      return !(isArrayType() || isStructureType() || isEnumType());
    }

    /**
     * Return current type descriptor as {@link PrimitiveTypeDescriptor}
     *
     * @return primitive type descriptor
     */
    default PrimitiveTypeDescriptor<T> asPrimitiveTypeDescriptor() {
      throw new UnsupportedOperationException(
          String.format(
              TYPE_CHECK_ERROR_MESSAGE_TEMPLATE,
              PrimitiveTypeDescriptor.class.getSimpleName(),
              getType()));
    }

    /**
     * Return {@code true} if type is an Array
     *
     * @return boolean
     */
    default boolean isArrayType() {
      return getType().equals(AttributeValueType.ARRAY);
    }

    /**
     * Return current type descriptor as {@link ArrayTypeDescriptor}.
     *
     * @return array type descriptor
     */
    default ArrayTypeDescriptor<T> asArrayTypeDescriptor() {
      throw new UnsupportedOperationException(
          String.format(TYPE_CHECK_ERROR_MESSAGE_TEMPLATE, AttributeValueType.ARRAY, getType()));
    }

    /**
     * Return {@code true} if type is an structure
     *
     * @return boolean
     */
    default boolean isStructureType() {
      return getType().equals(AttributeValueType.STRUCTURE);
    }

    /**
     * Return current type descriptor as {@link StructureTypeDescriptor}.
     *
     * @return Structure type descriptor
     */
    default StructureTypeDescriptor<T> asStructureTypeDescriptor() {
      throw new UnsupportedOperationException(
          String.format(
              TYPE_CHECK_ERROR_MESSAGE_TEMPLATE, AttributeValueType.STRUCTURE, getType()));
    }

    /**
     * Return {@code True} if type is an Enum
     *
     * @return boolean
     */
    default boolean isEnumType() {
      return getType().equals(AttributeValueType.ENUM);
    }

    /**
     * Return current type as {@link EnumTypeDescriptor}.
     *
     * @return enum type descriptor
     */
    default EnumTypeDescriptor<T> asEnumTypeDescriptor() {
      throw new UnsupportedOperationException(
          String.format(TYPE_CHECK_ERROR_MESSAGE_TEMPLATE, AttributeValueType.ENUM, getType()));
    }
  }

  /**
   * Primitive type descriptor with simple type (eq String, Long, Integer, etc).
   *
   * @param <T> value type
   */
  public static class PrimitiveTypeDescriptor<T> implements SchemaTypeDescriptor<T> {

    @Getter private final AttributeValueType type;

    public PrimitiveTypeDescriptor(AttributeValueType type) {
      this.type = type;
    }

    @Override
    public boolean isPrimitiveType() {
      return true;
    }

    @Override
    public PrimitiveTypeDescriptor<T> asPrimitiveTypeDescriptor() {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PrimitiveTypeDescriptor<?> that = (PrimitiveTypeDescriptor<?>) o;
      return type == that.type;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type.name());
    }

    @Override
    public String toString() {
      return getType().name();
    }
  }

  /**
   * Array type descriptor allows to use other descriptor as value.
   *
   * @param <T> value type
   */
  public static class ArrayTypeDescriptor<T> implements SchemaTypeDescriptor<T> {

    @Getter final AttributeValueType type;
    @Getter final SchemaTypeDescriptor<T> valueDescriptor;

    public ArrayTypeDescriptor(SchemaTypeDescriptor<T> valueDescriptor) {
      this.type = AttributeValueType.ARRAY;
      this.valueDescriptor = valueDescriptor;
    }

    /**
     * Get {@link AttributeValueType} for array value.
     *
     * @return value type
     */
    public AttributeValueType getValueType() {
      return valueDescriptor.getType();
    }

    @Override
    public boolean isPrimitiveType() {
      return !valueDescriptor.isArrayType() && valueDescriptor.isPrimitiveType();
    }

    @Override
    public PrimitiveTypeDescriptor<T> asPrimitiveTypeDescriptor() {
      return valueDescriptor.asPrimitiveTypeDescriptor();
    }

    @Override
    public ArrayTypeDescriptor<T> asArrayTypeDescriptor() {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ArrayTypeDescriptor<?> that = (ArrayTypeDescriptor<?>) o;
      return type == that.type && valueDescriptor.equals(that.valueDescriptor);
    }

    @Override
    public int hashCode() {
      return 31 + valueDescriptor.hashCode();
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getType().name(), getValueType().toString());
    }
  }

  /**
   * Structure type descriptor allows to have fields with type as another descriptor.
   *
   * @param <T> structure type
   */
  public static class StructureTypeDescriptor<T> implements SchemaTypeDescriptor<T> {

    @Getter private final AttributeValueType type;
    @Getter private final String name;
    private final Map<String, SchemaTypeDescriptor<?>> fields = new HashMap<>();

    public StructureTypeDescriptor(String name, Map<String, SchemaTypeDescriptor<?>> fields) {
      this.type = AttributeValueType.STRUCTURE;
      this.name = name;
      fields.forEach(this::addField);
    }

    /**
     * Get fields in structure type.
     *
     * @return map of field and type descriptor.
     */
    @SuppressWarnings("squid:S1452")
    public Map<String, SchemaTypeDescriptor<?>> getFields() {
      return Collections.unmodifiableMap(fields);
    }

    @Override
    public StructureTypeDescriptor<T> asStructureTypeDescriptor() {
      return this;
    }

    /**
     * Add Field into structure type.
     *
     * @param name field name
     * @param descriptor value descriptor
     * @return this
     */
    public StructureTypeDescriptor<T> addField(String name, SchemaTypeDescriptor<?> descriptor) {
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

    /**
     * Get field descriptor for given field name.
     *
     * @param name field name
     * @return field type descriptor
     */
    @SuppressWarnings("squid:S1452")
    public SchemaTypeDescriptor<?> getField(String name) {
      return Optional.ofNullable(fields.getOrDefault(name, null))
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "Field " + name + " not found in structure " + getName()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof StructureTypeDescriptor) {
        StructureTypeDescriptor<?> that = (StructureTypeDescriptor<?>) o;
        return name.equals(that.name) && fields.equals(that.fields);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), name, fields);
    }

    @Override
    public String toString() {
      StringBuilder builder =
          new StringBuilder(String.format("%s %s", getType().name(), getName()));
      getFields()
          .forEach(
              (field, fType) ->
                  builder.append(
                      String.format("%n\t%s: %s", field, fType.toString().replace("\t", "\t\t"))));
      return builder.toString();
    }
  }

  /** Enum type descriptor. */
  public static class EnumTypeDescriptor<T> implements SchemaTypeDescriptor<T> {

    @Getter private final AttributeValueType type;
    private final List<String> values;

    public EnumTypeDescriptor(List<String> values) {
      this.type = AttributeValueType.ENUM;
      this.values = values;
    }

    @Override
    public EnumTypeDescriptor<T> asEnumTypeDescriptor() {
      return this;
    }

    public List<String> getValues() {
      return Collections.unmodifiableList(values);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EnumTypeDescriptor<?> that = (EnumTypeDescriptor<?>) o;
      return type == that.type && values.equals(that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getType(), values);
    }

    @Override
    public String toString() {
      return getType().name() + getValues();
    }
  }
}
