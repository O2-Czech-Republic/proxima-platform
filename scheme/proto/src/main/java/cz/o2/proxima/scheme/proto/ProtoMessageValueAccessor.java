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
package cz.o2.proxima.scheme.proto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.scheme.AttributeValueAccessor;
import cz.o2.proxima.core.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.core.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.core.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.core.scheme.AttributeValueAccessors.StructureValueAccessor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link StructureValueAccessor} for ProtoBufs
 *
 * <p>Wrapper types are mapped to scalar equivalents as we can manage null values. See {@link
 * ProtoMessageValueAccessor#createFieldAccessor(FieldDescriptor, Builder)} for details.
 *
 * @param <T> Protobuf message type
 */
@Slf4j
public class ProtoMessageValueAccessor<T extends Message> implements StructureValueAccessor<T> {

  private final Factory<T> defaultValueFactory;
  private final Map<String, AttributeValueAccessor<?, ?>> fieldAccessors;
  @Nullable private transient T defaultValue;

  public ProtoMessageValueAccessor(Factory<T> defaultValueFactory) {
    this.defaultValueFactory = defaultValueFactory;
    Builder builder = getDefaultValue().toBuilder();
    this.fieldAccessors =
        getDefaultValue().getDescriptorForType().getFields().stream()
            .collect(
                Collectors.toMap(
                    FieldDescriptor::getName,
                    field -> {
                      Builder fieldBuilder = null;
                      if (field.getJavaType().equals(JavaType.MESSAGE)) {
                        fieldBuilder = builder.newBuilderForField(field);
                      }
                      return createFieldAccessor(field, fieldBuilder);
                    }));
  }

  @Override
  public StructureValue valueOf(T object) {
    /*
     Here we get all fields from descriptor which also returns empty fields (not set), those
     fields needs to be filtered -> see filter on stream, where we pass all non-message and
     non-repeated fields, repeated fields just when count > 0 and messages just when was set.
    */
    return StructureValue.of(
        object.getDescriptorForType().getFields().stream()
            .filter(
                f ->
                    (!f.isRepeated()
                            && (!f.getJavaType().equals(JavaType.MESSAGE) || object.hasField(f)))
                        || (f.isRepeated() && object.getRepeatedFieldCount(f) > 0))
            .collect(
                Collectors.toMap(
                    FieldDescriptor::getName,
                    field -> {
                      @SuppressWarnings("unchecked")
                      final AttributeValueAccessor<Object, Object> accessor =
                          (AttributeValueAccessor<Object, Object>)
                              fieldAccessors.get(field.getName());
                      final Object value = object.getField(field);
                      if (field.getJavaType().equals(JavaType.MESSAGE)
                          && !field.isRepeated()
                          && accessor.getType().equals(Type.STRUCTURE)) {
                        StructureValue v = (StructureValue) accessor.valueOf(value);
                        return v.value();
                      }
                      return accessor.valueOf(value);
                    })));
  }

  @Override
  @SuppressWarnings("unchecked")
  public T createFrom(StructureValue input) {
    final Builder builder = getDefaultValue().newBuilderForType();

    for (FieldDescriptor fieldDescriptor : getDefaultValue().getDescriptorForType().getFields()) {
      final String fieldName = fieldDescriptor.getName();
      if (input.containsKey(fieldName)) {
        final AttributeValueAccessor<Object, Object> fieldAccessor =
            (AttributeValueAccessor<Object, Object>) fieldAccessors.get(fieldName);
        Preconditions.checkNotNull(
            fieldAccessor, "Unable to get accessor for field %s.", fieldName);
        final Object value = prepareFieldValue(fieldAccessor, input.get(fieldName));
        if (fieldDescriptor.isRepeated()) {
          final List<Object> listValues = (List<Object>) fieldAccessor.createFrom(value);
          listValues.forEach(v -> builder.addRepeatedField(fieldDescriptor, v));
        } else {
          if (List.class.isAssignableFrom(value.getClass())) {
            builder.setField(
                fieldDescriptor, fieldAccessor.createFrom(Iterables.getLast((List<?>) value)));
          } else {
            builder.setField(fieldDescriptor, fieldAccessor.createFrom(value));
          }
        }
      }
    }

    return (T) builder.build();
  }

  @SuppressWarnings("unchecked")
  private Object prepareFieldValue(AttributeValueAccessor<?, ?> accessor, Object object) {
    if (accessor.getType().equals(Type.ARRAY)) {
      if (List.class.isAssignableFrom(object.getClass())) {
        return ((List<Object>) object)
            .stream().map(v -> mapValue(accessor, v)).collect(Collectors.toList());
      } else {
        // Create new list in case when originally scalar field changed to repeated
        return Collections.singletonList(mapValue(accessor, object));
      }
    }
    return mapValue(accessor, object);
  }

  @SuppressWarnings("unchecked")
  private Object mapValue(AttributeValueAccessor<?, ?> accessor, Object value) {
    if (accessor.getType().equals(Type.STRUCTURE)) {
      return StructureValue.of((Map<String, Object>) value);
    }
    return value;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <FInputT, FOutputT> AttributeValueAccessor<FInputT, FOutputT> createFieldAccessor(
      FieldDescriptor fieldDescriptor, @Nullable Builder fieldBuilder) {
    AttributeValueAccessor<FInputT, FOutputT> accessor;
    switch (fieldDescriptor.getJavaType()) {
      case MESSAGE:
        Preconditions.checkArgument(
            fieldBuilder != null,
            "Field builder must be specified for type %s.",
            fieldDescriptor.getJavaType());
        accessor =
            createFieldAccessorForWrappers(fieldDescriptor, fieldBuilder)
                .orElse(new ProtoMessageValueAccessor(fieldBuilder::getDefaultInstanceForType));
        break;
      case ENUM:
        accessor =
            (AttributeValueAccessor<FInputT, FOutputT>)
                PrimitiveValueAccessor.of(
                    EnumValueDescriptor::getName,
                    name -> fieldDescriptor.getEnumType().findValueByName(name));
        break;
      case BYTE_STRING:
        accessor =
            (AttributeValueAccessor<FInputT, FOutputT>)
                PrimitiveValueAccessor.of(ByteString::toByteArray, ByteString::copyFrom);
        break;
      default:
        accessor =
            (AttributeValueAccessor<FInputT, FOutputT>)
                PrimitiveValueAccessor.of(UnaryFunction.identity(), UnaryFunction.identity());
    }
    if (!fieldDescriptor.isRepeated()) {
      return accessor;
    } else {
      return (AttributeValueAccessor<FInputT, FOutputT>) ArrayValueAccessor.of(accessor);
    }
  }

  private T getDefaultValue() {
    if (defaultValue == null) {
      defaultValue = defaultValueFactory.apply();
    }
    return defaultValue;
  }

  @SuppressWarnings("unchecked")
  private <InputT, OutputT>
      Optional<AttributeValueAccessor<InputT, OutputT>> createFieldAccessorForWrappers(
          FieldDescriptor descriptor, Builder fieldBuilder) {
    switch (descriptor.getMessageType().getFullName()) {
      case "google.protobuf.BoolValue":
      case "google.protobuf.DoubleValue":
      case "google.protobuf.FloatValue":
      case "google.protobuf.Int32Value":
      case "google.protobuf.Int64Value":
      case "google.protobuf.StringValue":
      case "google.protobuf.UInt32Value":
      case "google.protobuf.UInt64Value":
        return Optional.of(
            (AttributeValueAccessor<InputT, OutputT>)
                new PrimitiveWrappersAccessor<>(() -> fieldBuilder));
      case "google.protobuf.BytesValue":
        return Optional.of(
            (AttributeValueAccessor<InputT, OutputT>)
                new PrimitiveValueAccessor<BytesValue, byte[]>() {
                  @Override
                  public byte[] valueOf(BytesValue object) {
                    return object.getValue().toByteArray();
                  }

                  @Override
                  public BytesValue createFrom(byte[] object) {
                    return BytesValue.newBuilder().setValue(ByteString.copyFrom(object)).build();
                  }
                });
      default:
        return Optional.empty();
    }
  }

  private static class PrimitiveWrappersAccessor<I extends Message, O>
      implements PrimitiveValueAccessor<I, O> {

    private final Factory<Builder> builderFactory;
    @Nullable private transient FieldDescriptor fieldDescriptor;

    public PrimitiveWrappersAccessor(Factory<Builder> builderFactory) {
      this.builderFactory = builderFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public O valueOf(I object) {
      return (O) object.getField(getFieldDescriptor());
    }

    @SuppressWarnings("unchecked")
    @Override
    public I createFrom(O object) {
      return (I) builderFactory.apply().setField(getFieldDescriptor(), object).build();
    }

    private FieldDescriptor getFieldDescriptor() {
      if (fieldDescriptor == null) {
        fieldDescriptor = builderFactory.apply().getDescriptorForType().findFieldByName("value");
        Preconditions.checkNotNull(fieldDescriptor, "Missing field with name value.");
      }
      return fieldDescriptor;
    }
  }
}
