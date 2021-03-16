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
package cz.o2.proxima.scheme.proto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.scheme.AttributeValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link StructureValueAccessor} for ProtoBufs
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
        getDefaultValue()
            .getDescriptorForType()
            .getFields()
            .stream()
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
    return StructureValue.of(
        object
            .getAllFields()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().getName(),
                    entry -> {
                      final FieldDescriptor field = entry.getKey();
                      @SuppressWarnings("unchecked")
                      final AttributeValueAccessor<Object, Object> accessor =
                          (AttributeValueAccessor<Object, Object>)
                              fieldAccessors.get(field.getName());
                      if (field.getJavaType().equals(JavaType.MESSAGE) && !field.isRepeated()) {
                        StructureValue v = (StructureValue) accessor.valueOf(entry.getValue());
                        return v.value();
                      }
                      return accessor.valueOf(entry.getValue());
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
        final Object value = prepareFieldValue(fieldDescriptor, input.get(fieldName));
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
  private Object prepareFieldValue(FieldDescriptor descriptor, Object object) {
    if (descriptor.isRepeated()) {
      if (List.class.isAssignableFrom(object.getClass())) {
        return ((List<Object>) object)
            .stream()
            .map(v -> mapValue(descriptor, v))
            .collect(Collectors.toList());
      } else {
        // Create new list in case when originally scalar field changed to repeated
        return Collections.singletonList(mapValue(descriptor, object));
      }
    }
    return mapValue(descriptor, object);
  }

  @SuppressWarnings("unchecked")
  private Object mapValue(FieldDescriptor descriptor, Object value) {
    if (descriptor.getJavaType().equals(JavaType.MESSAGE)) {
      return StructureValue.of((Map<String, Object>) value);
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private <InputT, OutputT> AttributeValueAccessor<InputT, OutputT> createFieldAccessor(
      FieldDescriptor fieldDescriptor, @Nullable Builder fieldBuilder) {
    AttributeValueAccessor<InputT, OutputT> accessor;
    switch (fieldDescriptor.getJavaType()) {
      case MESSAGE:
        Preconditions.checkArgument(
            fieldBuilder != null,
            "Field builder must be specified for type %s.",
            fieldDescriptor.getJavaType());
        accessor =
            (AttributeValueAccessor<InputT, OutputT>)
                new ProtoMessageValueAccessor<>(fieldBuilder::getDefaultInstanceForType);
        break;
      case ENUM:
        accessor =
            (AttributeValueAccessor<InputT, OutputT>)
                PrimitiveValueAccessor.of(
                    EnumValueDescriptor::getName,
                    name -> fieldDescriptor.getEnumType().findValueByName(name));
        break;
      case BYTE_STRING:
        accessor =
            (AttributeValueAccessor<InputT, OutputT>)
                PrimitiveValueAccessor.of(ByteString::toByteArray, ByteString::copyFrom);
        break;
      default:
        accessor =
            (AttributeValueAccessor<InputT, OutputT>)
                PrimitiveValueAccessor.of(UnaryFunction.identity(), UnaryFunction.identity());
    }
    if (!fieldDescriptor.isRepeated()) {
      return accessor;
    } else {
      return (AttributeValueAccessor<InputT, OutputT>) ArrayValueAccessor.of(accessor);
    }
  }

  private T getDefaultValue() {
    if (defaultValue == null) {
      defaultValue = defaultValueFactory.apply();
    }
    return defaultValue;
  }
}
