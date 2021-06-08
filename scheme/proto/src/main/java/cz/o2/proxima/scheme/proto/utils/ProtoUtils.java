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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoUtils {

  private ProtoUtils() {
    // no-op
  }

  /**
   * Covert proto object to proxima schema.
   *
   * @param proto proto message descriptor
   * @param <T> message type
   * @return structure type descriptor
   */
  public static <T extends Message> StructureTypeDescriptor<T> convertProtoToSchema(
      Descriptor proto) {
    return convertProtoMessage(proto);
  }

  static <T extends Message> StructureTypeDescriptor<T> convertProtoMessage(Descriptor proto) {
    final Map<String, SchemaTypeDescriptor<?>> fields =
        proto
            .getFields()
            .stream()
            .collect(
                Collectors.toMap(
                    FieldDescriptor::getName,
                    field -> {
                      if (field.getJavaType().equals(JavaType.MESSAGE)
                          && field.getMessageType().equals(proto)) {
                        throw new UnsupportedOperationException(
                            "Recursion in field [" + field.getName() + "] is not supported");
                      }
                      return convertField(field);
                    }));

    return SchemaDescriptors.structures(proto.getName(), fields);
  }

  /**
   * Convert field of proto message to type descriptor
   *
   * @param proto field proto descriptor
   * @param <T> field type
   * @return schema type descriptor
   */
  @SuppressWarnings("unchecked")
  static <T> SchemaTypeDescriptor<T> convertField(FieldDescriptor proto) {
    SchemaTypeDescriptor<T> descriptor;
    switch (proto.getJavaType()) {
      case STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.strings();
        break;
      case BOOLEAN:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.booleans();
        break;
      case BYTE_STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.bytes();
        break;
      case FLOAT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.floats();
        break;
      case DOUBLE:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.doubles();
        break;
      case LONG:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.longs();
        break;
      case INT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.integers();
        break;
      case ENUM:
        descriptor =
            (SchemaTypeDescriptor<T>)
                SchemaDescriptors.enums(
                    proto
                        .getEnumType()
                        .getValues()
                        .stream()
                        .map(EnumValueDescriptor::getName)
                        .collect(Collectors.toList()));
        break;
      case MESSAGE:
        descriptor = (SchemaTypeDescriptor<T>) convertProtoMessage(proto.getMessageType());

        break;
      default:
        throw new IllegalStateException(
            "Unable to convert type " + proto.getJavaType() + " to any proxima type");
    }

    if (proto.isRepeated()) {
      return SchemaDescriptors.arrays(descriptor);
    } else {
      return descriptor;
    }
  }
}
