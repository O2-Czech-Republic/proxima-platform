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
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoUtils {

  private static Map<String, SchemaTypeDescriptor<?>> structCache = new ConcurrentHashMap<>();

  private ProtoUtils() {
    // no-op
  }

  public static <T> SchemaTypeDescriptor<T> convertProtoToSchema(Descriptor proto) {
    StructureTypeDescriptor<T> schema = SchemaDescriptors.structures(proto.getName());
    proto.getFields().forEach(f -> schema.addField(f.getName(), convertField(f)));
    return schema.toTypeDescriptor();
  }

  @SuppressWarnings("unchecked")
  protected static <T> SchemaTypeDescriptor<T> convertField(FieldDescriptor proto) {
    SchemaTypeDescriptor<T> descriptor;

    switch (proto.getJavaType()) {
      case STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.strings().toTypeDescriptor();
        break;
      case BOOLEAN:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.booleans().toTypeDescriptor();
        break;
      case BYTE_STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.bytes().toTypeDescriptor();
        break;
      case FLOAT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.floats().toTypeDescriptor();
        break;
      case DOUBLE:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.doubles().toTypeDescriptor();
        break;
      case LONG:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.longs().toTypeDescriptor();
        break;
      case INT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.integers().toTypeDescriptor();
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
                            .collect(Collectors.toList()))
                    .toTypeDescriptor();
        break;
      case MESSAGE:
        final String messageTypeName = proto.getMessageType().toProto().getName();
        structCache.computeIfAbsent(
            messageTypeName, name -> convertProtoToSchema(proto.getMessageType()));
        descriptor = (SchemaTypeDescriptor<T>) structCache.get(messageTypeName).toTypeDescriptor();

        break;
      default:
        throw new IllegalStateException(
            "Unable to convert type " + proto.getJavaType() + " to any proxima type");
    }

    if (proto.isRepeated()) {
      return SchemaDescriptors.arrays(descriptor).toTypeDescriptor();
    } else {
      return descriptor;
    }
  }
}
