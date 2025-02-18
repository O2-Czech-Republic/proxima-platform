/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.scheme;

import com.google.auto.service.AutoService;
import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.net.URI;
import java.util.Optional;

/** This scheme parser always fails the parsing. */
@SuppressWarnings("unchecked")
@AutoService(ValueSerializerFactory.class)
public class AlwaysFailSchemeParser implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public String getAcceptableScheme() {
    return "fail";
  }

  @Override
  public ValueSerializer<byte[]> getValueSerializer(URI scheme) {
    return new ValueSerializer<byte[]>() {

      private static final long serialVersionUID = 1L;

      @Override
      public Optional<byte[]> deserialize(byte[] input) {
        return Optional.empty();
      }

      @Override
      public byte[] getDefault() {
        return null;
      }

      @Override
      public byte[] serialize(byte[] value) {
        return value;
      }

      @Override
      public boolean isUsable() {
        return true;
      }

      @Override
      public SchemaTypeDescriptor<byte[]> getValueSchemaDescriptor() {
        return SchemaDescriptors.bytes();
      }
    };
  }

  @Override
  public String getClassName(URI scheme) {
    return "byte[]";
  }
}
