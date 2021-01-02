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
package cz.o2.proxima.scheme.confluent;

import cz.o2.proxima.scheme.SerializationException;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericContainer;

@Slf4j
public class SchemaRegistrySerializerFactory implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  private Map<URI, ValueSerializer<?>> cache = new ConcurrentHashMap<>();

  @Override
  public String getAcceptableScheme() {
    return "schema-registry";
  }

  private static <M extends GenericContainer> ValueSerializer<M> createSerializer(URI scheme) {
    try {
      return new SchemaRegistryValueSerializer<>(scheme);
    } catch (Exception e) {
      throw new SerializationException("Unable to create value serializer.", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI scheme) {
    return (ValueSerializer<T>)
        cache.computeIfAbsent(scheme, SchemaRegistrySerializerFactory::createSerializer);
  }

  @Override
  public String getClassName(URI scheme) {
    @SuppressWarnings("unchecked")
    SchemaRegistryValueSerializer<?> serializer =
        (SchemaRegistryValueSerializer) getValueSerializer(scheme);

    return serializer.getClassName();
  }
}
