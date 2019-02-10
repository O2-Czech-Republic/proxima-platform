/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.scheme.avro;


import cz.o2.proxima.scheme.SerializationException;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.util.Classpath;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Avro serializer factory for manipulate with SpecificRecords
 */
@Slf4j
public class AvroSerializerFactory implements ValueSerializerFactory {
  private final Map<URI, ValueSerializer<?>> serializersCache = new ConcurrentHashMap<>();

  @Override
  public String getAcceptableScheme() {
    return "avro";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
    return (ValueSerializer<T>) serializersCache.computeIfAbsent(
        specifier, this::createSerializer);
  }

  private <M extends SpecificRecord> ValueSerializer<M> createSerializer(URI uri) {
    return new ValueSerializer<M>() {
      final String avroClassName = uri.getSchemeSpecificPart();

      transient M defaultInstance = null;

      transient AvroSerializer<M> avroSerializer = null;

      @Override
      public Optional<M> deserialize(byte[] input) {
        if (input.length == 0) {
          /**
           * This is a little bit weird but proxima try to deserialize empty bytes
           * in serializer validation {@link ValueSerializer#isValid(byte[])}
           */
          return Optional.of(getDefault());
        }
        if (avroSerializer == null) {
          avroSerializer = new AvroSerializer<>(getAvroSchemaForClass(avroClassName));
        }
        try {
          return Optional.of(avroSerializer.deserialize(input));
        } catch (IOException ex) {
          log.warn("Unable to deserialize avro payload", ex);
          return Optional.empty();
        }
      }

      @Override
      public byte[] serialize(M value) {
        if (avroSerializer == null) {
          avroSerializer = new AvroSerializer<>(getAvroSchemaForClass(avroClassName));
        }
        try {
          return avroSerializer.serialize(value);
        } catch (IOException ex) {
          throw new SerializationException("Unable to serialize avro object", ex);
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public M getDefault() {
        if (defaultInstance == null) {
          defaultInstance = Classpath.newInstance(
              (Class<M>) Classpath.findClass(avroClassName, SpecificRecord.class)
          );
        }
        return defaultInstance;
      }
    };
  }
  private Schema getAvroSchemaForClass(String avroClassName) {
    try {
      Class<SpecificRecord> avroClass = Classpath
          .findClass(avroClassName, SpecificRecord.class);
      Method method = avroClass.getMethod("getSchema");
      return (Schema) method.invoke(avroClass.newInstance());
    } catch (IllegalAccessException | IllegalArgumentException
        | NoSuchMethodException | SecurityException | InvocationTargetException
        | InstantiationException ex) {

      throw new IllegalArgumentException(
          "Cannot get schema from class " + avroClassName, ex);
    }

  }
}
