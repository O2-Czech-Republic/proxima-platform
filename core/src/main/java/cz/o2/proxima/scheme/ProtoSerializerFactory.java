/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Parser;
import cz.o2.proxima.util.Classpath;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializer from protobuffers.
 */
public class ProtoSerializerFactory<M extends AbstractMessage>
    implements ValueSerializerFactory<M> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoSerializerFactory.class);

  private final Map<URI, ValueSerializer<M>> parsers = new HashMap<>();

  @Override
  public String getAcceptableScheme() {
    return "proto";
  }

  @SuppressWarnings("unchecked")
  private ValueSerializer<M> createSerializer(URI uri) {
    String protoClass = uri.getSchemeSpecificPart();
    M defVal = getDefaultInstance(protoClass);
    return new ValueSerializer<M>() {

      transient Parser<?> parser = getParserForClass(protoClass);

      @Override
      public Optional<M> deserialize(byte[] input) {
        if (parser == null) {
          parser = getParserForClass(protoClass);
        }
        try {
          return Optional.of((M) parser.parseFrom(input));
        } catch (Exception ex) {
          LOG.debug("Failed to parse input bytes", ex);
        }
        return Optional.empty();
      }

      @Override
      public M getDefault() {
        return defVal;
      }

      @Override
      public byte[] serialize(M value) {
        return value.toByteArray();
      }

    };
  }

  @SuppressWarnings("unchecked")
  private Parser<?> getParserForClass(String protoClassName) {

    try {
      Class<?> protoClass = Classpath.findClass(protoClassName, GeneratedMessage.class);
      Method parser = protoClass.getMethod("parser");
      return (Parser) parser.invoke(null);
    } catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException
        | NoSuchMethodException | SecurityException | InvocationTargetException ex) {

      throw new IllegalArgumentException(
          "Cannot create parser from class " + protoClassName, ex);
    }
  }

  // this method is synchronized because of the cache
  @Override
  public synchronized ValueSerializer<M> getValueSerializer(URI scheme) {
    return parsers.computeIfAbsent(scheme, this::createSerializer);
  }

  @SuppressWarnings("unchecked")
  private M getDefaultInstance(String protoClass) {
    try {
      Class<GeneratedMessage> cls = Classpath.findClass(
          protoClass, GeneratedMessage.class);
      Method method = cls.getMethod("getDefaultInstance");
      return (M) method.invoke(null);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException
        | IllegalAccessException | InvocationTargetException ex) {
      throw new IllegalArgumentException(
          "Cannot retrieve default instance for type "
          + protoClass);
    }
  }

}
