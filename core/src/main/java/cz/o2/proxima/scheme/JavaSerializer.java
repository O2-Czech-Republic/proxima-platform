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

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.transaction.Commit;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.transaction.TransactionSerializerSchemeProvider;
import cz.o2.proxima.util.Classpath;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Value serializer for java classes.
 *
 * <p>Class must implements ${@link Serializable} interface. Examples: scheme:
 * "java:cz.o2.proxima.model.Event" scheme: "java:String"
 */
@Slf4j
@Stable
public class JavaSerializer implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  private final Map<URI, ValueSerializer<?>> cache = new ConcurrentHashMap<>();
  private static final List<String> javaPackages = Collections.singletonList("java.lang");

  @Override
  public String getAcceptableScheme() {
    return "java";
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
    return (ValueSerializer<T>) cache.computeIfAbsent(specifier, JavaSerializer::createSerializer);
  }

  @Override
  public boolean canProvideTransactionSerializer() {
    return true;
  }

  @Override
  public TransactionSerializerSchemeProvider createTransactionSerializerSchemeProvider() {
    String scheme = "java:";
    return TransactionSerializerSchemeProvider.of(
        scheme + Request.class.getName(),
        scheme + Response.class.getName(),
        scheme + State.class.getName(),
        scheme + Commit.class.getName());
  }

  private static <T> ValueSerializer<T> createSerializer(URI scheme) {
    log.warn(
        "Using JavaSerializer for URI {}. This can result in low performance, "
            + "please use with caution.",
        scheme);
    return new JavaValueSerializer<>(scheme);
  }

  /** Serializer implementation */
  private static final class JavaValueSerializer<T> implements ValueSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final URI scheme;
    private final String className;
    transient T defaultValue = null;

    JavaValueSerializer(URI scheme) {
      this.scheme = scheme;
      this.className = scheme.getSchemeSpecificPart();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<T> deserialize(byte[] input) {
      if (input.length == 0) {
        return Optional.of(getDefault());
      }
      try (final ObjectInputStream inputStream =
          new ObjectInputStream(new ByteArrayInputStream(input))) {

        return Optional.of((T) inputStream.readObject());
      } catch (IOException | ClassNotFoundException e) {
        log.warn("Unable to deserialize value for scheme: {}.", scheme, e);
      }
      return Optional.empty();
    }

    @Override
    public byte[] serialize(T value) {
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          final ObjectOutputStream oos = new ObjectOutputStream(baos)) {

        oos.writeObject(value);
        return baos.toByteArray();

      } catch (IOException e) {
        throw new SerializationException(
            "Unable to serialize value for scheme: " + scheme + ".", e);
      }
    }

    @Override
    public T getDefault() {
      if (defaultValue == null) {
        Class<T> clazz = findClass();
        if (clazz == null) {
          throw new SerializationException("Unable to find class for scheme: " + scheme + ".");
        }
        defaultValue = Classpath.newInstance(clazz);
      }
      return defaultValue;
    }

    @SuppressWarnings("unchecked")
    private Class<T> findClass() {
      Class<T> clazz = null;
      try {
        clazz = (Class<T>) Classpath.findClass(className, Serializable.class);
      } catch (RuntimeException e) {
        // This is a little bit dirty - noop
      }
      if (clazz == null) {
        for (String p : javaPackages) {
          try {
            clazz = (Class<T>) Classpath.findClass(p + "." + className, Serializable.class);
            if (clazz != null) {
              break;
            }
          } catch (RuntimeException e) {
            // This is a little bit dirty - noop
          }
        }
      }
      return clazz;
    }
  }
}
