package cz.o2.proxima.scheme;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain java object serializer. Please not it is not intended for production use.
 */
public class JavaValueSerializerFactory implements ValueSerializerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(JavaValueSerializerFactory.class);

  @Override
  public String getAcceptableScheme() {
    return "java";
  }

  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI uri) {

    final Class<?> clazz;
    try {
      clazz = Class.forName(uri.getSchemeSpecificPart());
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Unable to create serializer.", e);
    }

    if (Serializable.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Class '" + clazz.getName() + "' is not serializable.");
    }

    return new ValueSerializer<T>() {

      @SuppressWarnings("unchecked")
      @Override
      public Optional<T> deserialize(byte[] input) {

        try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(input))) {
          return Optional.of((T) ois.readObject());
        } catch (IOException | ClassNotFoundException e) {
          LOG.warn("Unable to deserialize value of '" + uri + "'.");
          return Optional.empty();
        }
      }

      @Override
      public byte[] serialize(T value) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
             final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
          oos.writeObject(value);
          return baos.toByteArray();
        } catch (IOException e) {
          throw new IllegalStateException("Unable to serialize value of '" + uri + "'.");
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public T getDefault() {
        try {
          return (T) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new IllegalStateException("Unable to create default value of '" + uri + "'.");
        }
      }
    };
  }
}
