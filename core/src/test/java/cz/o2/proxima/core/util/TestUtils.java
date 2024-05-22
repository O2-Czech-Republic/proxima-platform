/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.util;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor.Builder;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AccessType;
import cz.o2.proxima.core.storage.StorageType;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestUtils {

  private TestUtils() {}

  /**
   * Check if object is serializable and return deserialized.
   *
   * @param <T> type parameter
   * @param object Object to check
   * @return object Deserialized object
   * @throws IOException on IO errors
   * @throws ClassNotFoundException on class path errors
   */
  public static <T> T assertSerializable(T object) throws IOException, ClassNotFoundException {
    T deserialized = deserializeObject(serializeObject(object));
    Preconditions.checkState(
        deserialized.equals(object),
        "Deserialized object of class '%s' should be equals to input.",
        object.getClass().getName());
    return deserialized;
  }

  /**
   * Serialize object into bytes
   *
   * @param object object to serialize
   * @return byte[]
   * @throws IOException on IO errors
   */
  public static byte[] serializeObject(Object object) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
      oos.writeObject(object);
    }
    Preconditions.checkState(
        buffer.toByteArray().length > 0,
        "Class '%s' isn't serializable.",
        object.getClass().getName());
    return buffer.toByteArray();
  }

  /**
   * Deserialize object from bytes
   *
   * @param <T> type parameter
   * @param bytes bytes to deserialize
   * @return the deserialized object
   * @throws IOException on IO errors
   * @throws ClassNotFoundException on class path errors
   */
  @SuppressWarnings("unchecked")
  public static <T> T deserializeObject(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (T) ois.readObject();
    }
  }

  /**
   * Assert hashCode and equals of 2 objects.
   *
   * @param first first object to compare
   * @param second second object to compare
   */
  public static void assertHashCodeAndEquals(Object first, Object second) {
    Preconditions.checkState(first.equals(second));
    Preconditions.checkState(first.hashCode() == second.hashCode(), "Hashcode should be same.");
    Preconditions.checkState(!first.equals(new Object()));
  }

  /**
   * Create {@link AttributeFamilyDescriptor} for test purpose.
   *
   * @param entity entity
   * @param uri storage URI
   * @return attribute family descriptor
   */
  public static AttributeFamilyDescriptor createTestFamily(EntityDescriptor entity, URI uri) {
    return createTestFamily(entity, uri, Collections.emptyList(), Collections.emptyMap());
  }

  /**
   * Create {@link AttributeFamilyDescriptor} for test purpose.
   *
   * @param entity entity
   * @param uri storage URI
   * @param cfg configuration
   * @return attribute family descriptor
   */
  public static AttributeFamilyDescriptor createTestFamily(
      EntityDescriptor entity, URI uri, Map<String, Object> cfg) {
    return createTestFamily(entity, uri, Collections.emptyList(), cfg);
  }

  /**
   * Create {@link AttributeFamilyDescriptor} for test purpose.
   *
   * @param entity entity
   * @param uri storage URI
   * @param attr attributes
   * @param cfg configuration
   * @return attribute family descriptor
   */
  public static AttributeFamilyDescriptor createTestFamily(
      EntityDescriptor entity,
      URI uri,
      List<AttributeDescriptor<?>> attr,
      Map<String, Object> cfg) {
    Builder builder =
        AttributeFamilyDescriptor.newBuilder()
            .setName("test-family")
            .setEntity(entity)
            .setStorageUri(uri)
            .setType(StorageType.PRIMARY)
            .setAccess(AccessType.from("read-only"))
            .setCfg(cfg);
    attr.forEach(builder::addAttribute);
    return builder.build();
  }
}
