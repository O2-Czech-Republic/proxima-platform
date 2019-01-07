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
package cz.o2.proxima.repository;

import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.scheme.ValueSerializerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Repository of all entities configured in the system.
 */
@Evolving
public interface Repository {

  static Repository of(Config config) {
    return ConfigRepository.of(config);
  }

  /**
   * Find entity descriptor based on entity name.
   *
   * @param name name of the entity to search for
   * @return optional {@link EntityDescriptor} found by name
   */
  Optional<EntityDescriptor> findEntity(String name);

  /**
   * Retrieve stream of all entities.
   *
   * @return {@link Stream} of all entities specified in this repository
   */
  Stream<EntityDescriptor> getAllEntities();

  /**
   * Retrieve all transformers.
   *
   * @return all transformations by name
   */
  Map<String, TransformationDescriptor> getTransformations();

  /**
   * Check if this repository is empty.
   *
   * @return {@code true} if this repository is empty
   */
  boolean isEmpty();

  /**
   * List all unique attribute families.
   *
   * @return all families specified in this repository
   */
  Stream<AttributeFamilyDescriptor> getAllFamilies();

  /**
   * Retrieve list of attribute families for attribute.
   *
   * @param attr attribute descriptor
   * @return all families of given attribute
   */
  Set<AttributeFamilyDescriptor> getFamiliesForAttribute(AttributeDescriptor<?> attr);

  /**
   * Retrieve value serializer for given scheme.
   *
   * @param scheme scheme of the {@link cz.o2.proxima.scheme.ValueSerializerFactory}
   * @return {@link ValueSerializerFactory} for the scheme
   */
  @Nullable
  ValueSerializerFactory getValueSerializerFactory(String scheme);

  /**
   * Retrieve {@link DataOperator} representation for this {@link Repository}.
   *
   * @param <T> type of the operator
   * @param type the operator class
   *
   * @return the data operator of given type
   */
  @SuppressWarnings("unchecked")
  default <T extends DataOperator> T asDataOperator(Class<T> type) {

    ServiceLoader<DataOperatorFactory> loaders = ServiceLoader.load(
        DataOperatorFactory.class);

    return Streams
        .stream(loaders)
        .filter(factory ->  factory.isOfType(type))
        .findAny()
        .map(o -> (DataOperatorFactory<T>) o)
        .map(f -> f.create(this))
        .orElseThrow(() -> new IllegalStateException(
            "Operator " + type + " not found."));
  }

  /**
   * Check if given implementation of data operator is available on classpath
   * and {@link #asDataOperator(java.lang.Class)} will return non-null object
   * for class corresponding the given name.
   * @param name name of the operator
   * @return {@code true} if the operator is available, {@code false} otherwise
   */
  default boolean hasOperator(String name) {
    ServiceLoader<DataOperatorFactory> loaders = ServiceLoader.load(
        DataOperatorFactory.class);
    return Streams.stream(loaders)
        .filter(f -> f.getOperatorName().equals(name))
        .findAny()
        .isPresent();
  }


}
