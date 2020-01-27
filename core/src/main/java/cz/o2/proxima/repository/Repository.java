/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/** Repository of all entities configured in the system. */
@Evolving
public abstract class Repository {

  public interface ConfigFactory extends Serializable {
    Config apply();
  }

  private final ConfigFactory factory;

  private final Map<Class<? extends DataOperator>, DataOperator> operatorCache =
      new ConcurrentHashMap<>();

  /**
   * Construct the repository.
   *
   * @param factory the factory to create instance of this {@link Config}
   */
  Repository(ConfigFactory factory) {
    this.factory = factory;
  }

  /**
   * Create {@link Repository} from given {@link Config}.
   *
   * @param factory the config factory
   * @return repository
   */
  public static Repository of(ConfigFactory factory) {
    return ConfigRepository.of(factory);
  }

  /**
   * Create new {@link Repository} from {@link Config}.
   *
   * @param config the config to use
   * @return new {@link Repository}
   * @deprecated use {@link #of(ConfigFactory)} instead.
   */
  @Deprecated
  public static Repository of(Config config) {
    return ConfigRepository.of(config);
  }

  public RepositoryFactory asFactory() {
    return RepositoryFactory.caching(staticFactory(factory));
  }

  private static RepositoryFactory staticFactory(ConfigFactory factory) {
    return () -> Repository.of(factory.apply());
  }

  /**
   * Find entity descriptor based on entity name.
   *
   * @param name name of the entity to search for
   * @return optional {@link EntityDescriptor} found by name
   */
  public abstract Optional<EntityDescriptor> findEntity(String name);

  /**
   * Get entity descriptor based on entity name.
   *
   * @param name name of the entity to search for
   * @return {@link EntityDescriptor} found by name
   */
  public EntityDescriptor getEntity(String name) {
    return findEntity(name)
        .orElseThrow(
            () -> new IllegalArgumentException(String.format("Unable to find entity [%s].", name)));
  }

  /**
   * Retrieve stream of all entities.
   *
   * @return {@link Stream} of all entities specified in this repository
   */
  public abstract Stream<EntityDescriptor> getAllEntities();

  /**
   * Retrieve all transformers.
   *
   * @return all transformations by name
   */
  public abstract Map<String, TransformationDescriptor> getTransformations();

  /**
   * Check if this repository is empty.
   *
   * @return {@code true} if this repository is empty
   */
  public abstract boolean isEmpty();

  /**
   * List all unique attribute families.
   *
   * @return all families specified in this repository
   */
  public abstract Stream<AttributeFamilyDescriptor> getAllFamilies();

  /**
   * Retrieve list of attribute families for attribute.
   *
   * @param attr attribute descriptor
   * @return all families of given attribute
   */
  public abstract Set<AttributeFamilyDescriptor> getFamiliesForAttribute(
      AttributeDescriptor<?> attr);

  /**
   * Retrieve value serializer for given scheme.
   *
   * @param scheme scheme of the {@link cz.o2.proxima.scheme.ValueSerializerFactory}
   * @return optional {@link ValueSerializerFactory} for the scheme
   */
  public abstract Optional<ValueSerializerFactory> getValueSerializerFactory(String scheme);

  /**
   * Retrieve {@link DataOperator} representation for this {@link Repository}.
   *
   * @param <T> type of the operator
   * @param type the operator class
   * @param modifiers functions to be applied to the operator before it is returned
   * @return the data operator of given type
   */
  @SuppressWarnings("unchecked")
  @SafeVarargs
  public final synchronized <T extends DataOperator> T asDataOperator(
      Class<T> type, Consumer<T>... modifiers) {

    ServiceLoader<DataOperatorFactory> loaders = ServiceLoader.load(DataOperatorFactory.class);

    T ret =
        Streams.stream(loaders)
            .filter(f -> f.isOfType(type))
            .findAny()
            .map(o -> (DataOperatorFactory<T>) o)
            .map(
                f -> {
                  T op = f.create(this);
                  Arrays.stream(modifiers).forEach(m -> m.accept(op));
                  addedDataOperator(op);
                  return op;
                })
            .orElseThrow(() -> new IllegalStateException("Operator " + type + " not found."));

    operatorCache.put(type, ret);
    return ret;
  }

  /**
   * Retrieve an already created (via call to #asDataOperator} instance of data operator or create
   * new instance with default settings.
   *
   * @param <T> type of operator
   * @param type the operator class
   * @return the data operator of given type
   */
  @SuppressWarnings("unchecked")
  public final synchronized <T extends DataOperator> T getOrCreateOperator(Class<T> type) {

    T ret = (T) operatorCache.get(type);
    if (ret != null) {
      return ret;
    }
    return asDataOperator(type);
  }

  /**
   * Check if given implementation of data operator is available on classpath and {@link
   * #asDataOperator(java.lang.Class, Consumer...)} will return non-null object for class
   * corresponding the given name.
   *
   * @param name name of the operator
   * @return {@code true} if the operator is available, {@code false} otherwise
   */
  public boolean hasOperator(String name) {
    ServiceLoader<DataOperatorFactory> loaders = ServiceLoader.load(DataOperatorFactory.class);
    return Streams.stream(loaders).anyMatch(f -> f.getOperatorName().equals(name));
  }

  /**
   * Called when new {@link DataOperator} is created.
   *
   * @param op the operator that was created
   */
  protected void addedDataOperator(DataOperator op) {}

  /** Discard any cached {@link Repository}. */
  public void discard() {
    ((RepositoryFactory.Caching) asFactory()).drop();
  }
}
