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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Repository of all entities configured in the system. */
@Slf4j
@Evolving
public abstract class Repository implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Various validation flags. */
  public enum Validate {
    /** Do not perform any validations. */
    NONE(0x00),
    /** Validate that attributes have associated families. */
    FAMILIES(0x01),
    /** Validate that families have correctly configured access patterns. */
    ACCESSES(0x02),
    /** Validate scheme serializers. */
    SERIALIZERS(0x04),

    /** Turn on all validations. */
    ALL(FAMILIES.flag | ACCESSES.flag | SERIALIZERS.flag);

    @Getter final int flag;

    Validate(int value) {
      this.flag = value;
    }

    /**
     * A default flag that is used when constructing {@link Repository} using {@link
     * Repository#ofTest(Config, Validate...)}
     *
     * @return the default flag to use for testing repositories
     */
    public static int defaultTesting() {
      return ALL.getFlag() & ~Validate.FAMILIES.getFlag();
    }
  }

  /**
   * Create {@link Repository} from given {@link Config}.
   *
   * @param config the config
   * @return repository
   */
  public static Repository of(Config config) {
    return ConfigRepository.of(config);
  }

  /**
   * Create {@link Repository} from given {@link Config} for testing purposes.
   *
   * @param config the config
   * @param validate validations that should be performed
   * @return repository
   */
  public static Repository ofTest(Config config, Validate... validate) {
    return ConfigRepository.ofTest(config, validate);
  }

  RepositoryFactory factory;

  Repository() {
    this.factory =
        RepositoryFactory.local(
            this,
            () -> {
              throw new UnsupportedOperationException();
            });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Getter(AccessLevel.PACKAGE)
  private final transient Iterable<DataOperatorFactory<?>> dataOperatorFactories =
      (Iterable) ServiceLoader.load(DataOperatorFactory.class);

  private final transient Map<String, DataOperator> operatorCache = new ConcurrentHashMap<>();

  /**
   * Convert this repository to {@link Serializable} factory.
   *
   * @return this repository as factory
   */
  public RepositoryFactory asFactory() {
    return Objects.requireNonNull(factory);
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
   * Retrieve attribute family by name.
   *
   * <p>Note that this searched all families that were specified in configuration. It might include
   * families not listed in {@link #getAllFamilies()}, because some families might be removed for
   * various reasons (e.g. when proxying attributes).
   *
   * @param name name of the family
   * @return {@link Optional} {@link AttributeFamilyDescriptor} if family exists
   */
  public abstract Optional<AttributeFamilyDescriptor> findFamilyByName(String name);

  /**
   * Retrieve attribute family by name.
   *
   * <p>Note that this searched all families that were specified in configuration. It might include
   * families not listed in {@link #getAllFamilies()}, because some families might be removed for
   * various reasons (e.g. when proxying attributes).
   *
   * @param name name of the family
   * @return {@link AttributeFamilyDescriptor} if family exists
   * @throws IllegalArgumentException when family doesn't exist
   */
  public AttributeFamilyDescriptor getFamilyByName(String name) {
    return findFamilyByName(name)
        .orElseThrow(() -> new IllegalArgumentException("Family " + name + " doesn't exist"));
  }

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
  @VisibleForTesting
  @SuppressWarnings("unchecked")
  @SafeVarargs
  private final synchronized <T extends DataOperator> T asDataOperator(
      Class<T> type, Consumer<T>... modifiers) {

    Iterable<DataOperatorFactory<?>> loaders = getDataOperatorFactories();

    return Streams.stream(loaders)
        .filter(f -> f.isOfType(type))
        .findAny()
        .map(o -> (DataOperatorFactory<T>) o)
        .map(
            f -> {
              T op = f.create(this);
              Arrays.stream(modifiers).forEach(m -> m.accept(op));
              return cacheDataOperator(op);
            })
        .orElseThrow(() -> new IllegalStateException("Operator " + type + " not found."));
  }

  <T extends DataOperator> T cacheDataOperator(T op) {
    addedDataOperator(op);
    operatorCache.put(op.getClass().getName(), op);
    return op;
  }

  /**
   * Retrieve instance of data operator or create new instance with given settings.
   *
   * @param <T> type of operator
   * @param type the operator class
   * @param modifiers modifiers of operator applied when the operator is created
   * @return the data operator of given type
   */
  @SuppressWarnings("unchecked")
  public final synchronized <T extends DataOperator> T getOrCreateOperator(
      Class<T> type, Consumer<T>... modifiers) {

    T ret = (T) operatorCache.get(type.getName());
    if (ret != null) {
      return ret;
    }
    return asDataOperator(type, modifiers);
  }

  /**
   * Check if given implementation of data operator is available on classpath and {@link
   * #getOrCreateOperator(Class, Consumer[])} (java.lang.Class, Consumer...)} will return non-null
   * object for class corresponding the given name.
   *
   * @param name name of the operator
   * @return {@code true} if the operator is available, {@code false} otherwise
   */
  public boolean hasOperator(String name) {
    Iterable<DataOperatorFactory<?>> loaders = getDataOperatorFactories();
    return Streams.stream(loaders).anyMatch(f -> f.getOperatorName().equals(name));
  }

  /**
   * Check if this {@link Repository} should eagerly validate various settings.
   *
   * @param what validation flag
   * @return {@code true} if this Repository should validate settings before usage (typically
   *     production settings, while test settings can be less strict).
   */
  public abstract boolean isShouldValidate(Validate what);

  /**
   * Drop the {@link Repository} and let it recreate from scratch using factory. This is intended
   * for use in tests mostly to prevent influence between two test cases.
   */
  public abstract void drop();

  /**
   * Called when new {@link DataOperator} is created.
   *
   * @param op the operator that was created
   */
  protected void addedDataOperator(DataOperator op) {}
}
