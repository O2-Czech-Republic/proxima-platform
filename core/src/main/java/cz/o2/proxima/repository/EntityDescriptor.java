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
package cz.o2.proxima.repository;

import cz.o2.proxima.annotations.Stable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Setter;
import lombok.experimental.Accessors;

/** An interface representing descriptor of entity. */
@Stable
public interface EntityDescriptor extends Serializable {

  /** Builder of the descriptor. */
  class Builder {

    @Setter
    @Accessors(chain = true)
    private String name;

    private final Map<String, AttributeDescriptor<?>> attributes = new HashMap<>();

    public Builder addAttribute(AttributeDescriptor<?> attr) {
      String nameToCheck = attr.toAttributePrefix(false);
      if (!attr.isWildcard()) {
        nameToCheck = String.format("%s.*", attr.getName());
      }
      if (attributes.containsKey(nameToCheck)) {
        throw new IllegalArgumentException(
            String.format(
                "Attribute name [%s] must be unique in entity [%s]. Duplicated with [%s] or [%s.*].",
                attr.getName(), attr.getEntity(), nameToCheck, attr.toAttributePrefix(false)));
      }
      attributes.put(attr.getName(), attr);
      return this;
    }

    public EntityDescriptor build() {
      return new EntityDescriptorImpl(name, attributes.values());
    }

    AttributeDescriptor<?> getAttribute(String attr) {
      final AttributeDescriptor<?> attributeDescriptor = attributes.get(attr);
      if (attributeDescriptor == null) {
        throw new IllegalArgumentException(
            String.format("Unable to find attribute [%s] of entity [%s].", attr, name));
      }
      return attributeDescriptor;
    }
  }

  static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Name of the entity.
   *
   * @return name of the entity
   */
  String getName();

  /**
   * Find attribute by name.
   *
   * @param <T> value type
   * @param name name of the attribute to search for
   * @param includeProtected {@code true} to allow search for protected fields (prefixed by _).
   * @return optional found attribute descriptor
   */
  <T> Optional<AttributeDescriptor<T>> findAttribute(String name, boolean includeProtected);

  /**
   * Find attribute by name. Do not search protected fields (prefixed by _).
   *
   * @param <T> value type
   * @param name name of the attribute to search for
   * @return optional found attribute descriptor
   */
  default <T> Optional<AttributeDescriptor<T>> findAttribute(String name) {
    return findAttribute(name, false);
  }

  /**
   * Get attribute by name.
   *
   * @param <T> value type
   * @param name name of the attribute to search for
   * @param includeProtected {@code true} to allow search for protected fields (prefixed by _).
   * @return attribute descriptor
   */
  default <T> AttributeDescriptor<T> getAttribute(String name, boolean includeProtected) {
    final Optional<AttributeDescriptor<T>> maybeAttribute = findAttribute(name, includeProtected);
    return maybeAttribute.orElseThrow(
        () ->
            new IllegalArgumentException(
                String.format("Unable to find attribute [%s] of entity [%s].", name, getName())));
  }

  /** @return {@code true} if the entity contains any attribute that has transactions enabled. */
  default boolean isTransactional() {
    return getAllAttributes()
        .stream()
        .anyMatch(attr -> attr.getTransactionMode() != TransactionMode.NONE);
  }

  /**
   * @return {@code true} if this is system entity. System entities are created by the platform
   *     itself and should be (directly) accessed by users.
   */
  default boolean isSystemEntity() {
    return getName().startsWith("_");
  }

  /**
   * Get attribute by name.
   *
   * @param <T> value type
   * @param name name of the attribute to search for
   * @return attribute descriptor
   */
  default <T> AttributeDescriptor<T> getAttribute(String name) {
    return getAttribute(name, false);
  }

  /**
   * Find all attributes of this entity.
   *
   * @param includeProtected when {@code true} then protected attributes are also included (prefixed
   *     by _).
   * @return all attributes of entity (including protected or not)
   */
  List<AttributeDescriptor<?>> getAllAttributes(boolean includeProtected);

  /**
   * List all attribute descriptors of given entity.
   *
   * @return get all unprotected attributes of entity
   */
  default List<AttributeDescriptor<?>> getAllAttributes() {
    return getAllAttributes(false);
  }
}
