/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * An interface representing descriptor of entity.
 */
public interface EntityDescriptor extends Serializable {

  /** Builder of the descriptor. */
  class Builder {

    @Setter
    @Accessors(chain = true)
    private String name;

    private final List<AttributeDescriptorBase<?>> attributes = new ArrayList<>();

    public Builder addAttribute(AttributeDescriptorBase<?> attr) {
      attributes.add(attr);
      return this;
    }

    public EntityDescriptor build() {
      return new EntityDescriptorImpl(name, Collections.unmodifiableList(attributes));
    }

    AttributeDescriptorBase<?> findAttribute(String attr) {
      return attributes.stream().filter(a -> a.getName()
          .equals(attr)).findAny()
          .orElseThrow(() -> new IllegalArgumentException(
              "Cannot find attribute " + attr + " of entity " + this.name));
    }

  }

  static Builder newBuilder() {
    return new Builder();
  }


  /**
   * Name of the entity.
   * @return name of the entity
   */
  String getName();

  /**
   * Find attribute by name.
   * @param name name of the attribute to search for
   * @param includeProtected {@code true} to allow search for protected fields (prefixed by _).
   * @return optional found attribute descriptor
   */
  Optional<AttributeDescriptor<?>> findAttribute(String name, boolean includeProtected);

  /**
   * Find attribute by name.
   * Do not search protected fields (prefixed by _).
   * @param name name of the attribute to search for
   * @return optional found attribute descriptor
   */
  default Optional<AttributeDescriptor<?>> findAttribute(String name) {
    return findAttribute(name, false);
  }

  /**
   * Find all attributes of this entity.
   * @param includeProtected when {@code true} then protected attributes are
   * also included (prefixed by _).
   * @return all attributes of entity (including protected or not)
   */
  List<AttributeDescriptor<?>> getAllAttributes(boolean includeProtected);

  /**
   * List all attribute descriptors of given entity.
   * @return get all unprotected attributes of entity
   */
  default List<AttributeDescriptor<?>> getAllAttributes() {
    return getAllAttributes(false);
  }

}
