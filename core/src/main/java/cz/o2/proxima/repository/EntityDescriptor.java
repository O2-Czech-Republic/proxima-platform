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

    List<AttributeDescriptor> attributes = new ArrayList<>();

    public Builder addAttribute(AttributeDescriptor attr) {
      attributes.add(attr);
      return this;
    }

    public EntityDescriptor build() {
      return new EntityDescriptorImpl(name, Collections.unmodifiableList(attributes));
    }
  }

  static Builder newBuilder() {
    return new Builder();
  }


  /** Name of the entity. */
  String getName();

  /** Find attribute based by name. */
  Optional<AttributeDescriptor<?>> findAttribute(String name);

  /** List all attribute descriptors of given entity. */
  List<AttributeDescriptor> getAllAttributes();

}
