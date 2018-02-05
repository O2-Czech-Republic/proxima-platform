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

import cz.o2.proxima.util.NamePattern;
import cz.seznam.euphoria.shadow.com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Descriptor of entity.
 */
public class EntityDescriptorImpl implements EntityDescriptor {


  /** Name of the entity. */
  @Getter
  private final String name;

  /** List of all attribute descriptors. */
  private final List<AttributeDescriptor<?>> attributes;

  /** Map of attributes by name. */
  private final Map<String, AttributeDescriptor<?>> attributesByName;

  /** Map of attributes by pattern. */
  private final Map<NamePattern, AttributeDescriptor<?>> attributesByPattern;

  EntityDescriptorImpl(String name, List<AttributeDescriptor<?>> attrs) {
    this.name = Objects.requireNonNull(name);
    this.attributes = Collections.unmodifiableList(Objects.requireNonNull(attrs));

    List<AttributeDescriptor<?>> fullyQualified = attrs.stream()
        .filter(a -> !a.isWildcard())
        .collect(Collectors.toList());

    attributesByPattern = attrs.stream()
        .filter(AttributeDescriptor::isWildcard)
        .map(p -> Maps.immutableEntry(new NamePattern(p.getName()), p))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    this.attributesByName = Maps.uniqueIndex(
        fullyQualified, AttributeDescriptor::getName);
  }


  /** Find attribute based by name. */
  @SuppressWarnings("unchecked")
  @Override
  public <T> Optional<AttributeDescriptor<T>> findAttribute(
      String name, boolean includeProtected) {

    AttributeDescriptor found = attributesByName.get(name);
    if (found == null) {
      for (Map.Entry<NamePattern, AttributeDescriptor<?>> e : attributesByPattern.entrySet()) {
        if (e.getKey().matches(name)) {
          found = e.getValue();
          break;
        }
      }
    }
    if (found != null && (includeProtected || found.isPublic())) {
      return Optional.of(found);
    }
    return Optional.empty();
  }


  /** List all attribute descriptors of given entity. */
  @Override
  public List<AttributeDescriptor<?>> getAllAttributes(boolean includeProtected) {
    if (includeProtected) {
      return Collections.unmodifiableList(attributes);
    }
    return attributes.stream()
        .filter(a -> a.isPublic())
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "EntityDescriptor(" + name + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EntityDescriptor) {
      EntityDescriptor other = (EntityDescriptor) obj;
      return other.getName().equals(name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

}
