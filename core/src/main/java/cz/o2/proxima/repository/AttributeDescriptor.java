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

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import java.io.Serializable;
import java.net.URI;
import java.util.Objects;

import lombok.Setter;
import lombok.experimental.Accessors;
import cz.o2.proxima.scheme.ValueSerializer;

/**
 * An interface describing each attribute.
 */
@Stable
@Accessors(chain = true)
public interface AttributeDescriptor<T> extends Serializable {

  class Builder {

    private final Repository repo;

    private Builder(Repository repo) {
      this.repo = repo;
    }

    @Setter
    private String entity;

    @Setter
    private String name;

    @Setter
    private URI schemeURI;

    @Setter
    private boolean replica = false;

    @SuppressWarnings("unchecked")
    public <T> AttributeDescriptorImpl<T> build() {
      Objects.requireNonNull(name, "Please specify name");
      Objects.requireNonNull(entity, "Please specify entity");
      Objects.requireNonNull(schemeURI, "Please specify scheme URI");

      ValueSerializerFactory factory = repo.getValueSerializerFactory(schemeURI.getScheme());

      return new AttributeDescriptorImpl<>(
          name, entity,
          schemeURI,
          factory == null
              ? null
              : (ValueSerializer<T>) factory.getValueSerializer(schemeURI),
          replica);
    }
  }

  static Builder newBuilder(Repository repo) {
    return new Builder(repo);
  }

  static <T> AttributeDescriptorBase<T> newProxy(
      String name,
      AttributeDescriptor<T> targetRead,
      ProxyTransform transformRead,
      AttributeDescriptor<T> targetWrite,
      ProxyTransform transformWrite) {

    return new AttributeProxyDescriptorImpl<>(
        name, targetRead, transformRead, targetWrite, transformWrite);
  }

  /**
   * Retrieve name of the attribute.
   * @return name of the attribute
   */
  String getName();

  /**
   * Check if this is a wildcard attribute.
   * @return {@code true} when this is wildcard attribute
   */
  boolean isWildcard();

  /**
   * Retrieve URI of the scheme of this attribute.
   * @return scheme URI of this attribute
   */
  URI getSchemeURI();

  /**
   * Retrieve name of the associated entity.
   * @return name of the associated entity
   */
  String getEntity();

  /**
   * Retrieve name of the attribute if not wildcard, otherwise
   * retrieve the prefix without the last asterisk.
   * @return attribute prefix of this attribute
   */
  default String toAttributePrefix() {
    return toAttributePrefix(true);
  }

  /**
   * Retrieve name of the attribute if not wildcard, otherwise
   * retrieve the prefix without the last asterisk.
   * @param includeLastDot {@code true} to include dot suffix of the prefix
   * @return attribute prefix with or without dot
   */
  String toAttributePrefix(boolean includeLastDot);

  /**
   * Retrieve serializer for value type.
   * @return {@link ValueSerializer} of this attribute's value
   */
  ValueSerializer<T> getValueSerializer();

  /**
   * Marker if this is a public attribute.
   * @return {@code true} it this is public attribute
   */
  boolean isPublic();

  /**
   * Convert this attribute back to builder.
   * @param repo the repository
   * @return builder representing this attribute
   */
  public AttributeDescriptor.Builder toBuilder(Repository repo);

}
