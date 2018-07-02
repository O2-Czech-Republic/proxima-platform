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

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.scheme.ValueSerializer;
import java.net.URI;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Base class for {@link AttributeDescriptorImpl} and {@link AttributeProxyDescriptorImpl}.
 */
@Internal
public abstract class AttributeDescriptorBase<T> implements AttributeDescriptor<T> {

  @Getter
  protected final String entity;

  @Getter
  protected final String name;

  @Getter
  protected final URI schemeURI;

  @Getter
  protected final boolean proxy;

  @Getter
  protected final boolean wildcard;

  @Getter
  protected final boolean replica;

  protected @Nullable final ValueSerializer<T> valueSerializer;

  public AttributeDescriptorBase(
      String name, String entity, URI schemeURI,
      @Nullable ValueSerializer<T> valueSerializer,
      boolean replica) {

    this.name = Objects.requireNonNull(name);
    this.entity = Objects.requireNonNull(entity);
    this.schemeURI = Objects.requireNonNull(schemeURI);
    this.wildcard = this.name.endsWith(".*");
    this.proxy = false;
    this.valueSerializer = valueSerializer;
    this.replica = replica;
    if (this.wildcard) {
      if (name.length() < 3
          || name.substring(0, name.length() - 1).contains("*")
          || name.charAt(name.length() - 2) != '.') {

        throw new IllegalArgumentException(
            "Please specify wildcard attributes only in the format `<name>.*; for now. "
                + "That is - wildcard attributes can contain only single asterisk "
                + "right after a dot at the end of the attribute name. "
                + "This is implementation constraint for now.");
      }
    }
  }

  public AttributeDescriptorBase(
      String name,
      AttributeDescriptor<T> targetRead,
      AttributeDescriptor<T> targetWrite,
      boolean replica) {

    this.name = Objects.requireNonNull(name);
    Preconditions.checkArgument(
        targetRead.getEntity().equals(targetWrite.getEntity()),
        String.format(
            "Cannot mix entities in proxies, got %s and %s",
            targetRead.getEntity(), targetWrite.getEntity()));
    Preconditions.checkArgument(
        targetRead.getSchemeURI().equals(targetWrite.getSchemeURI()),
        String.format(
            "Cannot mix attributes with different schemes, got %s and %s",
            targetRead.getSchemeURI(), targetWrite.getSchemeURI()));
    Preconditions.checkArgument(
        targetRead.isWildcard() == targetWrite.isWildcard(),
        String.format(
            "Cannot mix non-wildcard and wildcard attributes in proxy"));
    this.entity = targetRead.getEntity();
    this.schemeURI = targetRead.getSchemeURI();
    this.proxy = true;
    this.replica = replica;
    this.wildcard = targetRead.isWildcard();
    this.valueSerializer = targetRead.getValueSerializer();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AttributeDescriptor) {
      AttributeDescriptor other = (AttributeDescriptor) obj;
      return Objects.equals(
          other.getEntity(), entity) && Objects.equals(other.getName(), name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity, name);
  }

  /**
   * Retrieve name of the attribute if not wildcard, otherwise
   * retrieve the prefix without the last asterisk.
   */
  @Override
  public String toAttributePrefix(boolean includeLastDot) {
    if (isWildcard()) {
      return name.substring(0, name.length() - (includeLastDot ? 1 : 2));
    }
    return name;
  }

  @Override
  public boolean isPublic() {
    return !name.startsWith("_");
  }

  @Override
  public ValueSerializer<T> getValueSerializer() {
    return Objects.requireNonNull(valueSerializer);
  }

  @Override
  public Builder toBuilder(Repository repo) {
    return AttributeDescriptor.newBuilder(repo)
        .setName(getName())
        .setEntity(getEntity())
        .setSchemeURI(getSchemeURI());
  }

  AttributeProxyDescriptorImpl<T> toProxy() {
    Preconditions.checkArgument(
        this instanceof AttributeProxyDescriptorImpl,
        "Attribute " + this + " is not proxy attribute");
    return (AttributeProxyDescriptorImpl<T>) this;
  }

}
