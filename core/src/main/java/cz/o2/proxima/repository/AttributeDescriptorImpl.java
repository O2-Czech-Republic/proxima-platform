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

import java.net.URI;
import java.util.Objects;
import lombok.Getter;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.scheme.ValueSerializer;

/**
 * Descriptor of attribute of entity.
 */
public class AttributeDescriptorImpl<T> implements AttributeDescriptor<T> {

  @Getter
  private final String name;

  @Getter
  private final boolean isWildcard;

  @Getter
  private final URI schemeURI;

  @Getter
  private final ValueSerializer<T> valueSerializer;

  @Getter
  private final String entity;

  @Getter
  private OnlineAttributeWriter writer;

  AttributeDescriptorImpl(
      String name, String entity,
      URI schemeURI, ValueSerializer<T> parser) {

    this.name = Objects.requireNonNull(name);
    this.schemeURI = Objects.requireNonNull(schemeURI);
    this.valueSerializer = Objects.requireNonNull(parser);
    this.entity = Objects.requireNonNull(entity);
    this.isWildcard = this.name.contains("*");
    if (this.isWildcard) {
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

  public void setWriter(OnlineAttributeWriter writer) {
    this.writer = writer;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AttributeDescriptor) {
      AttributeDescriptor other = (AttributeDescriptor) obj;
      return Objects.equals(other.getEntity(), entity)
          && Objects.equals(other.getName(), name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity, name);
  }

  @Override
  public String toString() {
    return "AttributeDescriptor(entity=" + entity + ", name=" + name + ")";
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

}
