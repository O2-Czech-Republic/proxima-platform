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
package cz.o2.proxima.transaction;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A combination of key of an entity, attribute descriptor and (optional) specific attribute. The
 * specific attribute is needed when this object needs to describe a specific attribute of wildcard
 * attribute descriptor.
 */
@Experimental
@ToString
@EqualsAndHashCode
public class KeyAttribute implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Create {@link KeyAttribute} for given entity, key and attribute descriptor. This describes
   * either all wildcard attributes of that key or a single regular attribute.
   *
   * @param entity the entity descriptor
   * @param key the entity key
   * @param attributeDescriptor descriptor of wildcard or regular attribute
   * @param sequenceId sequence ID of the read attribute
   */
  public static KeyAttribute ofAttributeDescriptor(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> attributeDescriptor,
      long sequenceId) {

    Preconditions.checkArgument(
        !attributeDescriptor.isWildcard(),
        "Please specify attribute suffix for wildcard attributes. Got attribute %s",
        attributeDescriptor);
    return new KeyAttribute(entity, key, attributeDescriptor, sequenceId, null);
  }

  /**
   * Create {@link KeyAttribute} for given entity, key and attribute descriptor. This describes
   * either all wildcard attributes of that key or a single regular attribute.
   *
   * @param entity the entity descriptor
   * @param key the entity key
   * @param attributeDescriptor descriptor of wildcard or regular attribute
   * @param sequenceId sequence ID of the read attribute
   * @param attributeSuffix a specific attribute suffix when {@code attributeDescriptor} is wildcard
   *     attribute
   */
  public static KeyAttribute ofAttributeDescriptor(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> attributeDescriptor,
      long sequenceId,
      @Nullable String attributeSuffix) {

    Preconditions.checkArgument(
        !attributeDescriptor.isWildcard() || attributeSuffix != null,
        "Please specify attribute suffix for wildcard attributes. Got attribute %s",
        attributeDescriptor);
    return new KeyAttribute(entity, key, attributeDescriptor, sequenceId, attributeSuffix);
  }

  /**
   * Create a {@link KeyAttribute} for given {@link StreamElement}.
   *
   * @param element the {@link StreamElement} that should be part of the transaction
   */
  public static KeyAttribute ofStreamElement(StreamElement element) {
    Preconditions.checkArgument(
        element.hasSequentialId(),
        "Elements read from commit-logs with enabled transactions need to use sequenceIds.");
    return new KeyAttribute(
        element.getEntityDescriptor(),
        element.getKey(),
        element.getAttributeDescriptor(),
        element.getSequentialId(),
        element.getAttributeDescriptor().isWildcard() ? element.getAttribute() : null);
  }

  @Getter private final EntityDescriptor entity;
  @Getter private final String key;
  @Getter private final AttributeDescriptor<?> attributeDescriptor;
  @Getter private final long sequenceId;
  @Nullable private final String attribute;

  private KeyAttribute(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> attributeDescriptor,
      long sequenceId,
      @Nullable String attribute) {

    this.entity = entity;
    this.key = key;
    this.attributeDescriptor = attributeDescriptor;
    this.sequenceId = sequenceId;
    this.attribute = attribute;

    Preconditions.checkArgument(sequenceId > 0, "Sequence ID must be positive, got %s", sequenceId);
  }

  public Optional<String> getAttribute() {
    return Optional.ofNullable(attribute);
  }
}
