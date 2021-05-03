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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

@Experimental
public class KeyAttributes {

  /**
   * Create a list of {@link KeyAttribute KeyAttributes} that represent a query for wildcard
   * attribute and the returned
   *
   * @param entity the entity descriptor
   * @param key the key of the entity
   * @param wildcardAttribute the descriptor of wildcard attribute
   * @param elements the elements returned by query
   * @return
   */
  public static List<KeyAttribute> ofWildcardQueryElements(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> wildcardAttribute,
      Iterable<StreamElement> elements) {

    List<KeyAttribute> ret = new ArrayList<>();
    long minSeqId = Long.MAX_VALUE;
    for (StreamElement e : elements) {
      Preconditions.checkArgument(
          e.getAttributeDescriptor().equals(wildcardAttribute),
          "All passed attribute descriptors must match %s, got %s",
          wildcardAttribute,
          e.getAttributeDescriptor());
      Preconditions.checkArgument(
          e.getKey().equals(key),
          "All passed attribute descriptors must match the same key %s, got %s",
          key,
          e.getKey());
      ret.add(ofStreamElement(e));
      minSeqId = Math.min(minSeqId, e.getSequentialId());
    }
    if (minSeqId == Long.MAX_VALUE) {
      minSeqId = 1L;
    }
    ret.add(new KeyAttribute(entity, key, wildcardAttribute, minSeqId, false, null));
    return ret;
  }

  private KeyAttributes() {}

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
    return new KeyAttribute(entity, key, attributeDescriptor, sequenceId, false, null);
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
    return new KeyAttribute(entity, key, attributeDescriptor, sequenceId, false, attributeSuffix);
  }

  /**
   * Create a {@link KeyAttribute} for given {@link StreamElement}.
   *
   * @param element the {@link StreamElement} that should be part of the transaction
   */
  public static KeyAttribute ofStreamElement(StreamElement element) {
    Preconditions.checkArgument(
        element.hasSequentialId(),
        "Elements read with enabled transactions need to use sequenceIds got %s.",
        element);
    Preconditions.checkArgument(!element.isDeleteWildcard(), "Wildcard deletes not yet supported");
    return new KeyAttribute(
        element.getEntityDescriptor(),
        element.getKey(),
        element.getAttributeDescriptor(),
        element.getSequentialId(),
        element.isDelete(),
        element.getAttributeDescriptor().isWildcard() ? element.getAttribute() : null);
  }
}
