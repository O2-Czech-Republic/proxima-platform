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

  @Getter private final EntityDescriptor entity;
  @Getter private final String key;
  @Getter private final AttributeDescriptor<?> attributeDescriptor;
  @Getter private final long sequenceId;
  @Getter private final boolean delete;
  @Nullable private final String attributeSuffix;

  public KeyAttribute(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> attributeDescriptor,
      long sequenceId,
      boolean delete,
      @Nullable String attributeSuffix) {

    this.entity = entity;
    this.key = key;
    this.attributeDescriptor = attributeDescriptor;
    this.sequenceId = sequenceId;
    this.delete = delete;
    this.attributeSuffix = attributeSuffix;

    Preconditions.checkArgument(sequenceId > 0, "Sequence ID must be positive, got %s", sequenceId);
    Preconditions.checkArgument(
        attributeSuffix == null
            || !attributeSuffix.startsWith(attributeDescriptor.toAttributePrefix()),
        "Attribute suffix %s must NOT start with %s",
        attributeSuffix,
        attributeDescriptor.toAttributePrefix());
  }

  public Optional<String> getAttributeSuffix() {
    return Optional.ofNullable(attributeSuffix);
  }

  public boolean isWildcardQuery() {
    return attributeDescriptor.isWildcard() && attributeSuffix == null;
  }
}
