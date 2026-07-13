/*
 * Copyright 2017-2026 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.repository;

import cz.o2.proxima.core.annotations.Stable;
import javax.annotation.Nullable;
import lombok.Value;

/** A specific attribute of a wildcard attribute descriptor. */
@Stable
@Value
public class TypedAttribute<T> {

  public static <T> TypedAttribute<T> of(
      AttributeDescriptor<T> descriptor, String attributeSuffix) {

    return new TypedAttribute<>(descriptor, attributeSuffix);
  }

  public static <T> TypedAttribute<T> of(AttributeDescriptor<T> descriptor) {
    return new TypedAttribute<>(descriptor, null);
  }

  AttributeDescriptor<T> descriptor;
  String attributeKey;
  @Nullable String attributeSuffix;

  private TypedAttribute(AttributeDescriptor<T> descriptor, @Nullable String attributeSuffix) {

    this.descriptor = descriptor;
    if (attributeSuffix != null) {
      this.attributeSuffix = attributeSuffix;
      this.attributeKey = descriptor.toAttributePrefix() + attributeSuffix;
    } else {
      this.attributeSuffix = null;
      this.attributeKey = descriptor.getName();
    }
  }

  public boolean isWildcard() {
    return attributeSuffix != null;
  }
}
