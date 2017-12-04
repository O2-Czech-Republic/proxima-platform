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

import cz.seznam.euphoria.shaded.guava.com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Getter;

/**
 * Descriptor of single transformation specified in {@code transformations}.
 */
public class TransformationDescriptor implements Serializable {

  static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {

    EntityDescriptor entity;
    final List<AttributeDescriptor<?>> attrs = new ArrayList<>();
    Class<? extends Transformation> transformation;

    Builder setEntity(EntityDescriptor entity) {
      this.entity = entity;
      return this;
    }

    Builder setTransformationClass(Class<? extends Transformation> transformation) {
      this.transformation = transformation;
      return this;
    }

    Builder addAttributes(AttributeDescriptor<?>... attrs) {
      Arrays.stream(attrs).forEach(this.attrs::add);
      return this;
    }

    Builder addAttributes(Iterable<AttributeDescriptor<?>> attrs) {
      attrs.forEach(this.attrs::add);
      return this;
    }

    TransformationDescriptor build() {

      Preconditions.checkArgument(
          !attrs.isEmpty(), "Please specify at least one attribute");
      Preconditions.checkArgument(transformation != null,
          "Please specify transformation function");
      Preconditions.checkArgument(entity != null,
          "Please specify source entity");

      try {
        return new TransformationDescriptor(entity, attrs, transformation.newInstance());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Getter
  private final EntityDescriptor entity;
  @Getter
  private final List<AttributeDescriptor<?>> attributes;
  @Getter
  private final Transformation transformation;

  private TransformationDescriptor(
      EntityDescriptor entity,
      List<AttributeDescriptor<?>> attributes,
      Transformation transformation) {

    this.entity = Objects.requireNonNull(entity);
    this.attributes = Collections.unmodifiableList(attributes);
    this.transformation = Objects.requireNonNull(transformation);
  }
}
