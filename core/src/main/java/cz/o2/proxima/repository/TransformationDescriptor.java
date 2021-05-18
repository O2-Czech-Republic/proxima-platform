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
package cz.o2.proxima.repository;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.transform.Transformation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.Getter;

/** Descriptor of single transformation specified in {@code transformations}. */
@Evolving
public class TransformationDescriptor implements Serializable {

  private static final long serialVersionUID = 1L;

  static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {

    String name;
    EntityDescriptor entity;
    final List<AttributeDescriptor<?>> attrs = new ArrayList<>();
    Transformation transformation;
    StorageFilter filter;
    boolean supportTransactions = true;

    Builder setEntity(EntityDescriptor entity) {
      this.entity = entity;
      return this;
    }

    Builder setName(String name) {
      this.name = name;
      return this;
    }

    Builder setTransformation(Transformation transformation) {
      this.transformation = transformation;
      return this;
    }

    Builder setFilter(StorageFilter filter) {
      this.filter = filter;
      return this;
    }

    Builder addAttributes(AttributeDescriptor<?>... attrs) {
      this.addAttributes(Arrays.asList(attrs));
      return this;
    }

    Builder addAttributes(Iterable<AttributeDescriptor<?>> attrs) {
      attrs.forEach(this.attrs::add);
      return this;
    }

    Builder disallowTransactions() {
      this.supportTransactions = false;
      return this;
    }

    TransformationDescriptor build() {

      Preconditions.checkArgument(!attrs.isEmpty(), "Please specify at least one attribute");
      Preconditions.checkArgument(transformation != null, "Please specify transformation function");
      Preconditions.checkArgument(entity != null, "Please specify source entity");

      return new TransformationDescriptor(
          name, entity, attrs, transformation, supportTransactions, filter);
    }
  }

  @Getter private final String name;

  @Getter private final EntityDescriptor entity;

  private final List<AttributeDescriptor<?>> attributes;

  @Getter private final Transformation transformation;

  @Getter private final boolean supportTransactions;

  @Getter private final StorageFilter filter;

  private TransformationDescriptor(
      String name,
      EntityDescriptor entity,
      List<AttributeDescriptor<?>> attributes,
      Transformation transformation,
      boolean supportTransactions,
      @Nullable StorageFilter filter) {

    this.name = Objects.requireNonNull(name);
    this.entity = Objects.requireNonNull(entity);
    this.attributes = Objects.requireNonNull(attributes);
    this.transformation = Objects.requireNonNull(transformation);
    this.supportTransactions = supportTransactions;
    this.filter = filter == null ? new PassthroughFilter() : filter;
  }

  public List<AttributeDescriptor<?>> getAttributes() {
    return Collections.unmodifiableList(attributes);
  }

  void replaceAttribute(AttributeDescriptor<?> attr) {
    attributes.remove(attr);
    attributes.add(attr);
  }

  public ConsumerNameFactory<TransformationDescriptor> getConsumerNameFactory() {
    return new ConsumerNameFactory<TransformationDescriptor>() {

      private static final long serialVersionUID = 1L;

      @Override
      public void setup(TransformationDescriptor descriptor) {
        // nop
      }

      @Override
      public String apply() {
        return "transformer-" + getName();
      }
    };
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("entity", entity)
        .add("attributes", attributes)
        .add("supportTransactions", supportTransactions)
        .toString();
  }
}
