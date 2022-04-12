/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
import com.google.common.collect.Iterables;
import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.transform.Transformation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
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
    final List<AttributeDescriptor<?>> attrs = new ArrayList<>();
    Transformation transformation;
    StorageFilter filter;
    boolean outputTransactions = true;

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

    Builder disableOutputTransactions() {
      this.outputTransactions = false;
      return this;
    }

    TransformationDescriptor build() {

      Preconditions.checkArgument(!attrs.isEmpty(), "Please specify at least one attribute");
      Preconditions.checkArgument(transformation != null, "Please specify transformation function");

      return new TransformationDescriptor(name, attrs, transformation, outputTransactions, filter);
    }
  }

  /** Mode of handling transactional attributes on input. */
  public enum InputTransactionMode {
    /** The input attributes are *all* transactional attributes. */
    TRANSACTIONAL,
    /** None of the input attributes is transactional attribute. */
    NON_TRANSACTIONAL
  }

  /** Mode of handling transactional attribute on output. */
  public enum OutputTransactionMode {
    /** Write all transactional attributes on output using transactions. */
    ENABLED,
    /** Write transactional attributes on output directly to target commit-log. */
    DISABLED
  }

  /** Name of the transformation. */
  @Getter private final String name;

  /** List of input attributes of the transformation. */
  private final List<AttributeDescriptor<?>> attributes;

  /** The (stateless) mapping function. */
  @Getter private final Transformation transformation;

  /** Input filter. */
  @Getter private final StorageFilter filter;

  @Getter private final InputTransactionMode inputTransactionMode;

  @Getter private final OutputTransactionMode outputTransactionMode;

  private TransformationDescriptor(
      String name,
      List<AttributeDescriptor<?>> attributes,
      Transformation transformation,
      boolean supportOutputTransactions,
      @Nullable StorageFilter filter) {

    this.name = Objects.requireNonNull(name);
    this.attributes = Objects.requireNonNull(attributes);
    this.transformation = Objects.requireNonNull(transformation);
    this.outputTransactionMode =
        supportOutputTransactions ? OutputTransactionMode.ENABLED : OutputTransactionMode.DISABLED;
    this.filter = filter == null ? new PassthroughFilter() : filter;
    this.inputTransactionMode =
        requireSingleTransactionMode(name, attributes) != TransactionMode.NONE
            ? InputTransactionMode.TRANSACTIONAL
            : InputTransactionMode.NON_TRANSACTIONAL;
  }

  private TransactionMode requireSingleTransactionMode(
      String name, List<AttributeDescriptor<?>> attributes) {
    Map<TransactionMode, List<AttributeDescriptor<?>>> grouped =
        attributes.stream().collect(Collectors.groupingBy(AttributeDescriptor::getTransactionMode));
    Preconditions.checkArgument(
        grouped.size() == 1,
        "Require all attributes from transform [ %s ] to have the same transaction "
            + "mode, got [ %s ] in [ %s ]",
        name,
        grouped.keySet(),
        attributes);
    return Iterables.getOnlyElement(grouped.keySet());
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
        .add("name", name)
        .add("attributes", attributes)
        .add("inputTransactionMode", inputTransactionMode)
        .add("outputTransactionMode", outputTransactionMode)
        .add("filter", filter)
        .toString();
  }
}
