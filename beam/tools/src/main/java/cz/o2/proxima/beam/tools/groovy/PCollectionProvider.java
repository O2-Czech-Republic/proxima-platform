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
package cz.o2.proxima.beam.tools.groovy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

interface PCollectionProvider<T> {

  static <T> PCollectionProvider<T> wrap(PCollection<T> collection) {
    return new PCollectionProvider<T>() {
      @Override
      public PCollection<T> materialize(Pipeline pipeline) {
        return collection;
      }

      @Override
      public void asUnbounded() {
        // nop
      }
    };
  }

  class ParentNotifyingProvider<T> implements PCollectionProvider<T> {

    final Function<Pipeline, PCollection<T>> factory;
    final List<PCollectionProvider<?>> parents = new ArrayList<>();

    ParentNotifyingProvider(
        Function<Pipeline, PCollection<T>> factory, List<PCollectionProvider<?>> parents) {

      this.factory = factory;
      this.parents.addAll(parents);
    }

    @Override
    public void asUnbounded() {
      parents.forEach(PCollectionProvider::asUnbounded);
    }

    @Override
    public PCollection<T> materialize(Pipeline pipeline) {
      return factory.apply(pipeline);
    }
  }

  class CachedPCollectionProvider<T> implements PCollectionProvider<T> {

    private final PCollectionProvider<T> underlying;
    private PCollection<T> materialized;

    CachedPCollectionProvider(PCollectionProvider<T> underlying) {
      this.underlying = underlying;
    }

    @Override
    public PCollection<T> materialize(Pipeline pipeline) {
      if (materialized == null || materialized.getPipeline() != pipeline) {
        materialized = underlying.materialize(pipeline);
      }
      return materialized;
    }

    @Override
    public void asUnbounded() {
      underlying.asUnbounded();
    }
  }

  @SafeVarargs
  static <T> PCollectionProvider<T> withParents(
      Function<Pipeline, PCollection<T>> factory, PCollectionProvider<?>... parents) {

    return new ParentNotifyingProvider<>(factory, Arrays.asList(parents));
  }

  static <T> PCollectionProvider<T> cached(PCollectionProvider<T> underlying) {
    return new CachedPCollectionProvider<>(underlying);
  }

  static <T> PCollectionProvider<T> fixedType(Function<Pipeline, PCollection<T>> factory) {
    return new ParentNotifyingProvider<>(factory, Collections.emptyList());
  }

  static <T> PCollectionProvider<T> boundedOrUnbounded(
      Function<Pipeline, PCollection<T>> boundedFactory,
      Function<Pipeline, PCollection<T>> unboundedFactory,
      boolean bounded) {

    return new PCollectionProvider<T>() {

      boolean isBounded = bounded;

      @Override
      public PCollection<T> materialize(Pipeline pipeline) {
        if (isBounded) {
          return boundedFactory.apply(pipeline);
        }
        return unboundedFactory.apply(pipeline);
      }

      @Override
      public void asUnbounded() {
        isBounded = false;
      }
    };
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline}.
   *
   * @param pipeline the pipeline to create to PCollection in
   * @return resulting PCollection
   */
  PCollection<T> materialize(Pipeline pipeline);

  /** Convert given materialization process to create unbounded PCollection, if possible. */
  void asUnbounded();
}
