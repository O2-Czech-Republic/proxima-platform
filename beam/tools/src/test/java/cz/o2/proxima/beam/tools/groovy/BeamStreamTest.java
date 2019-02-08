/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import cz.o2.proxima.tools.groovy.JavaTypedClosure;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamTest;
import cz.o2.proxima.tools.groovy.TestStreamProvider;
import groovy.lang.Closure;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TypeDescriptor;

public class BeamStreamTest extends StreamTest {

  public BeamStreamTest() {
    super(provider());
  }

  static TestStreamProvider provider() {
    return new TestStreamProvider() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> Stream<T> of(List<T> values) {
        Set<Class<?>> classes = values.stream()
            .map(Object::getClass).collect(Collectors.toSet());

        Preconditions.checkArgument(
            classes.size() == 1,
            "Please pass uniform object types, got " + classes);

        TypeDescriptor<T> typeDesc = TypeDescriptor.of(
            (Class) Iterables.getOnlyElement(classes));

        return injectTypeOf(
            new BeamStream<>(
                true,
                p -> p.apply(Create.of(values)).setTypeDescriptor(typeDesc)));
      }
    };
  }

  static <T> BeamStream<T> injectTypeOf(BeamStream<T> delegate) {
    return new BeamStream<T>(delegate.isBounded(), delegate.collection) {
      @Override
      <T> TypeDescriptor<T> typeOf(Closure<T> closure) {
        return getTypeOf(closure).orElseGet(() -> super.typeOf(closure));
      }

      @Override
      <X> BeamWindowedStream<X> windowed(
          PCollectionProvider<X> provider,
          WindowFn<? super X, ?> window) {

        return injectTypeOf(super.windowed(provider, window));
      }

      @Override
      <X> BeamStream<X> descendant(PCollectionProvider<X> provider) {
        return injectTypeOf(super.descendant(provider));
      }

    };
  }

  static <T> BeamWindowedStream<T> injectTypeOf(BeamWindowedStream<T> delegate) {
    return new BeamWindowedStream<T>(
        delegate.isBounded(), delegate.collection,
        delegate.getWindowing(), delegate.getMode()) {

      @Override
      <T> TypeDescriptor<T> typeOf(Closure<T> closure) {
        return getTypeOf(closure).orElseGet(() -> super.typeOf(closure));
      }

      @Override
      <X> BeamWindowedStream<X> windowed(
          PCollectionProvider<X> provider,
          WindowFn<? super X, ?> window) {

        return injectTypeOf(super.windowed(provider, window));
      }

      @Override
      <X> BeamWindowedStream<X> descendant(PCollectionProvider<X> provider) {
        return injectTypeOf(super.descendant(provider));
      }
    };
  }

  @SuppressWarnings("unchecked")
  static <T> Optional<TypeDescriptor<T>> getTypeOf(Closure<T> closure) {
    if (closure instanceof JavaTypedClosure) {
      return Optional.of(TypeDescriptor.of(((JavaTypedClosure) closure).getType()));
    }
    return Optional.empty();
  }


}
