/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.util.state;

import static cz.o2.proxima.beam.util.state.ExternalStateExpander.bagStateFromInputType;
import static cz.o2.proxima.beam.util.state.MethodCallUtils.*;

import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.annotation.AnnotationDescription.Builder;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

interface OnWindowParameterExpander {

  static OnWindowParameterExpander of(
      ParameterizedType inputType,
      Method processElement,
      @Nullable Method onWindowExpiration,
      TupleTag<?> mainTag,
      Type outputType) {

    final LinkedHashMap<TypeId, Pair<Annotation, Type>> processArgs = extractArgs(processElement);
    final LinkedHashMap<TypeId, Pair<Annotation, Type>> onWindowArgs =
        extractArgs(onWindowExpiration);
    final List<Pair<Annotation, Type>> wrapperArgList =
        createWrapperArgList(processArgs, onWindowArgs);
    final LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs =
        createWrapperArgs(inputType, wrapperArgList);
    final List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> processArgsGenerators =
        projectArgs(wrapperArgs, processArgs, mainTag, outputType);
    final List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> windowArgsGenerators =
        projectArgs(wrapperArgs, onWindowArgs, mainTag, outputType);

    return new OnWindowParameterExpander() {
      @Override
      public List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs() {
        return new ArrayList<>(wrapperArgs.values());
      }

      @Override
      public Object[] getProcessElementArgs(
          TimestampedValue<KV<?, ?>> input, Object[] wrapperArgs) {
        return fromGenerators(input, processArgsGenerators, wrapperArgs);
      }

      @Override
      public Object[] getOnWindowExpirationArgs(Object[] wrapperArgs) {
        return fromGenerators(null, windowArgsGenerators, wrapperArgs);
      }
    };
  }

  static List<Pair<Annotation, Type>> createWrapperArgList(
      LinkedHashMap<TypeId, Pair<Annotation, Type>> processArgs,
      LinkedHashMap<TypeId, Pair<Annotation, Type>> onWindowArgs) {

    Set<TypeId> union = new HashSet<>(Sets.union(processArgs.keySet(), onWindowArgs.keySet()));
    // @Element is not supported by @OnWindowExpiration
    union.remove(TypeId.of(AnnotationDescription.Builder.ofType(DoFn.Element.class).build()));
    return union.stream()
        .map(
            t -> {
              Pair<Annotation, Type> processPair = processArgs.get(t);
              Pair<Annotation, Type> windowPair = onWindowArgs.get(t);
              Type argType = MoreObjects.firstNonNull(processPair, windowPair).getSecond();
              Annotation processAnnotation =
                  Optional.ofNullable(processPair).map(Pair::getFirst).orElse(null);
              Annotation windowAnnotation =
                  Optional.ofNullable(windowPair).map(Pair::getFirst).orElse(null);
              Preconditions.checkState(
                  processPair == null
                      || windowPair == null
                      || processAnnotation == windowAnnotation
                      || processAnnotation.equals(windowAnnotation));
              return Pair.of(processAnnotation, argType);
            })
        .collect(Collectors.toList());
  }

  static LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> createWrapperArgs(
      ParameterizedType inputType, List<Pair<Annotation, Type>> wrapperArgList) {

    LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> res = new LinkedHashMap<>();
    wrapperArgList.stream()
        .map(
            p ->
                Pair.of(
                    p.getFirst() == null ? TypeId.of(p.getSecond()) : TypeId.of(p.getFirst()),
                    Pair.of(
                        p.getFirst() != null
                            ? (AnnotationDescription)
                                AnnotationDescription.ForLoadedAnnotation.of(p.getFirst())
                            : null,
                        (TypeDefinition)
                            TypeDescription.Generic.Builder.of(p.getSecond()).build())))
        .forEachOrdered(p -> res.put(p.getFirst(), p.getSecond()));

    // add @StateId for buffer
    AnnotationDescription buffer =
        Builder.ofType(StateId.class)
            .define("value", ExternalStateExpander.EXPANDER_BUF_STATE_NAME)
            .build();
    res.put(TypeId.of(buffer), Pair.of(buffer, bagStateFromInputType(inputType)));
    return res;
  }

  /**
   * Get arguments that must be declared by wrapper's call for both {@code @}ProcessElement and
   * {@code @}OnWindowExpiration be callable.
   */
  List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs();

  /**
   * Get parameters that should be passed to {@code @}ProcessElement from wrapper's
   * {@code @}OnWindowExpiration
   */
  Object[] getProcessElementArgs(TimestampedValue<KV<?, ?>> input, Object[] wrapperArgs);

  /** Get parameters that should be passed to {@code @}OnWindowExpiration from wrapper's call. */
  Object[] getOnWindowExpirationArgs(Object[] wrapperArgs);
}
