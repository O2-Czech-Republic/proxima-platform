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

import static cz.o2.proxima.beam.util.state.ExpandContext.*;
import static cz.o2.proxima.beam.util.state.MethodCallUtils.*;

import cz.o2.proxima.core.functional.BiFunction;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

interface FlushTimerParameterExpander extends Serializable {

  static FlushTimerParameterExpander of(
      DoFn<?, ?> doFn,
      ParameterizedType inputType,
      Method processElement,
      TupleTag<?> mainTag,
      Type outputType) {

    return new FlushTimerParameterExpanderImpl(
        extractArgs(processElement), createWrapperArgs(doFn, inputType), mainTag, outputType);
  }

  private static LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>>
      createWrapperArgs(DoFn<?, ?> doFn, ParameterizedType inputType) {

    List<Pair<Annotation, Type>> states =
        Arrays.stream(doFn.getClass().getDeclaredFields())
            .filter(f -> f.getAnnotation(DoFn.StateId.class) != null)
            .map(
                f -> {
                  Preconditions.checkArgument(
                      f.getGenericType() instanceof ParameterizedType,
                      "Field %s has invalid type %s",
                      f.getName(),
                      f.getGenericType());
                  return Pair.of(
                      (Annotation) f.getAnnotation(DoFn.StateId.class),
                      ((ParameterizedType) f.getGenericType()).getActualTypeArguments()[0]);
                })
            .collect(Collectors.toList());

    List<Pair<AnnotationDescription, TypeDefinition>> types =
        states.stream()
            .map(
                p ->
                    Pair.of(
                        (AnnotationDescription)
                            AnnotationDescription.ForLoadedAnnotation.of(p.getFirst()),
                        (TypeDefinition) TypeDescription.Generic.Builder.of(p.getSecond()).build()))
            .collect(Collectors.toList());
    // add parameter for timestamp, key, timer, state and output
    types.add(
        Pair.of(
            AnnotationDescription.Builder.ofType(DoFn.Timestamp.class).build(),
            TypeDescription.ForLoadedType.of(Instant.class)));
    types.add(
        Pair.of(
            AnnotationDescription.Builder.ofType(DoFn.Key.class).build(),
            TypeDescription.Generic.Builder.of(inputType.getActualTypeArguments()[0]).build()));
    types.add(
        Pair.of(
            AnnotationDescription.Builder.ofType(DoFn.TimerId.class)
                .define("value", EXPANDER_TIMER_NAME)
                .build(),
            TypeDescription.ForLoadedType.of(Timer.class)));
    types.add(
        Pair.of(
            AnnotationDescription.Builder.ofType(DoFn.StateId.class)
                .define("value", EXPANDER_FLUSH_STATE_NAME)
                .build(),
            TypeDescription.Generic.Builder.parameterizedType(ValueState.class, Instant.class)
                .build()));
    types.add(
        Pair.of(
            AnnotationDescription.Builder.ofType(DoFn.StateId.class)
                .define("value", EXPANDER_BUF_STATE_NAME)
                .build(),
            bagStateFromInputType(inputType)));
    types.add(Pair.of(null, TypeDescription.ForLoadedType.of(DoFn.MultiOutputReceiver.class)));

    LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> res = new LinkedHashMap<>();
    types.forEach(
        p -> {
          TypeId id = p.getFirst() == null ? TypeId.of(p.getSecond()) : TypeId.of(p.getFirst());
          res.put(id, p);
        });
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

  class FlushTimerParameterExpanderImpl implements FlushTimerParameterExpander {

    final transient LinkedHashMap<TypeId, Pair<Annotation, Type>> processArgs;
    final transient LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs;
    final List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> processArgsGenerators;

    private FlushTimerParameterExpanderImpl(
        LinkedHashMap<TypeId, Pair<Annotation, Type>> processArgs,
        LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs,
        TupleTag<?> mainTag,
        Type outputType) {

      this.processArgs = processArgs;
      this.wrapperArgs = wrapperArgs;
      processArgsGenerators = projectArgs(wrapperArgs, processArgs, mainTag, outputType);
    }

    @Override
    public List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs() {
      return new ArrayList<>(wrapperArgs.values());
    }

    @Override
    public Object[] getProcessElementArgs(TimestampedValue<KV<?, ?>> input, Object[] wrapperArgs) {
      return fromGenerators(input, processArgsGenerators, wrapperArgs);
    }
  }
}
