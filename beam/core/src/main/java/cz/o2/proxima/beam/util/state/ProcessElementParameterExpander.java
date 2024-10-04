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

import static cz.o2.proxima.beam.util.state.MethodCallUtils.*;
import static cz.o2.proxima.beam.util.state.MethodCallUtils.projectArgs;

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.annotation.AnnotationDescription.ForLoadedAnnotation;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.description.type.TypeDescription.Generic.Builder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

interface ProcessElementParameterExpander {

  static ProcessElementParameterExpander of(
      DoFn<?, ?> doFn,
      Method processElement,
      ParameterizedType inputType,
      TupleTag<?> mainTag,
      Type outputType,
      Instant stateWriteInstant) {

    final LinkedHashMap<TypeId, Pair<Annotation, Type>> processArgs = extractArgs(processElement);
    final LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs =
        createWrapperArgs(inputType, outputType, processArgs.values());
    final List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> processArgsGenerators =
        projectArgs(wrapperArgs, processArgs, mainTag, outputType);

    return new ProcessElementParameterExpander() {
      @Override
      public List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs() {
        return new ArrayList<>(wrapperArgs.values());
      }

      @Override
      public Object[] getProcessElementArgs(Object[] wrapperArgs) {
        return fromGenerators(processArgsGenerators, wrapperArgs);
      }

      @Override
      public UnaryFunction<Object[], Boolean> getProcessFn() {
        return createProcessFn(wrapperArgs, doFn, processElement, stateWriteInstant);
      }
    };
  }

  /** Get arguments that must be declared by wrapper's call. */
  List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs();

  /**
   * Get parameters that should be passed to {@code @}ProcessElement from wrapper's
   * {@code @}ProcessElement
   */
  Object[] getProcessElementArgs(Object[] wrapperArgs);

  /** Get function to process elements and delegate to original DoFn. */
  UnaryFunction<Object[], Boolean> getProcessFn();

  private static UnaryFunction<Object[], Boolean> createProcessFn(
      LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs,
      DoFn<?, ?> doFn,
      Method method,
      Instant stateWriteInstant) {

    Map<String, BiConsumer<Object, StateValue>> stateUpdaterMap = getStateUpdaters(doFn);
    return new ProcessFn(stateWriteInstant, wrapperArgs, method, stateUpdaterMap);
  }

  private static int findParameter(Collection<TypeId> args, Predicate<TypeId> predicate) {
    int i = 0;
    for (TypeId t : args) {
      if (predicate.test(t)) {
        return i;
      }
      i++;
    }
    return -1;
  }

  static LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> createWrapperArgs(
      ParameterizedType inputType,
      Type outputType,
      Collection<Pair<Annotation, Type>> processArgs) {

    LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> res = new LinkedHashMap<>();
    processArgs.stream()
        .map(p -> transformProcessArg(inputType, p))
        .filter(p -> !p.getFirst().isOutput(outputType) && !p.getFirst().isTimestamp())
        .forEachOrdered(p -> res.put(p.getFirst(), p.getSecond()));

    // add @Timestamp
    AnnotationDescription timestampAnnotation =
        AnnotationDescription.Builder.ofType(DoFn.Timestamp.class).build();
    res.put(
        TypeId.of(timestampAnnotation),
        Pair.of(timestampAnnotation, TypeDescription.ForLoadedType.of(Instant.class)));
    // add @TimerId for flush timer
    AnnotationDescription timerAnnotation =
        AnnotationDescription.Builder.ofType(DoFn.TimerId.class)
            .define("value", ExternalStateExpander.EXPANDER_TIMER_NAME)
            .build();
    res.put(
        TypeId.of(timerAnnotation),
        Pair.of(timerAnnotation, TypeDescription.ForLoadedType.of(Timer.class)));

    // add @StateId for finished buffer
    AnnotationDescription finishedAnnotation =
        AnnotationDescription.Builder.ofType(DoFn.StateId.class)
            .define("value", ExternalStateExpander.EXPANDER_FLUSH_STATE_NAME)
            .build();
    res.put(
        TypeId.of(finishedAnnotation),
        Pair.of(
            finishedAnnotation,
            TypeDescription.Generic.Builder.parameterizedType(ValueState.class, Instant.class)
                .build()));

    // add @StateId for buffer
    AnnotationDescription stateAnnotation =
        AnnotationDescription.Builder.ofType(StateId.class)
            .define("value", ExternalStateExpander.EXPANDER_BUF_STATE_NAME)
            .build();
    res.put(
        TypeId.of(stateAnnotation),
        Pair.of(stateAnnotation, ExternalStateExpander.bagStateFromInputType(inputType)));

    // add MultiOutputReceiver
    TypeDescription receiver = ForLoadedType.of(MultiOutputReceiver.class);
    res.put(TypeId.of(receiver), Pair.of(null, receiver));
    return res;
  }

  static Pair<TypeId, Pair<AnnotationDescription, TypeDefinition>> transformProcessArg(
      ParameterizedType inputType, Pair<Annotation, Type> p) {

    TypeId typeId = p.getFirst() == null ? TypeId.of(p.getSecond()) : TypeId.of(p.getFirst());
    AnnotationDescription annotation =
        p.getFirst() != null ? ForLoadedAnnotation.of(p.getFirst()) : null;
    Generic parameterType = Builder.of(p.getSecond()).build();
    if (typeId.equals(
        TypeId.of(AnnotationDescription.Builder.ofType(DoFn.Element.class).build()))) {
      parameterType = getWrapperInputType(inputType);
    }
    return Pair.of(typeId, Pair.of(annotation, parameterType));
  }

  @Slf4j
  class ProcessFn implements UnaryFunction<Object[], Boolean> {
    private final int elementPos;
    private final Instant stateWriteInstant;
    private final LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs;
    private final Method method;
    private final Map<String, BiConsumer<Object, StateValue>> stateUpdaterMap;

    public ProcessFn(
        Instant stateWriteInstant,
        LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs,
        Method method,
        Map<String, BiConsumer<Object, StateValue>> stateUpdaterMap) {

      this.elementPos = findParameter(wrapperArgs.keySet(), TypeId::isElement);
      this.stateWriteInstant = stateWriteInstant;
      this.wrapperArgs = wrapperArgs;
      this.method = method;
      this.stateUpdaterMap = stateUpdaterMap;
      Preconditions.checkState(elementPos >= 0, "Missing @Element annotation on method %s", method);
    }

    @Override
    public Boolean apply(Object[] args) {
      @SuppressWarnings("unchecked")
      KV<?, StateOrInput<?>> elem = (KV<?, StateOrInput<?>>) args[elementPos];
      Instant ts = (Instant) args[args.length - 5];
      Timer flushTimer = (Timer) args[args.length - 4];
      @SuppressWarnings("unchecked")
      ValueState<Instant> finishedState = (ValueState<Instant>) args[args.length - 3];
      boolean isState = Objects.requireNonNull(elem.getValue(), "elem").isState();
      if (isState) {
        StateValue state = elem.getValue().getState();
        String stateName = state.getName();
        // find state accessor
        int statePos = findParameter(wrapperArgs.keySet(), a -> a.isState(stateName));
        Preconditions.checkArgument(
            statePos < method.getParameterCount(), "Missing state accessor for %s", stateName);
        Object stateAccessor = args[statePos];
        // find declaration of state to find coder
        BiConsumer<Object, StateValue> updater = stateUpdaterMap.get(stateName);
        Preconditions.checkArgument(
            updater != null, "Missing updater for state %s in %s", stateName, stateUpdaterMap);
        updater.accept(stateAccessor, state);
        return false;
      }
      Instant nextFlush = finishedState.read();
      if (nextFlush == null) {
        // set the initial timer
        flushTimer.set(stateWriteInstant);
      }
      boolean shouldBuffer =
          nextFlush == null /* we have not finished reading state */
              || nextFlush.isBefore(ts) /* the timestamp if after next flush */;
      if (shouldBuffer) {
        log.debug("Buffering element {} at {} with nextFlush {}", elem, ts, nextFlush);
        // store to state
        @SuppressWarnings("unchecked")
        BagState<TimestampedValue<KV<?, ?>>> buffer =
            (BagState<TimestampedValue<KV<?, ?>>>) args[args.length - 2];
        buffer.add(TimestampedValue.of(KV.of(elem.getKey(), elem.getValue().getInput()), ts));
        return false;
      }
      return true;
    }
  }
}
