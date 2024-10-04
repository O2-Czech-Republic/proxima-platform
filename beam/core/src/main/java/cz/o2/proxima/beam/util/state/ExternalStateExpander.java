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

import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.dynamic.DynamicType.Builder;
import net.bytebuddy.dynamic.DynamicType.Builder.MethodDefinition;
import net.bytebuddy.dynamic.DynamicType.Unloaded;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Composable;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteBuddyUtils;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.TypeToken;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@Slf4j
public class ExternalStateExpander {

  static final String EXPANDER_BUF_STATE_SPEC = "expanderBufStateSpec";
  static final String EXPANDER_BUF_STATE_NAME = "_expanderBuf";
  static final String EXPANDER_FLUSH_STATE_SPEC = "expanderFlushStateSpec";
  static final String EXPANDER_FLUSH_STATE_NAME = "_expanderFlush";
  static final String EXPANDER_TIMER_SPEC = "expanderTimerSpec";
  static final String EXPANDER_TIMER_NAME = "_expanderTimer";
  static final String DELEGATE_FIELD_NAME = "delegate";

  static final TupleTag<StateValue> STATE_TUPLE_TAG = new StateTupleTag() {};

  /**
   * Expand the given @{link Pipeline} to support external state store and restore
   *
   * @param pipeline the Pipeline to expand
   * @param inputs transform to read inputs
   * @param stateWriteInstant the instant at which write of the last state occurred
   * @param nextFlushInstantFn function that returns instant of next flush from current time
   * @param stateSink transform to store outputs
   */
  public static Pipeline expand(
      Pipeline pipeline,
      PTransform<PBegin, PCollection<KV<String, StateValue>>> inputs,
      Instant stateWriteInstant,
      UnaryFunction<Instant, Instant> nextFlushInstantFn,
      PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink) {

    validatePipeline(pipeline);
    pipeline.getCoderRegistry().registerCoderForClass(StateValue.class, StateValue.coder());
    PCollection<KV<String, StateValue>> inputsMaterialized = pipeline.apply(inputs);
    // replace all MultiParDos
    pipeline.replaceAll(
        Collections.singletonList(
            statefulParMultiDoOverride(inputsMaterialized, stateWriteInstant, nextFlushInstantFn)));
    // collect all StateValues
    List<Pair<String, PCollection<StateValue>>> stateValues = new ArrayList<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            if (node.getTransform() instanceof ParDo.MultiOutput<?, ?>) {
              node.getOutputs().entrySet().stream()
                  .filter(e -> e.getKey() instanceof StateTupleTag)
                  .map(Entry::getValue)
                  .findAny()
                  .ifPresent(
                      p ->
                          stateValues.add(
                              Pair.of(node.getFullName(), (PCollection<StateValue>) p)));
            }
          }
        });
    if (!stateValues.isEmpty()) {
      PCollectionList<KV<String, StateValue>> list = PCollectionList.empty(pipeline);
      for (Pair<String, PCollection<StateValue>> p : stateValues) {
        PCollection<KV<String, StateValue>> mapped = p.getSecond().apply(WithKeys.of(p.getFirst()));
        list = list.and(mapped);
      }
      list.apply(Flatten.pCollections()).apply(stateSink);
    }

    return pipeline;
  }

  private static void validatePipeline(Pipeline pipeline) {
    // check that all nodes have unique names
    Set<String> names = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {

          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            Preconditions.checkState(names.add(node.getFullName()));
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            Preconditions.checkState(names.add(node.getFullName()));
          }
        });
  }

  private static PTransformOverride statefulParMultiDoOverride(
      PCollection<KV<String, StateValue>> inputs,
      Instant stateWriteInstant,
      UnaryFunction<Instant, Instant> nextFlushInstantFn) {

    return PTransformOverride.of(
        application -> application.getTransform() instanceof ParDo.MultiOutput,
        parMultiDoReplacementFactory(inputs, stateWriteInstant, nextFlushInstantFn));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PTransformOverrideFactory<?, ?, ?> parMultiDoReplacementFactory(
      PCollection<KV<String, StateValue>> inputs,
      Instant stateWriteInstant,
      UnaryFunction<Instant, Instant> nextFlushInstantFn) {

    return new PTransformOverrideFactory<>() {
      @Override
      public PTransformReplacement getReplacementTransform(AppliedPTransform transform) {
        return replaceParMultiDo(transform, inputs, stateWriteInstant, nextFlushInstantFn);
      }

      @SuppressWarnings("unchecked")
      @Override
      public Map<PCollection<?>, ReplacementOutput> mapOutputs(Map outputs, POutput newOutput) {
        return ReplacementOutputs.tagged(outputs, newOutput);
      }
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PTransformReplacement<PInput, POutput> replaceParMultiDo(
      AppliedPTransform<PInput, POutput, ?> transform,
      PCollection<KV<String, StateValue>> inputs,
      Instant stateWriteInstant,
      UnaryFunction<Instant, Instant> nextFlushInstantFn) {

    ParDo.MultiOutput<PInput, POutput> rawTransform =
        (ParDo.MultiOutput<PInput, POutput>) (PTransform) transform.getTransform();
    DoFn<KV<?, ?>, ?> doFn = (DoFn) rawTransform.getFn();
    PInput pMainInput = getMainInput(transform);
    if (!DoFnSignatures.isStateful(doFn)) {
      return PTransformReplacement.of(pMainInput, (PTransform) transform.getTransform());
    }
    String transformName = transform.getFullName();
    PCollection<StateValue> transformInputs =
        inputs
            .apply(Filter.by(kv -> kv.getKey().equals(transformName)))
            .apply(MapElements.into(TypeDescriptor.of(StateValue.class)).via(KV::getValue));
    TupleTag<POutput> mainOutputTag = rawTransform.getMainOutputTag();
    return PTransformReplacement.of(
        pMainInput,
        transformedParDo(
            transformInputs,
            (DoFn) doFn,
            mainOutputTag,
            TupleTagList.of(
                transform.getOutputs().keySet().stream()
                    .filter(t -> !t.equals(mainOutputTag))
                    .collect(Collectors.toList())),
            stateWriteInstant,
            nextFlushInstantFn));
  }

  @SuppressWarnings("unchecked")
  private static <K, V, InputT extends KV<K, V>, OutputT>
      PTransform<PCollection<InputT>, PCollectionTuple> transformedParDo(
          PCollection<StateValue> transformInputs,
          DoFn<KV<K, V>, OutputT> doFn,
          TupleTag<OutputT> mainOutputTag,
          TupleTagList otherOutputs,
          Instant stateWriteInstant,
          UnaryFunction<Instant, Instant> nextFlushInstantFn) {

    return new PTransform<>() {
      @Override
      public PCollectionTuple expand(PCollection<InputT> input) {
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
        Coder<K> keyCoder = coder.getKeyCoder();
        Coder<V> valueCoder = coder.getValueCoder();
        TypeDescriptor<StateOrInput<V>> valueDescriptor =
            new TypeDescriptor<>(new TypeToken<StateOrInput<V>>() {}) {};
        PCollection<KV<K, StateOrInput<V>>> state =
            transformInputs
                .apply(
                    MapElements.into(
                            TypeDescriptors.kvs(
                                keyCoder.getEncodedTypeDescriptor(), valueDescriptor))
                        .via(
                            e ->
                                ExceptionUtils.uncheckedFactory(
                                    () ->
                                        KV.of(
                                            CoderUtils.decodeFromByteArray(keyCoder, e.getKey()),
                                            StateOrInput.state(e)))))
                .setCoder(KvCoder.of(keyCoder, StateOrInput.coder(valueCoder)));
        PCollection<KV<K, StateOrInput<V>>> inputs =
            input
                .apply(
                    MapElements.into(
                            TypeDescriptors.kvs(
                                keyCoder.getEncodedTypeDescriptor(), valueDescriptor))
                        .via(e -> KV.of(e.getKey(), StateOrInput.input(e.getValue()))))
                .setCoder(KvCoder.of(keyCoder, StateOrInput.coder(valueCoder)));
        PCollection<KV<K, StateOrInput<V>>> flattened =
            PCollectionList.of(state).and(inputs).apply(Flatten.pCollections());
        PCollectionTuple tuple =
            flattened.apply(
                ParDo.of(
                        transformedDoFn(
                            doFn,
                            (KvCoder<K, V>) input.getCoder(),
                            mainOutputTag,
                            stateWriteInstant,
                            nextFlushInstantFn))
                    .withOutputTags(mainOutputTag, otherOutputs.and(STATE_TUPLE_TAG)));
        PCollectionTuple res = PCollectionTuple.empty(input.getPipeline());
        for (Entry<TupleTag<Object>, PCollection<Object>> e :
            (Set<Entry<TupleTag<Object>, PCollection<Object>>>) (Set) tuple.getAll().entrySet()) {
          if (!e.getKey().equals(STATE_TUPLE_TAG)) {
            res = res.and(e.getKey(), e.getValue());
          }
        }
        return res;
      }
    };
  }

  @VisibleForTesting
  static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      DoFn<InputT, OutputT> transformedDoFn(
          DoFn<KV<K, V>, OutputT> doFn,
          KvCoder<K, V> inputCoder,
          TupleTag<OutputT> mainTag,
          Instant stateWriteInstant,
          UnaryFunction<Instant, Instant> nextFlushInstantFn) {

    @SuppressWarnings("unchecked")
    Class<? extends DoFn<KV<K, V>, OutputT>> doFnClass =
        (Class<? extends DoFn<KV<K, V>, OutputT>>) doFn.getClass();

    ClassLoadingStrategy<ClassLoader> strategy = ByteBuddyUtils.getClassLoadingStrategy(doFnClass);
    final String className =
        doFnClass.getName()
            + "$Expanded"
            + (Objects.hash(stateWriteInstant, nextFlushInstantFn) & Integer.MAX_VALUE);
    final ClassLoader classLoader = ExternalStateExpander.class.getClassLoader();
    try {
      @SuppressWarnings("unchecked")
      Class<? extends DoFn<InputT, OutputT>> aClass =
          (Class<? extends DoFn<InputT, OutputT>>) classLoader.loadClass(className);
      // class found, return instance
      return ExceptionUtils.uncheckedFactory(
          () -> aClass.getConstructor(doFnClass).newInstance(doFn));
    } catch (ClassNotFoundException e) {
      // class not found, create it
    }

    ByteBuddy buddy = new ByteBuddy();
    @SuppressWarnings("unchecked")
    ParameterizedType parameterizedSuperClass =
        getParameterizedDoFn((Class<DoFn<KV<K, V>, OutputT>>) doFn.getClass());
    ParameterizedType inputType =
        (ParameterizedType) parameterizedSuperClass.getActualTypeArguments()[0];
    Preconditions.checkArgument(
        inputType.getRawType().equals(KV.class),
        "Input type to stateful DoFn must be KV, go %s",
        inputType);

    Type outputType = parameterizedSuperClass.getActualTypeArguments()[1];
    Generic wrapperInput = getWrapperInputType(inputType);

    Generic doFnGeneric =
        Generic.Builder.parameterizedType(
                TypeDescription.ForLoadedType.of(DoFn.class),
                wrapperInput,
                TypeDescription.Generic.Builder.of(outputType).build())
            .build();
    @SuppressWarnings("unchecked")
    Builder<DoFn<InputT, OutputT>> builder =
        (Builder<DoFn<InputT, OutputT>>)
            buddy
                .subclass(doFnGeneric)
                .name(className)
                .defineField(DELEGATE_FIELD_NAME, doFnClass, Visibility.PRIVATE);
    builder = addStateAndTimers(doFnClass, inputType, builder);
    builder =
        builder
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(doFnClass)
            .intercept(
                addStateAndTimerValues(
                    doFn,
                    inputCoder,
                    MethodCall.invoke(
                            ExceptionUtils.uncheckedFactory(() -> DoFn.class.getConstructor()))
                        .andThen(FieldAccessor.ofField(DELEGATE_FIELD_NAME).setsArgumentAt(0))));

    builder =
        addProcessingMethods(
            doFn,
            inputType,
            inputCoder.getKeyCoder(),
            mainTag,
            outputType,
            stateWriteInstant,
            nextFlushInstantFn,
            buddy,
            builder);
    Unloaded<DoFn<InputT, OutputT>> dynamicClass = builder.make();
    return ExceptionUtils.uncheckedFactory(
        () ->
            dynamicClass
                .load(null, strategy)
                .getLoaded()
                .getDeclaredConstructor(doFnClass)
                .newInstance(doFn));
  }

  private static <K, V, InputT extends KV<K, V>, OutputT> Implementation addStateAndTimerValues(
      DoFn<InputT, OutputT> doFn, Coder<? extends KV<K, V>> inputCoder, Composable delegate) {

    List<Class<? extends Annotation>> acceptable = Arrays.asList(StateId.class, TimerId.class);
    @SuppressWarnings("unchecked")
    Class<? extends DoFn<InputT, OutputT>> doFnClass =
        (Class<? extends DoFn<InputT, OutputT>>) doFn.getClass();
    for (Field f : doFnClass.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers())
          && acceptable.stream().anyMatch(a -> f.getAnnotation(a) != null)) {
        f.setAccessible(true);
        Object value = ExceptionUtils.uncheckedFactory(() -> f.get(doFn));
        delegate = delegate.andThen(FieldAccessor.ofField(f.getName()).setsValue(value));
      }
    }
    delegate =
        delegate
            .andThen(
                FieldAccessor.ofField(EXPANDER_BUF_STATE_SPEC)
                    .setsValue(StateSpecs.bag(TimestampedValueCoder.of(inputCoder))))
            .andThen(FieldAccessor.ofField(EXPANDER_FLUSH_STATE_SPEC).setsValue(StateSpecs.value()))
            .andThen(
                FieldAccessor.ofField(EXPANDER_TIMER_SPEC)
                    .setsValue(TimerSpecs.timer(TimeDomain.EVENT_TIME)));
    return delegate;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <InputT, OutputT> ParameterizedType getParameterizedDoFn(
      Class<? extends DoFn<InputT, OutputT>> doFnClass) {

    Type type = doFnClass.getGenericSuperclass();
    if (type instanceof ParameterizedType) {
      return (ParameterizedType) type;
    }
    if (doFnClass.getSuperclass().isAssignableFrom(DoFn.class)) {
      return getParameterizedDoFn((Class) doFnClass.getGenericSuperclass());
    }
    throw new IllegalStateException("Cannot get parameterized type of " + doFnClass);
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addProcessingMethods(
          DoFn<KV<K, V>, OutputT> doFn,
          ParameterizedType inputType,
          Coder<K> keyCoder,
          TupleTag<OutputT> mainTag,
          Type outputType,
          Instant stateWriteInstant,
          UnaryFunction<Instant, Instant> nextFlushInstantFn,
          ByteBuddy buddy,
          Builder<DoFn<InputT, OutputT>> builder) {

    builder = addProcessingMethod(doFn, DoFn.Setup.class, builder);
    builder = addProcessingMethod(doFn, DoFn.StartBundle.class, builder);
    builder =
        addProcessElementMethod(
            doFn, inputType, mainTag, outputType, stateWriteInstant, buddy, builder);
    builder = addProcessingMethod(doFn, DoFn.FinishBundle.class, builder);
    builder = addProcessingMethod(doFn, DoFn.Teardown.class, builder);
    builder = addOnWindowExpirationMethod(doFn, inputType, mainTag, buddy, builder);
    builder = addProcessingMethod(doFn, DoFn.GetInitialRestriction.class, builder);
    builder = addProcessingMethod(doFn, DoFn.SplitRestriction.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetRestrictionCoder.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetWatermarkEstimatorStateCoder.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetInitialWatermarkEstimatorState.class, builder);
    builder = addProcessingMethod(doFn, DoFn.NewWatermarkEstimator.class, builder);
    builder = addProcessingMethod(doFn, DoFn.NewTracker.class, builder);
    builder = addProcessingMethod(doFn, DoFn.OnTimer.class, builder);
    builder =
        addTimerFlushMethod(
            doFn,
            inputType,
            keyCoder,
            mainTag,
            STATE_TUPLE_TAG,
            outputType,
            nextFlushInstantFn,
            buddy,
            builder);
    return builder;
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addProcessElementMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          ParameterizedType inputType,
          TupleTag<OutputT> mainTag,
          Type outputType,
          Instant stateWriteInstant,
          ByteBuddy buddy,
          Builder<DoFn<InputT, OutputT>> builder) {

    Class<? extends Annotation> annotation = ProcessElement.class;
    Method method = findMethod(doFn, annotation);
    if (method != null) {
      ProcessElementParameterExpander expander =
          ProcessElementParameterExpander.of(
              doFn, method, inputType, mainTag, outputType, stateWriteInstant);
      List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
      MethodDefinition<DoFn<InputT, OutputT>> methodDefinition =
          builder
              .defineMethod(method.getName(), method.getReturnType(), Visibility.PUBLIC)
              .withParameters(
                  wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
              .intercept(
                  MethodDelegation.to(
                      new ProcessElementInterceptor<>(doFn, expander, method, buddy)));

      for (int i = 0; i < wrapperArgs.size(); i++) {
        Pair<AnnotationDescription, TypeDefinition> arg = wrapperArgs.get(i);
        if (arg.getFirst() != null) {
          methodDefinition = methodDefinition.annotateParameter(i, arg.getFirst());
        }
      }
      return methodDefinition.annotateMethod(
          AnnotationDescription.Builder.ofType(annotation).build());
    }
    return builder;
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addOnWindowExpirationMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          ParameterizedType inputType,
          TupleTag<OutputT> mainTag,
          ByteBuddy buddy,
          Builder<DoFn<InputT, OutputT>> builder) {

    Class<? extends Annotation> annotation = DoFn.OnWindowExpiration.class;
    @Nullable Method onWindowExpirationMethod = findMethod(doFn, annotation);
    Method processElementMethod = findMethod(doFn, DoFn.ProcessElement.class);
    Type outputType = doFn.getOutputTypeDescriptor().getType();
    if (processElementMethod != null) {
      OnWindowParameterExpander expander =
          OnWindowParameterExpander.of(
              inputType, processElementMethod, onWindowExpirationMethod, mainTag, outputType);
      List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
      MethodDefinition<DoFn<InputT, OutputT>> methodDefinition =
          builder
              .defineMethod(
                  onWindowExpirationMethod == null
                      ? "_onWindowExpiration"
                      : onWindowExpirationMethod.getName(),
                  void.class,
                  Visibility.PUBLIC)
              .withParameters(
                  wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
              .intercept(
                  MethodDelegation.to(
                      new OnWindowExpirationInterceptor<>(
                          doFn, processElementMethod, onWindowExpirationMethod, expander, buddy)));

      // retrieve parameter annotations and apply them
      for (int i = 0; i < wrapperArgs.size(); i++) {
        AnnotationDescription ann = wrapperArgs.get(i).getFirst();
        if (ann != null) {
          methodDefinition = methodDefinition.annotateParameter(i, ann);
        }
      }
      return methodDefinition.annotateMethod(
          AnnotationDescription.Builder.ofType(annotation).build());
    }
    return builder;
  }

  private static <K, V, OutputT, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, OutputT>> addTimerFlushMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          ParameterizedType inputType,
          Coder<K> keyCoder,
          TupleTag<?> mainTag,
          TupleTag<StateValue> stateTag,
          Type outputType,
          UnaryFunction<Instant, Instant> nextFlushInstantFn,
          ByteBuddy buddy,
          Builder<DoFn<InputT, OutputT>> builder) {

    Method processElement = findMethod(doFn, ProcessElement.class);
    FlushTimerParameterExpander expander =
        FlushTimerParameterExpander.of(doFn, inputType, processElement, mainTag, outputType);
    List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
    MethodDefinition<DoFn<InputT, OutputT>> methodDefinition =
        builder
            .defineMethod("expanderFlushTimer", void.class, Visibility.PUBLIC)
            .withParameters(wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
            .intercept(
                MethodDelegation.to(
                    new FlushTimerInterceptor<>(
                        doFn,
                        processElement,
                        expander,
                        keyCoder,
                        stateTag,
                        nextFlushInstantFn,
                        buddy)));
    int i = 0;
    for (Pair<AnnotationDescription, TypeDefinition> p : wrapperArgs) {
      if (p.getFirst() != null) {
        methodDefinition = methodDefinition.annotateParameter(i, p.getFirst());
      }
      i++;
    }
    return methodDefinition.annotateMethod(
        AnnotationDescription.Builder.ofType(DoFn.OnTimer.class)
            .define("value", EXPANDER_TIMER_NAME)
            .build());
  }

  private static <K, V, OutputT> Method findMethod(
      DoFn<KV<K, V>, OutputT> doFn, Class<? extends Annotation> annotation) {

    return Iterables.getOnlyElement(
        Arrays.stream(doFn.getClass().getMethods())
            .filter(m -> m.getAnnotation(annotation) != null)
            .collect(Collectors.toList()),
        null);
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT, T extends Annotation>
      Builder<DoFn<InputT, OutputT>> addProcessingMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          Class<T> annotation,
          Builder<DoFn<InputT, OutputT>> builder) {

    Method method = findMethod(doFn, annotation);
    if (method != null) {
      MethodDefinition<DoFn<InputT, OutputT>> methodDefinition =
          builder
              .defineMethod(method.getName(), method.getReturnType(), Visibility.PUBLIC)
              .withParameters(method.getGenericParameterTypes())
              .intercept(MethodCall.invoke(method).onField(DELEGATE_FIELD_NAME).withAllArguments());

      // retrieve parameter annotations and apply them
      Annotation[][] parameterAnnotations = method.getParameterAnnotations();
      for (int i = 0; i < parameterAnnotations.length; i++) {
        for (Annotation paramAnnotation : parameterAnnotations[i]) {
          methodDefinition = methodDefinition.annotateParameter(i, paramAnnotation);
        }
      }
      return methodDefinition.annotateMethod(
          AnnotationDescription.Builder.ofType(annotation).build());
    }
    return builder;
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addStateAndTimers(
          Class<? extends DoFn<KV<K, V>, OutputT>> doFnClass,
          ParameterizedType inputType,
          Builder<DoFn<InputT, OutputT>> builder) {

    builder = cloneFields(doFnClass, StateId.class, builder);
    builder = cloneFields(doFnClass, TimerId.class, builder);
    builder = addBufferingStatesAndTimer(inputType, builder);
    return builder;
  }

  /** Add state that buffers inputs until we process all state updates. */
  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addBufferingStatesAndTimer(
          ParameterizedType inputType, Builder<DoFn<InputT, OutputT>> builder) {

    // type: StateSpec<BagState<KV<K, V>>>
    Generic bufStateSpecFieldType =
        Generic.Builder.parameterizedType(
                TypeDescription.ForLoadedType.of(StateSpec.class), bagStateFromInputType(inputType))
            .build();
    // type: StateSpec<ValueState<Instant>>
    Generic finishedStateSpecFieldType =
        Generic.Builder.parameterizedType(
                TypeDescription.ForLoadedType.of(StateSpec.class),
                Generic.Builder.parameterizedType(ValueState.class, Instant.class).build())
            .build();

    Generic timerSpecFieldType = Generic.Builder.of(TimerSpec.class).build();

    builder =
        defineStateField(
            builder,
            bufStateSpecFieldType,
            DoFn.StateId.class,
            EXPANDER_BUF_STATE_SPEC,
            EXPANDER_BUF_STATE_NAME);
    builder =
        defineStateField(
            builder,
            finishedStateSpecFieldType,
            DoFn.StateId.class,
            EXPANDER_FLUSH_STATE_SPEC,
            EXPANDER_FLUSH_STATE_NAME);
    builder =
        defineStateField(
            builder,
            timerSpecFieldType,
            DoFn.TimerId.class,
            EXPANDER_TIMER_SPEC,
            EXPANDER_TIMER_NAME);

    return builder;
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> defineStateField(
          Builder<DoFn<InputT, OutputT>> builder,
          Generic stateSpecFieldType,
          Class<? extends Annotation> annotation,
          String fieldName,
          String name) {

    return builder
        .defineField(
            fieldName,
            stateSpecFieldType,
            Visibility.PUBLIC.getMask() + FieldManifestation.FINAL.getMask())
        .annotateField(
            AnnotationDescription.Builder.ofType(annotation).define("value", name).build());
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT, T extends Annotation>
      Builder<DoFn<InputT, OutputT>> cloneFields(
          Class<? extends DoFn<KV<K, V>, OutputT>> doFnClass,
          Class<T> annotationClass,
          Builder<DoFn<InputT, OutputT>> builder) {

    for (Field f : doFnClass.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers()) && f.getAnnotation(annotationClass) != null) {
        builder =
            builder
                .defineField(f.getName(), f.getGenericType(), f.getModifiers())
                .annotateField(f.getDeclaredAnnotations());
      }
    }
    return builder;
  }

  private static PInput getMainInput(AppliedPTransform<PInput, POutput, ?> transform) {
    Map<TupleTag<?>, PCollection<?>> mainInputs = transform.getMainInputs();
    if (mainInputs.size() == 1) {
      return Iterables.getOnlyElement(mainInputs.values());
    }
    return asTuple(mainInputs);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static @NonNull PInput asTuple(Map<TupleTag<?>, PCollection<?>> mainInputs) {
    Preconditions.checkArgument(!mainInputs.isEmpty());
    PCollectionTuple res = null;
    for (Map.Entry<TupleTag<?>, PCollection<?>> e : mainInputs.entrySet()) {
      if (res == null) {
        res = PCollectionTuple.of((TupleTag) e.getKey(), e.getValue());
      } else {
        res = res.and((TupleTag) e.getKey(), e.getValue());
      }
    }
    return Objects.requireNonNull(res);
  }

  private static class ProcessElementInterceptor<K, V> {

    private final DoFn<KV<K, V>, ?> doFn;
    private final ProcessElementParameterExpander expander;
    private final UnaryFunction<Object[], Boolean> processFn;
    private final VoidMethodInvoker<Object> invoker;

    ProcessElementInterceptor(
        DoFn<KV<K, V>, ?> doFn,
        ProcessElementParameterExpander expander,
        Method process,
        ByteBuddy buddy) {

      this.doFn = doFn;
      this.expander = expander;
      this.processFn = expander.getProcessFn();
      this.invoker = ExceptionUtils.uncheckedFactory(() -> VoidMethodInvoker.of(process, buddy));
    }

    @RuntimeType
    public void intercept(
        @This DoFn<KV<V, StateOrInput<V>>, ?> proxy, @AllArguments Object[] allArgs) {

      if (processFn.apply(allArgs)) {
        Object[] methodArgs = expander.getProcessElementArgs(allArgs);
        ExceptionUtils.unchecked(() -> invoker.invoke(doFn, methodArgs));
      }
    }
  }

  private static class OnWindowExpirationInterceptor<K, V> {
    private final DoFn<KV<K, V>, ?> doFn;
    private final VoidMethodInvoker<DoFn<KV<K, V>, ?>> processElement;
    private final @Nullable VoidMethodInvoker<DoFn<KV<K, V>, ?>> onWindowExpiration;
    private final OnWindowParameterExpander expander;

    public OnWindowExpirationInterceptor(
        DoFn<KV<K, V>, ?> doFn,
        Method processElementMethod,
        @Nullable Method onWindowExpirationMethod,
        OnWindowParameterExpander expander,
        ByteBuddy buddy) {

      this.doFn = doFn;
      this.processElement =
          ExceptionUtils.uncheckedFactory(() -> VoidMethodInvoker.of(processElementMethod, buddy));
      this.onWindowExpiration =
          onWindowExpirationMethod == null
              ? null
              : ExceptionUtils.uncheckedFactory(
                  () -> VoidMethodInvoker.of(onWindowExpirationMethod, buddy));
      this.expander = expander;
    }

    @RuntimeType
    public void intercept(
        @This DoFn<KV<V, StateOrInput<V>>, ?> proxy, @AllArguments Object[] allArgs) {

      @SuppressWarnings("unchecked")
      BagState<TimestampedValue<KV<?, ?>>> buf =
          (BagState<TimestampedValue<KV<?, ?>>>) allArgs[allArgs.length - 1];
      Iterable<TimestampedValue<KV<?, ?>>> buffered = buf.read();
      // feed all data to @ProcessElement
      for (TimestampedValue<KV<?, ?>> kv : buffered) {
        ExceptionUtils.unchecked(
            () -> processElement.invoke(doFn, expander.getProcessElementArgs(kv, allArgs)));
      }
      // invoke onWindowExpiration
      if (onWindowExpiration != null) {
        ExceptionUtils.unchecked(
            () -> onWindowExpiration.invoke(doFn, expander.getOnWindowExpirationArgs(allArgs)));
      }
    }
  }

  private static class FlushTimerInterceptor<K, V> {

    private final DoFn<KV<K, V>, ?> doFn;
    private final LinkedHashMap<String, BiFunction<Object, byte[], Iterable<StateValue>>>
        stateReaders;
    private final VoidMethodInvoker<Object> processElementMethod;
    private final FlushTimerParameterExpander expander;
    private final Coder<K> keyCoder;
    private final TupleTag<StateValue> stateTag;
    private final UnaryFunction<Instant, Instant> nextFlushInstantFn;

    FlushTimerInterceptor(
        DoFn<KV<K, V>, ?> doFn,
        Method processElementMethod,
        FlushTimerParameterExpander expander,
        Coder<K> keyCoder,
        TupleTag<StateValue> stateTag,
        UnaryFunction<Instant, Instant> nextFlushInstantFn,
        ByteBuddy buddy) {

      this.doFn = doFn;
      this.stateReaders = getStateReaders(doFn);
      this.processElementMethod =
          ExceptionUtils.uncheckedFactory(() -> VoidMethodInvoker.of(processElementMethod, buddy));
      this.expander = expander;
      this.keyCoder = keyCoder;
      this.stateTag = stateTag;
      this.nextFlushInstantFn = nextFlushInstantFn;
    }

    @RuntimeType
    public void intercept(@This DoFn<KV<V, StateOrInput<V>>, ?> doFn, @AllArguments Object[] args) {
      Instant now = (Instant) args[args.length - 6];
      @SuppressWarnings("unchecked")
      K key = (K) args[args.length - 5];
      @SuppressWarnings("unchecked")
      ValueState<Instant> nextFlushState = (ValueState<Instant>) args[args.length - 3];
      Timer flushTimer = (Timer) args[args.length - 4];
      Instant nextFlush = nextFlushInstantFn.apply(now);
      Instant lastFlush = nextFlushState.read();
      boolean isNextScheduled =
          nextFlush != null && nextFlush.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE);
      if (isNextScheduled) {
        flushTimer.set(nextFlush);
        nextFlushState.write(nextFlush);
      }
      @SuppressWarnings("unchecked")
      BagState<TimestampedValue<KV<K, V>>> bufState =
          (BagState<TimestampedValue<KV<K, V>>>) args[args.length - 2];
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<TimestampedValue<KV<K, V>>> pushedBackElements =
          processBuffer(args, (Iterable) bufState.read(), MoreObjects.firstNonNull(lastFlush, now));
      bufState.clear();
      // if we have already processed state data
      if (lastFlush != null) {
        MultiOutputReceiver outputReceiver = (MultiOutputReceiver) args[args.length - 1];
        OutputReceiver<StateValue> output = outputReceiver.get(stateTag);
        byte[] keyBytes =
            ExceptionUtils.uncheckedFactory(() -> CoderUtils.encodeToByteArray(keyCoder, key));
        int i = 0;
        for (BiFunction<Object, byte[], Iterable<StateValue>> f : stateReaders.values()) {
          Object accessor = args[i++];
          Iterable<StateValue> values = f.apply(accessor, keyBytes);
          values.forEach(output::output);
        }
      }
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<TimestampedValue<KV<K, V>>> remaining =
          processBuffer(
              args,
              (List) pushedBackElements,
              MoreObjects.firstNonNull(nextFlush, BoundedWindow.TIMESTAMP_MAX_VALUE));
      remaining.forEach(bufState::add);
    }

    private List<TimestampedValue<KV<K, V>>> processBuffer(
        Object[] args, Iterable<TimestampedValue<KV<?, ?>>> buffer, Instant maxTs) {

      List<TimestampedValue<KV<?, ?>>> pushedBackElements = new ArrayList<>();
      buffer.forEach(
          kv -> {
            if (kv.getTimestamp().isBefore(maxTs)) {
              Object[] processArgs = expander.getProcessElementArgs(kv, args);
              ExceptionUtils.unchecked(() -> processElementMethod.invoke(this.doFn, processArgs));
            } else {
              // return back to buffer
              log.debug("Returning element {} to flush buffer", kv);
              pushedBackElements.add(kv);
            }
          });
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<TimestampedValue<KV<K, V>>> res = (List) pushedBackElements;
      return res;
    }
  }

  static Generic bagStateFromInputType(ParameterizedType inputType) {
    return Generic.Builder.parameterizedType(
            TypeDescription.ForLoadedType.of(BagState.class),
            Generic.Builder.parameterizedType(TimestampedValue.class, inputType).build())
        .build();
  }

  private static class StateTupleTag extends TupleTag<StateValue> {}

  // do not construct
  private ExternalStateExpander() {}
}
