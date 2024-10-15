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

import static cz.o2.proxima.beam.util.state.MethodCallUtils.getStateReaders;
import static cz.o2.proxima.beam.util.state.MethodCallUtils.getWrapperInputType;

import cz.o2.proxima.beam.util.state.MethodCallUtils.VoidMethodInvoker;
import cz.o2.proxima.core.functional.BiFunction;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
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
import net.bytebuddy.dynamic.DynamicType.Loaded;
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
class ExpandContext {

  static final String EXPANDER_BUF_STATE_SPEC = "expanderBufStateSpec";
  static final String EXPANDER_BUF_STATE_NAME = "_expanderBuf";
  static final String EXPANDER_FLUSH_STATE_SPEC = "expanderFlushStateSpec";
  static final String EXPANDER_FLUSH_STATE_NAME = "_expanderFlush";
  static final String EXPANDER_TIMER_SPEC = "expanderTimerSpec";
  static final String EXPANDER_TIMER_NAME = "_expanderTimer";
  static final String DELEGATE_FIELD_NAME = "delegate";
  static final String PROCESS_ELEMENT_INTERCEPTOR_FIELD_NAME = "__processElementInterceptor";
  static final String ON_WINDOW_INTERCEPTOR_FIELD_NAME = "__onWindowInterceptor";
  static final String FLUSH_TIMER_INTERCEPTOR_FIELD_NAME = "__flushTimerInterceptor";

  static final TupleTag<StateValue> STATE_TUPLE_TAG = new StateTupleTag() {};

  private static class StateTupleTag extends TupleTag<StateValue> {}

  @Getter
  class DoFnExpandContext<K, V> {

    private final DoFn<KV<K, V>, ?> doFn;
    private final TupleTag<Object> mainTag;
    private final KvCoder<K, V> inputCoder;
    private final Coder<K> keyCoder;

    private final Class<? extends DoFn<KV<K, V>, ?>> doFnClass;

    private final ParameterizedType inputType;
    private final Type outputType;
    private final Generic doFnGeneric;

    private final Method processElement;
    private final @Nullable Method onWindowMethod;

    private final FlushTimerParameterExpander flushExpander;
    private final OnWindowParameterExpander onWindowExpander;
    private final ProcessElementParameterExpander processElementExpander;

    private final FlushTimerInterceptor<K, V> flushTimerInterceptor;
    private final OnWindowExpirationInterceptor<K, V> onWindowExpirationInterceptor;
    private final ProcessElementInterceptor<K, V> processElementInterceptor;

    @SuppressWarnings("unchecked")
    DoFnExpandContext(DoFn<KV<K, V>, ?> doFn, KvCoder<K, V> inputCoder, TupleTag<Object> mainTag)
        throws InvocationTargetException,
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException {

      this.doFn = doFn;
      this.inputCoder = inputCoder;
      this.mainTag = mainTag;
      this.keyCoder = inputCoder.getKeyCoder();
      this.doFnClass = (Class<? extends DoFn<KV<K, V>, ?>>) doFn.getClass();
      this.processElement = findMethod(doFn, DoFn.ProcessElement.class);
      this.onWindowMethod = findMethod(doFn, DoFn.OnWindowExpiration.class);

      ParameterizedType parameterizedSuperClass = getParameterizedDoFn(doFnClass);
      this.inputType = (ParameterizedType) parameterizedSuperClass.getActualTypeArguments()[0];
      Preconditions.checkArgument(
          inputType.getRawType().equals(KV.class),
          "Input type to stateful DoFn must be KV, go %s",
          inputType);

      this.outputType = parameterizedSuperClass.getActualTypeArguments()[1];
      Generic wrapperInput = getWrapperInputType(inputType);

      this.doFnGeneric =
          Generic.Builder.parameterizedType(
                  TypeDescription.ForLoadedType.of(DoFn.class),
                  wrapperInput,
                  TypeDescription.Generic.Builder.of(outputType).build())
              .build();

      this.flushExpander =
          FlushTimerParameterExpander.of(doFn, inputType, processElement, mainTag, outputType);
      this.onWindowExpander =
          OnWindowParameterExpander.of(
              inputType, processElement, onWindowMethod, mainTag, outputType);
      this.processElementExpander =
          ProcessElementParameterExpander.of(
              doFn, processElement, inputType, mainTag, outputType, stateWriteInstant);

      this.flushTimerInterceptor =
          new FlushTimerInterceptor<>(
              doFn,
              processElement,
              flushExpander,
              keyCoder,
              STATE_TUPLE_TAG,
              nextFlushInstantFn,
              buddy,
              collector);
      VoidMethodInvoker<Object> processElementInvoker =
          VoidMethodInvoker.of(processElement, buddy, collector);
      @Nullable VoidMethodInvoker<Object> onWindowInvoker =
          onWindowMethod == null ? null : VoidMethodInvoker.of(onWindowMethod, buddy, collector);
      this.onWindowExpirationInterceptor =
          new OnWindowExpirationInterceptor<>(
              processElementInvoker, onWindowInvoker, onWindowExpander);
      this.processElementInterceptor =
          new ProcessElementInterceptor<>(processElementExpander, processElementInvoker);
    }
  }

  private final ByteBuddy buddy = new ByteBuddy();
  private final Map<Class<?>, byte[]> generatedClasses = new HashMap<>();
  private final ClassCollector collector = generatedClasses::put;

  private final PTransform<PBegin, PCollection<KV<String, StateValue>>> inputs;
  private final Instant stateWriteInstant;
  private final UnaryFunction<Instant, Instant> nextFlushInstantFn;
  private final PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink;

  public ExpandContext(
      PTransform<PBegin, PCollection<KV<String, StateValue>>> inputs,
      Instant stateWriteInstant,
      UnaryFunction<Instant, Instant> nextFlushInstantFn,
      PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink) {

    this.inputs = inputs;
    this.stateWriteInstant = stateWriteInstant;
    this.nextFlushInstantFn = nextFlushInstantFn;
    this.stateSink = stateSink;
  }

  Pipeline expand(Pipeline pipeline) {
    validatePipeline(pipeline);
    // collect generated classes
    pipeline.getCoderRegistry().registerCoderForClass(StateValue.class, StateValue.coder());
    PCollection<KV<String, StateValue>> inputsMaterialized = pipeline.apply(inputs);
    // replace all MultiParDos
    pipeline.replaceAll(Collections.singletonList(statefulParMultiDoOverride(inputsMaterialized)));
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
                              Pair.of(
                                  asStableTransformName(node.getFullName()),
                                  (PCollection<StateValue>) p)));
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

  Map<Class<?>, byte[]> getGeneratedClasses() {
    return generatedClasses;
  }

  private static void validatePipeline(org.apache.beam.sdk.Pipeline pipeline) {
    // check that all nodes have unique names
    Set<String> names = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {

          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            String name = node.getFullName();
            Preconditions.checkState(
                names.add(name), "Node %s has conflicting state name %s", node, name);
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            String name = node.getFullName();
            Preconditions.checkState(
                names.add(name), "Node %s has conflicting state name %s", node, name);
          }
        });
  }

  private PTransformOverride statefulParMultiDoOverride(
      PCollection<KV<String, StateValue>> inputs) {

    return PTransformOverride.of(
        application -> application.getTransform() instanceof ParDo.MultiOutput,
        parMultiDoReplacementFactory(inputs));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private PTransformOverrideFactory<?, ?, ?> parMultiDoReplacementFactory(
      PCollection<KV<String, StateValue>> inputs) {

    return new PTransformOverrideFactory<>() {
      @Override
      public PTransformReplacement getReplacementTransform(AppliedPTransform transform) {
        return replaceParMultiDo(transform, inputs);
      }

      @SuppressWarnings("unchecked")
      @Override
      public Map<PCollection<?>, ReplacementOutput> mapOutputs(Map outputs, POutput newOutput) {
        return ReplacementOutputs.tagged(outputs, newOutput);
      }
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private PTransformReplacement<PInput, POutput> replaceParMultiDo(
      AppliedPTransform<PInput, POutput, ?> transform, PCollection<KV<String, StateValue>> inputs) {

    ParDo.MultiOutput<?, ?> rawTransform =
        (ParDo.MultiOutput<?, ?>) (PTransform) transform.getTransform();
    DoFn<KV<?, ?>, ?> doFn = (DoFn) rawTransform.getFn();
    PInput pMainInput = getMainInput(transform);
    if (!DoFnSignatures.isStateful(doFn)) {
      return PTransformReplacement.of(pMainInput, (PTransform) transform.getTransform());
    }
    String transformName = asStableTransformName(transform.getFullName());
    PCollection<StateValue> transformInputs =
        inputs
            .apply(Filter.by(kv -> kv.getKey().equals(transformName)))
            .apply(MapElements.into(TypeDescriptor.of(StateValue.class)).via(KV::getValue));
    TupleTag<Object> mainOutputTag = (TupleTag<Object>) rawTransform.getMainOutputTag();
    return PTransformReplacement.of(
        pMainInput,
        transformedParDo(
            transformInputs,
            (DoFn) doFn,
            mainOutputTag,
            TupleTagList.of(
                transform.getOutputs().keySet().stream()
                    .filter(t -> !t.equals(mainOutputTag))
                    .collect(Collectors.toList()))));
  }

  @SuppressWarnings("unchecked")
  private <K, V, InputT extends KV<K, V>>
      PTransform<PCollection<InputT>, PCollectionTuple> transformedParDo(
          PCollection<StateValue> transformInputs,
          DoFn<KV<K, V>, ?> doFn,
          TupleTag<Object> mainOutputTag,
          TupleTagList otherOutputs) {

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
        PCollection<KV<K, StateOrInput<V>>> transformInputs =
            input
                .apply(
                    MapElements.into(
                            TypeDescriptors.kvs(
                                keyCoder.getEncodedTypeDescriptor(), valueDescriptor))
                        .via(e -> KV.of(e.getKey(), StateOrInput.input(e.getValue()))))
                .setCoder(KvCoder.of(keyCoder, StateOrInput.coder(valueCoder)));
        PCollection<KV<K, StateOrInput<V>>> flattened =
            PCollectionList.of(state).and(transformInputs).apply(Flatten.pCollections());
        PCollectionTuple tuple =
            flattened.apply(
                "expanded",
                ParDo.of(transformedDoFn(doFn, (KvCoder<K, V>) input.getCoder(), mainOutputTag))
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

  private <K, V, InputT extends KV<K, StateOrInput<V>>> DoFn<InputT, Object> transformedDoFn(
      DoFn<KV<K, V>, ?> doFn, KvCoder<K, V> inputCoder, TupleTag<Object> mainTag) {

    DoFnExpandContext<K, V> context =
        ExceptionUtils.uncheckedFactory(() -> new DoFnExpandContext<>(doFn, inputCoder, mainTag));

    Class<? extends DoFn<KV<K, V>, ?>> doFnClass = context.getDoFnClass();
    ClassLoadingStrategy<ClassLoader> strategy = ByteBuddyUtils.getClassLoadingStrategy(doFnClass);
    final String className = doFnClass.getName() + "$Expanded";
    final ClassLoader classLoader = ExternalStateExpander.class.getClassLoader();
    try {
      @SuppressWarnings("unchecked")
      Class<? extends DoFn<InputT, Object>> aClass =
          (Class<? extends DoFn<InputT, Object>>) classLoader.loadClass(className);
      // class found, return instance
      return newInstance(aClass, context);
    } catch (ClassNotFoundException e) {
      // class not found, create it
    }

    @SuppressWarnings("unchecked")
    Builder<DoFn<InputT, ?>> builder =
        (Builder<DoFn<InputT, ?>>)
            buddy.subclass(context.getDoFnGeneric()).name(className).implement(DoFnProvider.class);

    ParameterizedType inputType = context.getInputType();
    builder = defineInvokerFields(doFnClass, inputType, builder);
    builder = addStateAndTimers(doFnClass, inputType, builder);
    builder =
        builder
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(
                doFnClass,
                context.getFlushTimerInterceptor().getClass(),
                context.getOnWindowExpirationInterceptor().getClass(),
                context.getProcessElementInterceptor().getClass())
            .intercept(
                addStateAndTimerValues(
                    doFn,
                    inputCoder,
                    MethodCall.invoke(
                            ExceptionUtils.uncheckedFactory(() -> DoFn.class.getConstructor()))
                        .andThen(FieldAccessor.ofField(DELEGATE_FIELD_NAME).setsArgumentAt(0))
                        .andThen(
                            FieldAccessor.ofField(FLUSH_TIMER_INTERCEPTOR_FIELD_NAME)
                                .setsArgumentAt(1))
                        .andThen(
                            FieldAccessor.ofField(ON_WINDOW_INTERCEPTOR_FIELD_NAME)
                                .setsArgumentAt(2))
                        .andThen(
                            FieldAccessor.ofField(PROCESS_ELEMENT_INTERCEPTOR_FIELD_NAME)
                                .setsArgumentAt(3))));

    builder = addProcessingMethods(context, builder);
    builder = implementDoFnProvider(builder);

    Unloaded<DoFn<InputT, ?>> dynamicClass = builder.make();
    Loaded<DoFn<InputT, ?>> unloaded = dynamicClass.load(null, strategy);
    Class<? extends DoFn<InputT, ?>> cls = unloaded.getLoaded();
    collector.collect(cls, unloaded.getBytes());
    return newInstance(cls, context);
  }

  private <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> implementDoFnProvider(Builder<DoFn<InputT, ?>> builder) {

    return builder
        .defineMethod("getDoFn", DoFn.class, Visibility.PUBLIC)
        .intercept(FieldAccessor.ofField(DELEGATE_FIELD_NAME));
  }

  private <InputT extends KV<K, StateOrInput<V>>, V, K> DoFn<InputT, Object> newInstance(
      Class<? extends DoFn<InputT, ?>> cls, DoFnExpandContext<K, V> context) {

    try {
      Constructor<? extends DoFn<InputT, ?>> ctor =
          cls.getDeclaredConstructor(
              context.getDoFnClass(),
              context.getFlushTimerInterceptor().getClass(),
              context.getOnWindowExpirationInterceptor().getClass(),
              context.getProcessElementInterceptor().getClass());
      @SuppressWarnings("unchecked")
      DoFn<InputT, Object> instance =
          (DoFn<InputT, Object>)
              ctor.newInstance(
                  context.getDoFn(),
                  context.getFlushTimerInterceptor(),
                  context.getOnWindowExpirationInterceptor(),
                  context.getProcessElementInterceptor());
      return instance;
    } catch (Exception ex) {
      throw new IllegalStateException(String.format("Cannot instantiate class %s", cls), ex);
    }
  }

  private <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> defineInvokerFields(
          Class<? extends DoFn<KV<K, V>, ?>> doFnClass,
          ParameterizedType inputType,
          Builder<DoFn<InputT, ?>> builder) {

    int privateFinal = Visibility.PRIVATE.getMask() + FieldManifestation.FINAL.getMask();
    Type keyType = inputType.getActualTypeArguments()[0];
    Type valueType = inputType.getActualTypeArguments()[1];
    Generic processInterceptor =
        Generic.Builder.parameterizedType(ProcessElementInterceptor.class, keyType, valueType)
            .build();
    Generic onWindowInterceptor =
        Generic.Builder.parameterizedType(OnWindowExpirationInterceptor.class, keyType, valueType)
            .build();
    Generic flushTimerInterceptor =
        Generic.Builder.parameterizedType(FlushTimerInterceptor.class, keyType, valueType).build();

    return builder
        .defineField(DELEGATE_FIELD_NAME, doFnClass, privateFinal)
        .defineField(PROCESS_ELEMENT_INTERCEPTOR_FIELD_NAME, processInterceptor, privateFinal)
        .defineField(ON_WINDOW_INTERCEPTOR_FIELD_NAME, onWindowInterceptor, privateFinal)
        .defineField(FLUSH_TIMER_INTERCEPTOR_FIELD_NAME, flushTimerInterceptor, privateFinal);
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
  private static <InputT> ParameterizedType getParameterizedDoFn(
      Class<? extends DoFn<InputT, ?>> doFnClass) {

    Type type = doFnClass.getGenericSuperclass();
    if (type instanceof ParameterizedType) {
      return (ParameterizedType) type;
    }
    if (doFnClass.getSuperclass().isAssignableFrom(DoFn.class)) {
      return getParameterizedDoFn((Class) doFnClass.getGenericSuperclass());
    }
    throw new IllegalStateException("Cannot get parameterized type of " + doFnClass);
  }

  private <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> addProcessingMethods(
          DoFnExpandContext<K, V> context, Builder<DoFn<InputT, ?>> builder) {

    DoFn<KV<K, V>, ?> doFn = context.getDoFn();
    builder = addProcessingMethod(doFn, DoFn.Setup.class, builder);
    builder = addProcessingMethod(doFn, DoFn.StartBundle.class, builder);
    builder = addProcessElementMethod(context, builder);
    builder = addOnWindowExpirationMethod(context, builder);
    builder = addTimerFlushMethod(context, builder);
    builder = addProcessingMethod(doFn, DoFn.FinishBundle.class, builder);
    builder = addProcessingMethod(doFn, DoFn.Teardown.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetInitialRestriction.class, builder);
    builder = addProcessingMethod(doFn, DoFn.SplitRestriction.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetRestrictionCoder.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetWatermarkEstimatorStateCoder.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetInitialWatermarkEstimatorState.class, builder);
    builder = addProcessingMethod(doFn, DoFn.NewWatermarkEstimator.class, builder);
    builder = addProcessingMethod(doFn, DoFn.NewTracker.class, builder);
    builder = addProcessingMethod(doFn, DoFn.OnTimer.class, builder);

    return builder;
  }

  private <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> addProcessElementMethod(
          DoFnExpandContext<K, V> context, Builder<DoFn<InputT, ?>> builder) {

    Method method = Objects.requireNonNull(context.getProcessElement());
    ProcessElementParameterExpander expander = context.getProcessElementExpander();
    List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
    Preconditions.checkArgument(void.class.isAssignableFrom(method.getReturnType()));
    MethodDefinition<DoFn<InputT, ?>> methodDefinition =
        builder
            .defineMethod(method.getName(), void.class, Visibility.PUBLIC)
            .withParameters(wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
            .intercept(MethodDelegation.toField(PROCESS_ELEMENT_INTERCEPTOR_FIELD_NAME));

    for (int i = 0; i < wrapperArgs.size(); i++) {
      Pair<AnnotationDescription, TypeDefinition> arg = wrapperArgs.get(i);
      if (arg.getFirst() != null) {
        methodDefinition = methodDefinition.annotateParameter(i, arg.getFirst());
      }
    }
    return methodDefinition.annotateMethod(method.getDeclaredAnnotations());
  }

  private <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> addOnWindowExpirationMethod(
          DoFnExpandContext<K, V> context, Builder<DoFn<InputT, ?>> builder) {

    Class<? extends Annotation> annotation = DoFn.OnWindowExpiration.class;
    @Nullable Method onWindowExpirationMethod = context.getOnWindowMethod();
    OnWindowParameterExpander expander = context.getOnWindowExpander();
    List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
    MethodDefinition<DoFn<InputT, ?>> methodDefinition =
        builder
            .defineMethod(
                onWindowExpirationMethod == null
                    ? "_onWindowExpiration"
                    : onWindowExpirationMethod.getName(),
                void.class,
                Visibility.PUBLIC)
            .withParameters(wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
            .intercept(MethodDelegation.toField(ON_WINDOW_INTERCEPTOR_FIELD_NAME));

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

  private <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> addTimerFlushMethod(
          DoFnExpandContext<K, V> context, Builder<DoFn<InputT, ?>> builder) {

    FlushTimerParameterExpander expander = context.getFlushExpander();
    List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
    MethodDefinition<DoFn<InputT, ?>> methodDefinition =
        builder
            .defineMethod("expanderFlushTimer", void.class, Visibility.PUBLIC)
            .withParameters(wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
            .intercept(MethodDelegation.toField(FLUSH_TIMER_INTERCEPTOR_FIELD_NAME));
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

  private static <K, V, OutputT> @Nullable Method findMethod(
      DoFn<? super KV<K, V>, OutputT> doFn, Class<? extends Annotation> annotation) {

    return Iterables.getOnlyElement(
        Arrays.stream(doFn.getClass().getMethods())
            .filter(m -> m.getAnnotation(annotation) != null)
            .collect(Collectors.toList()),
        null);
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, T extends Annotation>
      Builder<DoFn<InputT, ?>> addProcessingMethod(
          DoFn<KV<K, V>, ?> doFn, Class<T> annotation, Builder<DoFn<InputT, ?>> builder) {

    Method method = findMethod(doFn, annotation);
    if (method != null) {
      MethodDefinition<DoFn<InputT, ?>> methodDefinition =
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

  private static <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> addStateAndTimers(
          Class<? extends DoFn<KV<K, V>, ?>> doFnClass,
          ParameterizedType inputType,
          Builder<DoFn<InputT, ?>> builder) {

    builder = cloneFields(doFnClass, StateId.class, builder);
    builder = cloneFields(doFnClass, TimerId.class, builder);
    builder = addBufferingStatesAndTimer(inputType, builder);
    return builder;
  }

  /** Add state that buffers inputs until we process all state updates. */
  private static <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> addBufferingStatesAndTimer(
          ParameterizedType inputType, Builder<DoFn<InputT, ?>> builder) {

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

  private static <K, V, InputT extends KV<K, StateOrInput<V>>>
      Builder<DoFn<InputT, ?>> defineStateField(
          Builder<DoFn<InputT, ?>> builder,
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

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, T extends Annotation>
      Builder<DoFn<InputT, ?>> cloneFields(
          Class<? extends DoFn<KV<K, V>, ?>> doFnClass,
          Class<T> annotationClass,
          Builder<DoFn<InputT, ?>> builder) {

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

  public static class ProcessElementInterceptor<K, V> implements Serializable {

    private final ProcessElementParameterExpander expander;
    private final UnaryFunction<Object[], Boolean> processFn;
    private final VoidMethodInvoker<? super DoFn<KV<K, V>, ?>> invoker;

    private ProcessElementInterceptor(
        ProcessElementParameterExpander expander,
        VoidMethodInvoker<? super DoFn<KV<K, V>, ?>> processInvoker) {

      this.expander = expander;
      this.processFn = expander.getProcessFn();
      this.invoker = processInvoker;
    }

    @RuntimeType
    public void intercept(
        @This DoFn<KV<V, StateOrInput<V>>, ?> proxy, @AllArguments Object[] allArgs) {

      if (Boolean.TRUE.equals(processFn.apply(allArgs))) {
        Object[] methodArgs = expander.getProcessElementArgs(allArgs);
        DoFn<KV<K, V>, ?> doFn = ((DoFnProvider) proxy).getDoFn();
        ExceptionUtils.unchecked(() -> invoker.invoke(doFn, methodArgs));
      }
    }
  }

  public static class OnWindowExpirationInterceptor<K, V> implements Serializable {

    private final VoidMethodInvoker<? super DoFn<KV<K, V>, ?>> processElement;
    private final @Nullable VoidMethodInvoker<? super DoFn<KV<K, V>, ?>> onWindowExpiration;
    private final OnWindowParameterExpander expander;

    public OnWindowExpirationInterceptor(
        VoidMethodInvoker<? super DoFn<KV<K, V>, ?>> processElementInvoker,
        @Nullable VoidMethodInvoker<? super DoFn<KV<K, V>, ?>> onWindowExpirationInvoker,
        OnWindowParameterExpander expander) {

      this.processElement =
          Objects.requireNonNull(processElementInvoker, "Missing @ProcessElement");
      this.onWindowExpiration = onWindowExpirationInvoker;
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
        DoFn<KV<K, V>, ?> doFn = ((DoFnProvider) proxy).getDoFn();
        ExceptionUtils.unchecked(
            () -> processElement.invoke(doFn, expander.getProcessElementArgs(kv, allArgs)));
      }
      // invoke onWindowExpiration
      if (onWindowExpiration != null) {
        DoFn<KV<K, V>, ?> doFn = ((DoFnProvider) proxy).getDoFn();
        ExceptionUtils.unchecked(
            () -> onWindowExpiration.invoke(doFn, expander.getOnWindowExpirationArgs(allArgs)));
      }
    }
  }

  public static class FlushTimerInterceptor<K, V> implements Serializable {

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
        ByteBuddy buddy,
        ClassCollector collector) {

      this.stateReaders = getStateReaders(doFn);
      this.processElementMethod =
          ExceptionUtils.uncheckedFactory(
              () -> VoidMethodInvoker.of(processElementMethod, buddy, collector));
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
          processBuffer(
              (DoFnProvider) doFn,
              args,
              (Iterable) bufState.read(),
              MoreObjects.firstNonNull(lastFlush, now));
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
              (DoFnProvider) doFn,
              args,
              (List) pushedBackElements,
              MoreObjects.firstNonNull(nextFlush, BoundedWindow.TIMESTAMP_MAX_VALUE));
      remaining.forEach(bufState::add);
    }

    private List<TimestampedValue<KV<K, V>>> processBuffer(
        DoFnProvider provider,
        Object[] args,
        Iterable<TimestampedValue<KV<?, ?>>> buffer,
        Instant maxTs) {

      List<TimestampedValue<KV<?, ?>>> pushedBackElements = new ArrayList<>();
      buffer.forEach(
          kv -> {
            if (kv.getTimestamp().isBefore(maxTs)) {
              Object[] processArgs = expander.getProcessElementArgs(kv, args);
              DoFn<KV<K, V>, ?> doFn = provider.getDoFn();
              ExceptionUtils.unchecked(() -> processElementMethod.invoke(doFn, processArgs));
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

  private static String asStableTransformName(String name) {
    if (name.endsWith("/expanded")) {
      return asStableTransformName(name.substring(0, name.length() - 9));
    }
    if (name.endsWith("/ParMultiDo(Anonymous)")) {
      return asStableTransformName(name.substring(0, name.length() - 22));
    }
    return name;
  }

  public interface DoFnProvider {
    <K, V> DoFn<KV<K, V>, ?> getDoFn();
  }
}
