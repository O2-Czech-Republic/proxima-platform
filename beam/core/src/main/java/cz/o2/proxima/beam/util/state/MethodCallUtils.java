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

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.functional.BiFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.ParameterDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.description.type.TypeDescription.Generic.Builder;
import net.bytebuddy.dynamic.DynamicType.Unloaded;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodCall.ArgumentLoader;
import net.bytebuddy.implementation.MethodCall.ArgumentLoader.ArgumentProvider;
import net.bytebuddy.implementation.MethodCall.ArgumentLoader.Factory;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.ByteBuddyUtils;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@Slf4j
class MethodCallUtils {

  static Object[] fromGenerators(
      List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> generators,
      Object[] wrapperArgs) {

    return fromGenerators(null, generators, wrapperArgs);
  }

  static Object[] fromGenerators(
      @Nullable TimestampedValue<KV<?, ?>> elem,
      List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> generators,
      Object[] wrapperArgs) {

    Object[] res = new Object[generators.size()];
    for (int i = 0; i < generators.size(); i++) {
      res[i] = generators.get(i).apply(wrapperArgs, elem);
    }
    return res;
  }

  static List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> projectArgs(
      LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgList,
      LinkedHashMap<TypeId, Pair<Annotation, Type>> argsMap,
      TupleTag<?> mainTag,
      Type outputType) {

    List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> res =
        new ArrayList<>(argsMap.size());
    List<TypeDefinition> wrapperParamsIds =
        wrapperArgList.values().stream()
            .map(p -> p.getFirst() != null ? p.getFirst().getAnnotationType() : p.getSecond())
            .collect(Collectors.toList());
    for (TypeId t : argsMap.keySet()) {
      int wrapperArg = findArgIndex(wrapperArgList.keySet(), t);
      if (wrapperArg < 0) {
        addUnknownWrapperArgument(
            mainTag, outputType, t, wrapperParamsIds, wrapperArgList.keySet(), res);
      } else {
        remapKnownArgument(outputType, t, wrapperArg, res);
      }
    }
    return res;
  }

  private static void remapKnownArgument(
      Type outputType,
      TypeId typeId,
      int wrapperArg,
      List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> res) {

    if (typeId.isElement()) {
      // this applies to @ProcessElement only, the input element holds KV<?, StateOrInput>
      res.add((args, elem) -> extractValue((KV<?, StateOrInput<?>>) args[wrapperArg]));
    } else if (typeId.isTimestamp()) {
      res.add((args, elem) -> elem == null ? args[wrapperArg] : elem.getTimestamp());
    } else if (typeId.isOutput(outputType)) {
      remapOutputs(typeId, wrapperArg, res);
    } else {
      res.add((args, elem) -> args[wrapperArg]);
    }
  }

  private static void remapOutputs(
      TypeId typeId,
      int wrapperArg,
      List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> res) {
    if (typeId.isMultiOutput()) {
      res.add(
          (args, elem) ->
              elem == null
                  ? args[wrapperArg]
                  : remapTimestampIfNeeded((MultiOutputReceiver) args[wrapperArg], elem));
    } else {
      res.add(
          (args, elem) -> {
            if (elem == null) {
              return args[wrapperArg];
            }
            OutputReceiver<?> parent = (OutputReceiver<?>) args[wrapperArg];
            return new TimestampedOutputReceiver<>(parent, elem.getTimestamp());
          });
    }
  }

  private static void addUnknownWrapperArgument(
      TupleTag<?> mainTag,
      Type outputType,
      TypeId typeId,
      List<TypeDefinition> wrapperParamsIds,
      Iterable<TypeId> wrapperIds,
      List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> res) {

    // the wrapper does not have the required argument
    if (typeId.isElement()) {
      // wrapper does not have @Element, we need to provide it from input element
      res.add((args, elem) -> Objects.requireNonNull(elem.getValue()));
    } else if (typeId.isTimestamp()) {
      // wrapper does not have timestamp
      res.add((args, elem) -> elem.getTimestamp());
    } else if (typeId.isOutput(outputType)) {
      int wrapperPos = wrapperParamsIds.indexOf(ForLoadedType.of(MultiOutputReceiver.class));
      if (typeId.isMultiOutput()) {
        // inject timestamp
        res.add(
            (args, elem) -> remapTimestampIfNeeded((MultiOutputReceiver) args[wrapperPos], elem));
      } else {
        // remap MultiOutputReceiver to OutputReceiver
        Preconditions.checkState(wrapperPos >= 0);
        res.add(
            (args, elem) -> singleOutput((MultiOutputReceiver) args[wrapperPos], elem, mainTag));
      }
    } else {
      throw new IllegalStateException(
          String.format(
              "Missing argument %s in wrapper. Available options are %s", typeId, wrapperIds));
    }
  }

  private static DoFn.MultiOutputReceiver remapTimestampIfNeeded(
      MultiOutputReceiver parent, @Nullable TimestampedValue<KV<?, ?>> elem) {

    if (elem == null) {
      return parent;
    }
    return new MultiOutputReceiver() {
      @Override
      public <T> OutputReceiver<T> get(TupleTag<T> tag) {
        OutputReceiver<T> parentReceiver = parent.get(tag);
        return new TimestampedOutputReceiver<>(parentReceiver, elem.getTimestamp());
      }

      @Override
      public <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
        OutputReceiver<Row> parentReceiver = parent.getRowReceiver(tag);
        return new TimestampedOutputReceiver<>(parentReceiver, elem.getTimestamp());
      }
    };
  }

  private static KV<?, ?> extractValue(KV<?, StateOrInput<?>> arg) {
    Preconditions.checkArgument(!arg.getValue().isState());
    return KV.of(arg.getKey(), arg.getValue().getInput());
  }

  private static int findArgIndex(Collection<TypeId> collection, TypeId key) {
    int i = 0;
    for (TypeId t : collection) {
      if (key.equals(t)) {
        return i;
      }
      i++;
    }
    return -1;
  }

  static LinkedHashMap<TypeId, Pair<Annotation, Type>> extractArgs(Method method) {
    LinkedHashMap<TypeId, Pair<Annotation, Type>> res = new LinkedHashMap<>();
    if (method != null) {
      for (int i = 0; i < method.getParameterCount(); i++) {
        Type parameterType = method.getGenericParameterTypes()[i];
        verifyArg(parameterType);
        Annotation[] annotations = method.getParameterAnnotations()[i];
        TypeId paramId =
            annotations.length > 0
                ? TypeId.of(getSingleAnnotation(annotations))
                : TypeId.of(parameterType);
        res.put(paramId, Pair.of(annotations.length == 0 ? null : annotations[0], parameterType));
      }
    }
    return res;
  }

  private static void verifyArg(Type parameterType) {
    Preconditions.checkArgument(
        !(parameterType instanceof DoFn.ProcessContext),
        "ProcessContext is not supported. Please use the new-style @Element, @Timestamp, etc.");
  }

  static Annotation getSingleAnnotation(Annotation[] annotations) {
    Preconditions.checkArgument(annotations.length == 1, Arrays.toString(annotations));
    return annotations[0];
  }

  static Generic getInputKvType(ParameterizedType inputType) {
    Type keyType = inputType.getActualTypeArguments()[0];
    Type valueType = inputType.getActualTypeArguments()[1];

    // generic type: KV<K, V>
    return Generic.Builder.parameterizedType(KV.class, keyType, valueType).build();
  }

  private static <T> OutputReceiver<T> singleOutput(
      MultiOutputReceiver multiOutput,
      @Nullable TimestampedValue<KV<?, ?>> elem,
      TupleTag<T> mainTag) {

    return new OutputReceiver<T>() {
      @Override
      public void output(T output) {
        if (elem == null) {
          multiOutput.get(mainTag).output(output);
        } else {
          multiOutput.get(mainTag).outputWithTimestamp(output, elem.getTimestamp());
        }
      }

      @Override
      public void outputWithTimestamp(T output, Instant timestamp) {
        multiOutput.get(mainTag).outputWithTimestamp(output, timestamp);
      }
    };
  }

  static Generic getWrapperInputType(ParameterizedType inputType) {
    Type kType = inputType.getActualTypeArguments()[0];
    Type vType = inputType.getActualTypeArguments()[1];
    return Generic.Builder.parameterizedType(
            ForLoadedType.of(KV.class),
            Generic.Builder.of(kType).build(),
            Generic.Builder.parameterizedType(StateOrInput.class, vType).build())
        .build();
  }

  static Map<String, BiConsumer<Object, StateValue>> getStateUpdaters(DoFn<?, ?> doFn) {
    Field[] fields = doFn.getClass().getDeclaredFields();
    return Arrays.stream(fields)
        .map(f -> Pair.of(f, f.getAnnotation(DoFn.StateId.class)))
        .filter(p -> p.getSecond() != null)
        .map(
            p -> {
              p.getFirst().setAccessible(true);
              return p;
            })
        .map(
            p ->
                Pair.of(
                    p.getSecond().value(),
                    createUpdater(
                        ((StateSpec<?>)
                            ExceptionUtils.uncheckedFactory(() -> p.getFirst().get(doFn))))))
        .filter(p -> p.getSecond() != null)
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  @SuppressWarnings("unchecked")
  private static @Nullable BiConsumer<Object, StateValue> createUpdater(StateSpec<?> stateSpec) {
    AtomicReference<BiConsumer<Object, StateValue>> consumer = new AtomicReference<>();
    stateSpec.bind("dummy", createUpdaterBinder(consumer));
    return consumer.get();
  }

  static LinkedHashMap<String, BiFunction<Object, byte[], Iterable<StateValue>>> getStateReaders(
      DoFn<?, ?> doFn) {

    Field[] fields = doFn.getClass().getDeclaredFields();
    LinkedHashMap<String, BiFunction<Object, byte[], Iterable<StateValue>>> res =
        new LinkedHashMap<>();
    Arrays.stream(fields)
        .map(f -> Pair.of(f, f.getAnnotation(DoFn.StateId.class)))
        .filter(p -> p.getSecond() != null)
        .map(
            p -> {
              p.getFirst().setAccessible(true);
              return p;
            })
        .map(
            p ->
                Pair.of(
                    p.getSecond().value(),
                    createReader(
                        ((StateSpec<?>)
                            ExceptionUtils.uncheckedFactory(() -> p.getFirst().get(doFn))))))
        .filter(p -> p.getSecond() != null)
        .forEachOrdered(p -> res.put(p.getFirst(), p.getSecond()));
    return res;
  }

  @SuppressWarnings("unchecked")
  private static @Nullable BiFunction<Object, byte[], Iterable<StateValue>> createReader(
      StateSpec<?> stateSpec) {
    AtomicReference<BiFunction<Object, byte[], Iterable<StateValue>>> res = new AtomicReference<>();
    stateSpec.bind("dummy", createStateReaderBinder(res));
    return res.get();
  }

  @VisibleForTesting
  static StateBinder createUpdaterBinder(AtomicReference<BiConsumer<Object, StateValue>> consumer) {
    return new StateBinder() {
      @Override
      public <T> @Nullable ValueState<T> bindValue(
          String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
        consumer.set(
            (accessor, value) ->
                ((ValueState<T>) accessor)
                    .write(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(coder, value.getValue()))));
        return null;
      }

      @Override
      public <T> @Nullable BagState<T> bindBag(
          String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
        consumer.set(
            (accessor, value) ->
                ((BagState<T>) accessor)
                    .add(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(elemCoder, value.getValue()))));
        return null;
      }

      @Override
      public <T> @Nullable SetState<T> bindSet(
          String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
        consumer.set(
            (accessor, value) ->
                ((SetState<T>) accessor)
                    .add(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(elemCoder, value.getValue()))));
        return null;
      }

      @Override
      public <KeyT, ValueT> @Nullable MapState<KeyT, ValueT> bindMap(
          String id,
          StateSpec<MapState<KeyT, ValueT>> spec,
          Coder<KeyT> mapKeyCoder,
          Coder<ValueT> mapValueCoder) {
        KvCoder<KeyT, ValueT> coder = KvCoder.of(mapKeyCoder, mapValueCoder);
        consumer.set(
            (accessor, value) -> {
              KV<KeyT, ValueT> decoded =
                  ExceptionUtils.uncheckedFactory(
                      () -> CoderUtils.decodeFromByteArray(coder, value.getValue()));
              ((MapState<KeyT, ValueT>) accessor).put(decoded.getKey(), decoded.getValue());
            });
        return null;
      }

      @Override
      public <T> @Nullable OrderedListState<T> bindOrderedList(
          String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
        KvCoder<T, Instant> coder = KvCoder.of(elemCoder, InstantCoder.of());
        consumer.set(
            (accessor, value) -> {
              KV<T, Instant> decoded =
                  ExceptionUtils.uncheckedFactory(
                      () -> CoderUtils.decodeFromByteArray(coder, value.getValue()));
              ((OrderedListState<T>) accessor)
                  .add(TimestampedValue.of(decoded.getKey(), decoded.getValue()));
            });
        return null;
      }

      @Override
      public <KeyT, ValueT> @Nullable MultimapState<KeyT, ValueT> bindMultimap(
          String id,
          StateSpec<MultimapState<KeyT, ValueT>> spec,
          Coder<KeyT> keyCoder,
          Coder<ValueT> valueCoder) {
        KvCoder<KeyT, ValueT> coder = KvCoder.of(keyCoder, valueCoder);
        consumer.set(
            (accessor, value) -> {
              KV<KeyT, ValueT> decoded =
                  ExceptionUtils.uncheckedFactory(
                      () -> CoderUtils.decodeFromByteArray(coder, value.getValue()));
              ((MapState<KeyT, ValueT>) accessor).put(decoded.getKey(), decoded.getValue());
            });
        return null;
      }

      @Override
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombining(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFn<InputT, AccumT, OutputT> combineFn) {
        consumer.set(
            (accessor, value) ->
                ((CombiningState<InputT, AccumT, OutputT>) accessor)
                    .addAccum(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(accumCoder, value.getValue()))));
        return null;
      }

      @Override
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        consumer.set(
            (accessor, value) ->
                ((CombiningState<InputT, AccumT, OutputT>) accessor)
                    .addAccum(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(accumCoder, value.getValue()))));
        return null;
      }

      @Override
      public @Nullable WatermarkHoldState bindWatermark(
          String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
        return null;
      }
    };
  }

  @VisibleForTesting
  static StateBinder createStateReaderBinder(
      AtomicReference<BiFunction<Object, byte[], Iterable<StateValue>>> res) {

    return new StateBinder() {
      @Override
      public <T> @Nullable ValueState<T> bindValue(
          String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
        res.set(
            (accessor, key) -> {
              T value = ((ValueState<T>) accessor).read();
              if (value != null) {
                byte[] bytes =
                    ExceptionUtils.uncheckedFactory(
                        () -> CoderUtils.encodeToByteArray(coder, value));
                return Collections.singletonList(new StateValue(key, id, bytes));
              }
              return Collections.emptyList();
            });
        return null;
      }

      @Override
      public <T> @Nullable BagState<T> bindBag(
          String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((BagState<T>) accessor).read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () -> CoderUtils.encodeToByteArray(elemCoder, v)))));
        return null;
      }

      @Override
      public <T> @Nullable SetState<T> bindSet(
          String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((SetState<T>) accessor).read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () -> CoderUtils.encodeToByteArray(elemCoder, v)))));
        return null;
      }

      @Override
      public <KeyT, ValueT> @Nullable MapState<KeyT, ValueT> bindMap(
          String id,
          StateSpec<MapState<KeyT, ValueT>> spec,
          Coder<KeyT> mapKeyCoder,
          Coder<ValueT> mapValueCoder) {
        KvCoder<KeyT, ValueT> coder = KvCoder.of(mapKeyCoder, mapValueCoder);
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((MapState<KeyT, ValueT>) accessor).entries().read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () ->
                                    CoderUtils.encodeToByteArray(
                                        coder, KV.of(v.getKey(), v.getValue()))))));
        return null;
      }

      @Override
      public <T> @Nullable OrderedListState<T> bindOrderedList(
          String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
        KvCoder<T, Instant> coder = KvCoder.of(elemCoder, InstantCoder.of());
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((OrderedListState<T>) accessor).read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () ->
                                    CoderUtils.encodeToByteArray(
                                        coder, KV.of(v.getValue(), v.getTimestamp()))))));
        return null;
      }

      @Override
      public <KeyT, ValueT> @Nullable MultimapState<KeyT, ValueT> bindMultimap(
          String id,
          StateSpec<MultimapState<KeyT, ValueT>> spec,
          Coder<KeyT> keyCoder,
          Coder<ValueT> valueCoder) {
        KvCoder<KeyT, ValueT> coder = KvCoder.of(keyCoder, valueCoder);
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((MultimapState<KeyT, ValueT>) accessor).entries().read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () ->
                                    CoderUtils.encodeToByteArray(
                                        coder, KV.of(v.getKey(), v.getValue()))))));
        return null;
      }

      @Override
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombining(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFn<InputT, AccumT, OutputT> combineFn) {
        res.set(
            (accessor, key) -> {
              AccumT accum = ((CombiningState<InputT, AccumT, OutputT>) accessor).getAccum();
              return Collections.singletonList(
                  new StateValue(
                      key,
                      id,
                      ExceptionUtils.uncheckedFactory(
                          () -> CoderUtils.encodeToByteArray(accumCoder, accum))));
            });
        return null;
      }

      @Override
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        res.set(
            (accessor, key) -> {
              AccumT accum = ((CombiningState<InputT, AccumT, OutputT>) accessor).getAccum();
              return Collections.singletonList(
                  new StateValue(
                      key,
                      id,
                      ExceptionUtils.uncheckedFactory(
                          () -> CoderUtils.encodeToByteArray(accumCoder, accum))));
            });
        return null;
      }

      @Override
      public @Nullable WatermarkHoldState bindWatermark(
          String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
        return null;
      }
    };
  }

  private static class TimestampedOutputReceiver<T> implements OutputReceiver<T> {

    private final OutputReceiver<T> parentReceiver;
    private final Instant elementTimestamp;

    public TimestampedOutputReceiver(OutputReceiver<T> parentReceiver, Instant timestamp) {
      this.parentReceiver = parentReceiver;
      this.elementTimestamp = timestamp;
    }

    @Override
    public void output(T output) {
      outputWithTimestamp(output, elementTimestamp);
    }

    @Override
    public void outputWithTimestamp(T output, Instant timestamp) {
      parentReceiver.outputWithTimestamp(output, timestamp);
    }
  }

  public static class MethodParameterArrayElement implements ArgumentLoader {
    private final ParameterDescription parameterDescription;
    private final int index;

    public MethodParameterArrayElement(ParameterDescription parameterDescription, int index) {
      this.parameterDescription = parameterDescription;
      this.index = index;
    }

    @Override
    public StackManipulation toStackManipulation(
        ParameterDescription target, Assigner assigner, Assigner.Typing typing) {
      StackManipulation stackManipulation =
          new StackManipulation.Compound(
              MethodVariableAccess.load(this.parameterDescription),
              IntegerConstant.forValue(this.index),
              ArrayAccess.of(this.parameterDescription.getType().getComponentType()).load(),
              assigner.assign(
                  this.parameterDescription.getType().getComponentType(),
                  target.getType(),
                  Typing.DYNAMIC));
      if (!stackManipulation.isValid()) {
        throw new IllegalStateException(
            "Cannot assign "
                + this.parameterDescription.getType().getComponentType()
                + " to "
                + target);
      } else {
        return stackManipulation;
      }
    }
  }

  public static class ArrayArgumentProvider implements Factory, ArgumentProvider {

    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    public ArgumentProvider make(Implementation.Target implementationTarget) {
      return this;
    }

    public List<ArgumentLoader> resolve(
        MethodDescription instrumentedMethod, MethodDescription invokedMethod) {

      ParameterDescription desc = instrumentedMethod.getParameters().get(1);
      List<ArgumentLoader> res = new ArrayList<>(invokedMethod.getParameters().size());
      for (int i = 0; i < invokedMethod.getParameters().size(); ++i) {
        res.add(new MethodParameterArrayElement(desc, i));
      }
      return res;
    }
  }

  public interface MethodInvoker<T, R> extends Serializable {

    static <T, R> MethodInvoker<T, R> of(Method method, ByteBuddy buddy, ClassCollector collector)
        throws NoSuchMethodException,
            InvocationTargetException,
            InstantiationException,
            IllegalAccessException {

      return getInvoker(method, buddy, collector);
    }

    R invoke(T _this, Object[] args);
  }

  public interface VoidMethodInvoker<T> extends Serializable {

    static <T> VoidMethodInvoker<T> of(Method method, ByteBuddy buddy, ClassCollector collector)
        throws NoSuchMethodException,
            InvocationTargetException,
            InstantiationException,
            IllegalAccessException {

      return getInvoker(method, buddy, collector);
    }

    void invoke(T _this, Object[] args);
  }

  private static <T> T getInvoker(Method method, ByteBuddy buddy, ClassCollector collector)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    Class<?> declaringClass = method.getDeclaringClass();
    Class<?> superClass = fromDeclaringClass(declaringClass);
    String methodName = method.getName();
    Type returnType = method.getGenericReturnType();
    Generic implement =
        returnType.equals(void.class)
            ? Builder.parameterizedType(VoidMethodInvoker.class, declaringClass).build()
            : Builder.parameterizedType(MethodInvoker.class, declaringClass, returnType).build();
    ClassLoadingStrategy<ClassLoader> strategy = ByteBuddyUtils.getClassLoadingStrategy(superClass);
    String subclassName = declaringClass.getName() + "$" + methodName + "Invoker";
    try {
      @SuppressWarnings("unchecked")
      Class<T> loaded = (Class<T>) superClass.getClassLoader().loadClass(subclassName);
      return newInstance(loaded);
    } catch (Exception ex) {
      // define the class
    }
    Unloaded<?> unloaded =
        buddy
            .subclass(superClass)
            .implement(implement)
            .name(subclassName)
            .defineMethod("invoke", returnType, Visibility.PUBLIC)
            .withParameters(declaringClass, Object[].class)
            .intercept(MethodCall.invoke(method).onArgument(0).with(new ArrayArgumentProvider()))
            .make();
    @SuppressWarnings("unchecked")
    Class<T> cls = (Class<T>) unloaded.load(null, strategy).getLoaded();
    collector.collect(cls, unloaded.getBytes());
    return newInstance(cls);
  }

  private static Class<?> fromDeclaringClass(Class<?> cls) {
    if (cls.getEnclosingClass() != null && !hasDefaultConstructor(cls)) {
      return fromDeclaringClass(cls.getEnclosingClass());
    }
    return cls;
  }

  private static boolean hasDefaultConstructor(Class<?> cls) {
    return Arrays.stream(cls.getDeclaredConstructors()).anyMatch(c -> c.getParameterCount() == 0);
  }

  private static <T> T newInstance(Class<T> cls)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    return cls.getDeclaredConstructor().newInstance();
  }

  private MethodCallUtils() {}
}
