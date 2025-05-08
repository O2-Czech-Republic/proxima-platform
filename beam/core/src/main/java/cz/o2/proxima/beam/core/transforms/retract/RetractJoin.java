/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core.transforms.retract;

import cz.o2.proxima.beam.core.transforms.retract.LeftOrRight.LeftOrRightCoder;
import cz.o2.proxima.beam.util.state.ExcludeExternal;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeParameter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Slf4j
public class RetractJoin {

  public static <K1, K2, V1, V2, JK> RetractPCollection<KV<KV<K1, K2>, KV<V1, V2>>> join(
      KeyedRetractPCollection<K1, V1> lhs,
      KeyedRetractPCollection<K2, V2> rhs,
      UnaryFunction<V1, JK> leftJoinKeyExtractor,
      UnaryFunction<V2, JK> rightJoinKeyExtractor,
      TypeDescriptor<JK> joinKeyDescriptor,
      Duration cleanupDuration) {

    TypeDescriptor<K1> leftKey = lhs.getKeyDescriptor();
    TypeDescriptor<K2> rightKey = rhs.getKeyDescriptor();
    TypeDescriptor<V1> leftValue = lhs.getValueDescriptor();
    TypeDescriptor<V2> rightValue = rhs.getValueDescriptor();
    TypeDescriptor<LeftOrRight<RetractElement<V1>, RetractElement<V2>>> unionType =
        new TypeDescriptor<LeftOrRight<RetractElement<V1>, RetractElement<V2>>>() {}.where(
                new TypeParameter<>() {}, leftValue)
            .where(new TypeParameter<>() {}, rightValue);
    CoderRegistry coderRegistry = lhs.getPipeline().getCoderRegistry();
    Coder<K1> leftKeyCoder = ExceptionUtils.uncheckedFactory(() -> coderRegistry.getCoder(leftKey));
    Coder<K2> rightKeyCoder =
        ExceptionUtils.uncheckedFactory(() -> coderRegistry.getCoder(rightKey));
    Coder<V1> leftValueCoder =
        ExceptionUtils.uncheckedFactory(() -> coderRegistry.getCoder(leftValue));
    Coder<V2> rightValueCoder =
        ExceptionUtils.uncheckedFactory(() -> coderRegistry.getCoder(rightValue));

    Coder<LeftOrRight<RetractElement<V1>, RetractElement<V2>>> valueCoder =
        new LeftOrRightCoder<>(
            RetractElement.Coder.of(leftValueCoder), RetractElement.Coder.of(rightValueCoder));
    KvCoder<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>> unionCoder =
        KvCoder.of(
            ExceptionUtils.uncheckedFactory(() -> coderRegistry.getCoder(joinKeyDescriptor)),
            valueCoder);

    PCollection<KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>>> lhsKv =
        lhs.unwrappedValues()
            .apply(
                MapElements.into(TypeDescriptors.kvs(joinKeyDescriptor, unionType))
                    .via(
                        e ->
                            KV.of(
                                Objects.requireNonNull(leftJoinKeyExtractor.apply(e.getValue())),
                                LeftOrRight.left(e))))
            .setCoder(unionCoder);
    PCollection<KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>>> rhsKv =
        rhs.unwrappedValues()
            .apply(
                MapElements.into(TypeDescriptors.kvs(joinKeyDescriptor, unionType))
                    .via(
                        e ->
                            KV.of(
                                Objects.requireNonNull(rightJoinKeyExtractor.apply(e.getValue())),
                                LeftOrRight.right(e))))
            .setCoder(unionCoder);

    PCollection<KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>>> union =
        PCollectionList.of(lhsKv).and(rhsKv).apply(Flatten.pCollections());

    PCollection<RetractElement<KV<KV<K1, K2>, KV<V1, V2>>>> res =
        union
            .apply(
                "retractMostRecentJoin",
                ParDo.of(
                    new RetractMostRecentJoinFn<>(
                        lhs.getKeyExtractor(),
                        rhs.getKeyExtractor(),
                        leftKeyCoder,
                        rightKeyCoder,
                        leftValueCoder,
                        rightValueCoder,
                        cleanupDuration)))
            .setCoder(
                RetractElement.Coder.of(
                    KvCoder.of(
                        KvCoder.of(leftKeyCoder, rightKeyCoder),
                        KvCoder.of(leftValueCoder, rightValueCoder))));

    return RetractPCollection.fromRetractedElements(
        res,
        e -> e,
        TypeDescriptors.kvs(
            TypeDescriptors.kvs(leftKey, rightKey), TypeDescriptors.kvs(leftValue, rightValue)));
  }

  private static class RetractMostRecentJoinFn<K1, K2, V1, V2, JK>
      extends DoFn<
          KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>>,
          RetractElement<KV<KV<K1, K2>, KV<V1, V2>>>> {

    private final UnaryFunction<V1, K1> leftKeyExtractor;
    private final UnaryFunction<V2, K2> rightKeyExtractor;

    final Duration cleanupDuration;

    @StateId("left")
    private final StateSpec<MapState<K1, KV<SequentialInstant, RetractElement<V1>>>> leftState;

    @StateId("right")
    private final StateSpec<MapState<K2, KV<SequentialInstant, RetractElement<V2>>>> rightState;

    @StateId("cleanupTs")
    private final StateSpec<ValueState<Instant>> cleanupTsSpec = StateSpecs.value();

    @StateId("seq")
    private final StateSpec<ValueState<Long>> seqSpec = StateSpecs.value();

    @TimerId("cleanupTimer")
    private final TimerSpec cleanupTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    public RetractMostRecentJoinFn(
        UnaryFunction<V1, K1> leftKeyExtractor,
        UnaryFunction<V2, K2> rightKeyExtractor,
        Coder<K1> leftKeyCoder,
        Coder<K2> rightKeyCoder,
        Coder<V1> leftValueCoder,
        Coder<V2> rightValueCoder,
        Duration cleanupDuration) {

      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;

      this.leftState =
          StateSpecs.map(
              leftKeyCoder,
              KvCoder.of(new SequentialInstant.Coder(), RetractElement.Coder.of(leftValueCoder)));
      this.rightState =
          StateSpecs.map(
              rightKeyCoder,
              KvCoder.of(new SequentialInstant.Coder(), RetractElement.Coder.of(rightValueCoder)));
      this.cleanupDuration = cleanupDuration;
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(Long.MAX_VALUE);
    }

    @ExcludeExternal
    @ProcessElement
    public void process(
        @Element KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>> elem,
        @Timestamp Instant ts,
        @StateId("left") MapState<K1, KV<SequentialInstant, RetractElement<V1>>> leftState,
        @StateId("right") MapState<K2, KV<SequentialInstant, RetractElement<V2>>> rightState,
        @StateId("seq") ValueState<Long> seqState,
        @StateId("cleanupTs") ValueState<Instant> cleanupTs,
        @TimerId("cleanupTimer") Timer cleanupTimer,
        OutputReceiver<RetractElement<KV<KV<K1, K2>, KV<V1, V2>>>> output) {

      Instant cleanupStamp = cleanupTs.read();
      if (cleanupStamp == null) {
        // first element set up timer
        cleanupTimer.offset(cleanupDuration).setRelative();
        cleanupStamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
        cleanupTs.write(cleanupStamp);
      }

      AtomicLong seq = new AtomicLong(MoreObjects.firstNonNull(seqState.read(), 0L));
      LeftOrRight<RetractElement<V1>, RetractElement<V2>> value = elem.getValue();
      if (value.isLeft()) {
        retractAndEmit(
            leftState,
            rightState,
            Objects.requireNonNull(leftKeyExtractor.apply(value.getLeft().getValue())),
            value.getLeft(),
            ts,
            (lk, rk, lv, rv, stamp) ->
                output.outputWithTimestamp(
                    RetractElement.ofAddition(
                        KV.of(KV.of(lk, rk), KV.of(lv, rv)), seq.incrementAndGet()),
                    stamp),
            (lk, rk, lv, rv, stamp) ->
                output.outputWithTimestamp(
                    RetractElement.ofDeletion(
                        KV.of(KV.of(lk, rk), KV.of(lv, rv)), seq.incrementAndGet()),
                    stamp));
      } else {
        retractAndEmit(
            rightState,
            leftState,
            Objects.requireNonNull(rightKeyExtractor.apply(value.getRight().getValue())),
            value.getRight(),
            ts,
            (rk, lk, rv, lv, stamp) ->
                output.outputWithTimestamp(
                    RetractElement.ofAddition(
                        KV.of(KV.of(lk, rk), KV.of(lv, rv)), seq.incrementAndGet()),
                    stamp),
            (rk, lk, rv, lv, stamp) ->
                output.outputWithTimestamp(
                    RetractElement.ofDeletion(
                        KV.of(KV.of(lk, rk), KV.of(lv, rv)), seq.incrementAndGet()),
                    stamp));
      }
      seqState.write(seq.get());
    }

    private <PK, SK, PV, SV> void retractAndEmit(
        MapState<PK, KV<SequentialInstant, RetractElement<PV>>> primaryState,
        MapState<SK, KV<SequentialInstant, RetractElement<SV>>> secondaryState,
        PK primaryKey,
        RetractElement<PV> primaryValue,
        Instant ts,
        OutputConsumer<PK, SK, PV, SV> addConsumer,
        OutputConsumer<PK, SK, PV, SV> retractConsumer) {

      SequentialInstant thisInstant = new SequentialInstant(ts, primaryValue.getSeqId());
      @Nullable KV<SequentialInstant, RetractElement<PV>> oldPrimaryValue =
          primaryState.get(primaryKey).read();

      if (oldPrimaryValue != null && oldPrimaryValue.getKey().compareTo(thisInstant) >= 0) {
        log.info(
            "Got stale update at {} old {} new {} of {}",
            thisInstant,
            oldPrimaryValue,
            primaryValue,
            primaryKey);
        return;
      }

      // update primary value
      primaryState.put(primaryKey, KV.of(thisInstant, primaryValue));

      if ((oldPrimaryValue == null || !oldPrimaryValue.getValue().isAddition())
          && !primaryValue.isAddition()) {
        // delete is replaced by delete, no additions or retractions
        return;
      }

      Iterable<Entry<SK, KV<SequentialInstant, RetractElement<SV>>>> secondaryKeyedElements =
          secondaryState.entries().read();

      // infer time-range for updates
      for (Map.Entry<SK, KV<SequentialInstant, RetractElement<SV>>> e : secondaryKeyedElements) {
        processSecondaryElement(
            primaryKey,
            primaryValue,
            e,
            thisInstant,
            oldPrimaryValue,
            addConsumer,
            retractConsumer);
      }
    }

    private <PK, SK, PV, SV> void processSecondaryElement(
        PK primaryKey,
        RetractElement<PV> primaryValue,
        Entry<SK, KV<SequentialInstant, RetractElement<SV>>> e,
        SequentialInstant thisInstant,
        @Nullable KV<SequentialInstant, RetractElement<PV>> oldPrimaryValue,
        OutputConsumer<PK, SK, PV, SV> addConsumer,
        OutputConsumer<PK, SK, PV, SV> retractConsumer) {

      SK secondaryKey = e.getKey();
      KV<SequentialInstant, RetractElement<SV>> secondaryValue = e.getValue();
      // if secondary value exists
      if (secondaryValue.getValue().isAddition()) {
        // remove old value, if exists
        if (oldPrimaryValue != null && oldPrimaryValue.getValue().isAddition()) {
          retractConsumer.apply(
              primaryKey,
              secondaryKey,
              oldPrimaryValue.getValue().getValue(),
              secondaryValue.getValue().getValue(),
              max(oldPrimaryValue.getKey(), secondaryValue.getKey()));
        }
        if (primaryValue.isAddition()) {
          // insert new value
          addConsumer.apply(
              primaryKey,
              secondaryKey,
              primaryValue.getValue(),
              secondaryValue.getValue().getValue(),
              max(thisInstant, secondaryValue.getKey()));
        } else {
          // retract old value
          retractConsumer.apply(
              primaryKey,
              secondaryKey,
              oldPrimaryValue.getValue().getValue(),
              secondaryValue.getValue().getValue(),
              max(thisInstant, secondaryValue.getKey()));
        }
      }
    }

    private Instant max(SequentialInstant a, SequentialInstant b) {
      return a.compareTo(b) < 0 ? b.getTimestamp() : a.getTimestamp();
    }

    @OnTimer("cleanupTimer")
    public void onTimer(
        @Key JK key,
        @TimerId("cleanupTimer") Timer cleanupTimer,
        @StateId("left") MapState<K1, KV<SequentialInstant, RetractElement<V1>>> leftState,
        @StateId("right") MapState<K2, KV<SequentialInstant, RetractElement<V2>>> rightState,
        @StateId("seq") ValueState<Long> seqState,
        @StateId("cleanupTs") ValueState<Instant> cleanupTs) {

      Instant cleanStamp = cleanupTimer.getCurrentRelativeTime().minus(cleanupDuration);
      cleanupTs.write(cleanStamp);
      boolean isEmpty = cleanupState(leftState, cleanStamp);
      isEmpty |= cleanupState(rightState, cleanStamp);
      if (isEmpty) {
        log.info("Clearing complete state of join key {}", key);
        seqState.clear();
        cleanupTs.clear();
      } else if (cleanupTimer
          .getCurrentRelativeTime()
          .isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        cleanupTimer.offset(cleanupDuration).setRelative();
      }
    }

    private <K, V> boolean cleanupState(
        MapState<K, KV<SequentialInstant, RetractElement<V>>> state, Instant cleanStamp) {

      boolean allCleared = true;
      List<K> toRemove = new ArrayList<>();
      Iterable<Entry<K, KV<SequentialInstant, RetractElement<V>>>> entries = state.entries().read();
      for (Entry<K, KV<SequentialInstant, RetractElement<V>>> e : entries) {
        if (e.getValue().getValue().isAddition()
            || !e.getValue().getKey().getTimestamp().isBefore(cleanStamp)) {
          allCleared = false;
        } else {
          toRemove.add(e.getKey());
        }
      }
      if (allCleared) {
        state.clear();
        return true;
      }
      toRemove.forEach(state::remove);
      return false;
    }
  }

  @FunctionalInterface
  interface OutputConsumer<PK, SK, PV, SV> {
    void apply(PK pk, SK sk, PV pv, SV sv, Instant ts);
  }
}
