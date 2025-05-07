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
package cz.o2.proxima.beam.core.transform.retract;

import cz.o2.proxima.beam.core.transform.retract.LeftOrRight.LeftOrRightCoder;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SortedMapCoder;
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
public class RetractInnerJoin<K1, V1, K2, V2, JK> {

  public static <K1, V1, K2, V2, JK>
      KeyedRetractPCollection<KV<K1, K2>, KV<KV<K1, K2>, KV<V1, V2>>> join(
          KeyedRetractPCollection<K1, V1> lhs,
          KeyedRetractPCollection<K2, V2> rhs,
          UnaryFunction<V1, JK> leftJoinKeyExtractor,
          UnaryFunction<V2, JK> rightJoinKeyExtractor,
          TypeDescriptor<JK> joinKeyDescriptor) {

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
                    .via(e -> KV.of(leftJoinKeyExtractor.apply(e.getValue()), LeftOrRight.left(e))))
            .setCoder(unionCoder);
    PCollection<KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>>> rhsKv =
        rhs.unwrappedValues()
            .apply(
                MapElements.into(TypeDescriptors.kvs(joinKeyDescriptor, unionType))
                    .via(
                        e ->
                            KV.of(rightJoinKeyExtractor.apply(e.getValue()), LeftOrRight.right(e))))
            .setCoder(unionCoder);

    PCollection<KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>>> union =
        PCollectionList.of(lhsKv).and(rhsKv).apply(Flatten.pCollections());

    PCollection<RetractElement<KV<KV<K1, K2>, KV<V1, V2>>>> res =
        union
            .apply(
                "retractInnerJoin",
                ParDo.of(
                    new RetractInnerJoinFn<>(
                        lhs.getKeyExtractor(),
                        rhs.getKeyExtractor(),
                        leftKeyCoder,
                        rightKeyCoder,
                        leftValueCoder,
                        rightValueCoder)))
            .setCoder(
                RetractElement.Coder.of(
                    KvCoder.of(
                        KvCoder.of(leftKeyCoder, rightKeyCoder),
                        KvCoder.of(leftValueCoder, rightValueCoder))));

    return RetractPCollection.fromRetractedElements(
            res,
            e -> e,
            TypeDescriptors.kvs(
                TypeDescriptors.kvs(leftKey, rightKey), TypeDescriptors.kvs(leftValue, rightValue)))
        .keyed(KV::getKey, TypeDescriptors.kvs(leftKey, rightKey));
  }

  private static class RetractInnerJoinFn<K1, K2, V1, V2, JK>
      extends DoFn<
          KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>>,
          RetractElement<KV<KV<K1, K2>, KV<V1, V2>>>> {

    private final UnaryFunction<V1, K1> leftKeyExtractor;
    private final UnaryFunction<V2, K2> rightKeyExtractor;

    // FIXME
    Duration cleanupDuration = Duration.standardDays(2);

    @StateId("left")
    private final StateSpec<MapState<K1, SortedMap<SequentialInstant, V1>>> leftState;

    @StateId("right")
    private final StateSpec<MapState<K2, SortedMap<SequentialInstant, V2>>> rightState;

    @StateId("cleanupTs")
    private final StateSpec<ValueState<Instant>> cleanupTsSpec = StateSpecs.value();

    @StateId("seq")
    private final StateSpec<ValueState<Long>> seqSpec = StateSpecs.value();

    @TimerId("cleanupTimer")
    private final TimerSpec cleanupTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    public RetractInnerJoinFn(
        UnaryFunction<V1, K1> leftKeyExtractor,
        UnaryFunction<V2, K2> rightKeyExtractor,
        Coder<K1> leftKeyCoder,
        Coder<K2> rightKeyCoder,
        Coder<V1> leftValueCoder,
        Coder<V2> rightValueCoder) {

      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;

      this.leftState =
          StateSpecs.map(
              leftKeyCoder, SortedMapCoder.of(new SequentialInstant.Coder(), leftValueCoder));
      this.rightState =
          StateSpecs.map(
              rightKeyCoder, SortedMapCoder.of(new SequentialInstant.Coder(), rightValueCoder));
    }

    @ProcessElement
    public void process(
        @Element KV<JK, LeftOrRight<RetractElement<V1>, RetractElement<V2>>> elem,
        @Timestamp Instant ts,
        @StateId("left") MapState<K1, SortedMap<SequentialInstant, V1>> leftState,
        @StateId("right") MapState<K2, SortedMap<SequentialInstant, V2>> rightState,
        @StateId("seq") ValueState<Long> seqState,
        @StateId("cleanupTs") ValueState<Instant> cleanupTs,
        @TimerId("cleanupTimer") Timer cleanupTimer,
        OutputReceiver<RetractElement<KV<KV<K1, K2>, KV<V1, V2>>>> output) {

      AtomicLong seq = new AtomicLong(MoreObjects.firstNonNull(seqState.read(), 0L));
      Instant minAcceptStamp = cleanupTs.read();
      if (minAcceptStamp == null) {
        // first element setup timer
        cleanupTimer.offset(cleanupDuration).setRelative();
        cleanupTs.write(minAcceptStamp = BoundedWindow.TIMESTAMP_MIN_VALUE);
      }
      if (!minAcceptStamp.isBefore(ts)) {
        LeftOrRight<RetractElement<V1>, RetractElement<V2>> value = elem.getValue();
        if (value.isLeft()) {
          retractAndEmit(
              leftState,
              rightState,
              leftKeyExtractor.apply(value.getLeft().getValue()),
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
              rightKeyExtractor.apply(value.getRight().getValue()),
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
      }
      seqState.write(seq.get());
    }

    private <PK, SK, PV, SV> void retractAndEmit(
        MapState<PK, SortedMap<SequentialInstant, PV>> primaryState,
        MapState<SK, SortedMap<SequentialInstant, SV>> secondaryState,
        PK primaryKey,
        RetractElement<PV> primaryValue,
        Instant ts,
        OutputConsumer<PK, SK, PV, SV> add,
        OutputConsumer<PK, SK, PV, SV> retract) {

      SequentialInstant thisInstant = new SequentialInstant(ts, primaryValue.getSeqId());
      SortedMap<SequentialInstant, PV> primaryValues =
          Optional.ofNullable(primaryState.get(primaryKey).read()).orElseGet(TreeMap::new);

      PV sameUpdate = primaryValues.get(thisInstant);
      if (sameUpdate != null) {
        log.warn("Got already received update to {} of {}", thisInstant, primaryKey);
        return;
      }

      // infer time-range for updates
      @Nullable final SequentialInstant previousUpdate;
      @Nullable final SequentialInstant nextUpdate;
      @Nullable final PV previousValue;
      @Nullable final PV nextValue;
      {
        SortedMap<SequentialInstant, PV> headMap = primaryValues.headMap(thisInstant);
        SortedMap<SequentialInstant, PV> tailMap = primaryValues.tailMap(thisInstant);
        previousUpdate = headMap.isEmpty() ? null : headMap.lastKey();
        nextUpdate = tailMap.isEmpty() ? null : tailMap.firstKey();
        previousValue = previousUpdate == null ? null : headMap.get(previousUpdate);
        nextValue = nextUpdate == null ? null : tailMap.get(nextUpdate);
      }

      for (Map.Entry<SK, SortedMap<SequentialInstant, SV>> e : secondaryState.entries().read()) {
        SK secondaryKey = e.getKey();
        final SortedMap<SequentialInstant, SV> affectedRange;
        if (previousUpdate != null && nextUpdate != null) {
          affectedRange = e.getValue().subMap(previousUpdate, nextUpdate);
        } else if (previousUpdate != null) {
          affectedRange = e.getValue().tailMap(previousUpdate);
        } else if (nextUpdate != null) {
          affectedRange = e.getValue().headMap(nextUpdate);
        } else {
          affectedRange = e.getValue();
        }
        if (primaryValue.isAddition()) {
          // retract:
          // a) elements _below_ current with nextValue
          // b) elements _above_ current with previousValue

          // add:
          // a) elements _below_ with currentValue
          // b) elements _above_ with currentValue

          for (Map.Entry<SequentialInstant, SV> affectedEntry : affectedRange.entrySet()) {
            final SV retractSecondary = affectedEntry.getValue();
            final PV retractPrimary;
            final Instant retractStamp;
            if (affectedEntry.getKey().compareTo(thisInstant) < 0) {
              retractPrimary = nextValue;
              retractStamp = nextUpdate != null ? nextUpdate.getTimestamp() : null;
            } else {
              retractPrimary = previousValue;
              retractStamp = previousUpdate != null ? previousUpdate.getTimestamp() : null;
            }
            if (retractPrimary != null) {
              retract.apply(
                  primaryKey, secondaryKey, retractPrimary, retractSecondary, retractStamp);
            }
          }
          for (Map.Entry<SequentialInstant, SV> affectedEntry : affectedRange.entrySet()) {
            final SV secondaryValue = affectedEntry.getValue();
            final SequentialInstant addStamp =
                affectedEntry.getKey().compareTo(thisInstant) < 0
                    ? thisInstant
                    : affectedEntry.getKey();
            add.apply(
                primaryKey,
                secondaryKey,
                primaryValue.getValue(),
                secondaryValue,
                addStamp.getTimestamp());
          }
        } else {
          // retract:
          // a) elements _below_ current with currentValue
          // b) elements _above_ current with currentValue

          // add:
          // a) elements _below_ with nextValue
          // b) elements _above_ with previousValue

          for (Map.Entry<SequentialInstant, SV> affectedEntry : affectedRange.entrySet()) {
            final SV secondaryValue = affectedEntry.getValue();
            final SequentialInstant addStamp =
                affectedEntry.getKey().compareTo(thisInstant) < 0
                    ? thisInstant
                    : affectedEntry.getKey();
            retract.apply(
                primaryKey,
                secondaryKey,
                primaryValue.getValue(),
                secondaryValue,
                addStamp.getTimestamp());
          }
          for (Map.Entry<SequentialInstant, SV> affectedEntry : affectedRange.entrySet()) {
            final SV retractSecondary = affectedEntry.getValue();
            final PV retractPrimary;
            final Instant retractStamp;
            if (affectedEntry.getKey().compareTo(thisInstant) < 0) {
              retractPrimary = nextValue;
              retractStamp = nextUpdate != null ? nextUpdate.getTimestamp() : null;
            } else {
              retractPrimary = previousValue;
              retractStamp = previousUpdate != null ? previousUpdate.getTimestamp() : null;
            }
            if (retractPrimary != null) {
              add.apply(primaryKey, secondaryKey, retractPrimary, retractSecondary, retractStamp);
            }
          }
        }
      }
      // update primary values
      primaryValues.put(thisInstant, primaryValue.getValue());
      primaryState.put(primaryKey, primaryValues);
    }

    @OnTimer("cleanupTimer")
    public void onTimer(
        @TimerId("cleanupTimer") Timer cleanupTimer,
        @StateId("cleanupTs") ValueState<Instant> cleanupTs) {

      Instant cleanStamp = cleanupTimer.getCurrentRelativeTime().minus(cleanupDuration);
      cleanupTs.write(cleanStamp);
      if (cleanupTimer.getCurrentRelativeTime().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        cleanupTimer.offset(cleanupDuration).setRelative();
      }
    }
  }

  @FunctionalInterface
  interface OutputConsumer<PK, SK, PV, SV> {
    void apply(PK pk, SK sk, PV pv, SV sv, Instant ts);
  }
}
