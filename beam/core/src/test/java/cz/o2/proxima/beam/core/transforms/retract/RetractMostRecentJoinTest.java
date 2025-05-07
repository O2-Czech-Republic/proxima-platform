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

import cz.o2.proxima.core.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeParameter;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class RetractMostRecentJoinTest {

  final Duration cleanupDuration = Duration.standardDays(2);

  @Test
  public void testSimple() {
    Pipeline p = Pipeline.create();
    PCollection<KV<String, Integer>> lhsRaw = p.apply(Create.of(KV.of("a", 1), KV.of("b", 2)));
    PCollection<KV<Integer, String>> rhsRaw = p.apply(Create.of(KV.of(1, "b"), KV.of(2, "c")));

    KeyedRetractPCollection<String, KV<String, Integer>> lhs =
        RetractPCollection.wrap(lhsRaw, ign -> 0L, ign -> true)
            .keyed(KV::getKey, TypeDescriptors.strings());
    KeyedRetractPCollection<Integer, KV<Integer, String>> rhs =
        RetractPCollection.wrap(rhsRaw, ign -> 0L, ign -> true)
            .keyed(KV::getKey, TypeDescriptors.integers());

    PCollection<
            RetractElement<KV<KV<String, Integer>, KV<KV<String, Integer>, KV<Integer, String>>>>>
        joined =
            RetractMostRecentJoin.join(
                    lhs, rhs, KV::getValue, KV::getKey, TypeDescriptors.integers(), cleanupDuration)
                .unwrapped();
    PCollection<String> res =
        joined.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    e ->
                        String.format(
                            "%s:%d::%s:%d:%d:%s",
                            e.getValue().getKey().getKey(),
                            e.getValue().getKey().getValue(),
                            e.getValue().getValue().getKey().getKey(),
                            e.getValue().getValue().getKey().getValue(),
                            e.getValue().getValue().getValue().getKey(),
                            e.getValue().getValue().getValue().getValue())));

    PAssert.that(res).containsInAnyOrder("b:2::b:2:2:c", "a:1::a:1:1:b");
    p.run();
  }

  @Test
  public void testWithRetractions() {
    Pipeline p = Pipeline.create();
    Instant now = Instant.now();
    PCollection<RetractElement<KV<String, Integer>>> leftRetract =
        p.apply(
            Create.timestamped(
                    _shuffle(
                        _r(KV.of("a", 1), now, true),
                        _r(KV.of("b", 2), now, true),
                        _r(KV.of("a", 1), now.plus(1), false),
                        _r(KV.of("a", 3), now.plus(2), true), // last
                        _r(KV.of("b", 2), now.plus(3), false),
                        _r(KV.of("b", 4), now.plus(3), true))) // last
                .withCoder(
                    RetractElement.Coder.of(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))));

    PCollection<KV<Integer, String>> rhsRaw = p.apply(Create.of(KV.of(4, "x"), KV.of(3, "c")));

    KeyedRetractPCollection<String, KV<String, Integer>> lhs =
        RetractPCollection.fromRetractedElements(
                leftRetract,
                e -> e,
                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
            .keyed(KV::getKey, TypeDescriptors.strings());
    KeyedRetractPCollection<Integer, KV<Integer, String>> rhs =
        RetractPCollection.wrap(rhsRaw, ign -> 0L, ign -> true)
            .keyed(KV::getKey, TypeDescriptors.integers());

    PCollection<
            RetractElement<KV<KV<String, Integer>, KV<KV<String, Integer>, KV<Integer, String>>>>>
        joined =
            RetractMostRecentJoin.join(
                    lhs, rhs, KV::getValue, KV::getKey, TypeDescriptors.integers(), cleanupDuration)
                .unwrapped();
    PCollection<String> res =
        joined
            .apply(ReduceToLatest.of())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        e ->
                            String.format(
                                "%s:%d::%s:%d:%d:%s",
                                e.getKey().getKey(),
                                e.getKey().getValue(),
                                e.getValue().getKey().getKey(),
                                e.getValue().getKey().getValue(),
                                e.getValue().getValue().getKey(),
                                e.getValue().getValue().getValue())));

    PAssert.that(res).containsInAnyOrder("a:3::a:3:3:c", "b:4::b:4:4:x");
    p.run();
  }

  @Test
  public void testWithManyRetractions() {
    Pipeline p = Pipeline.create();
    Instant now = Instant.now();
    Pair<
            List<TimestampedValue<RetractElement<KV<String, Integer>>>>,
            List<TimestampedValue<RetractElement<KV<Integer, String>>>>>
        inputs = generateInputs(10000, 15, 15, now);
    PCollection<RetractElement<KV<String, Integer>>> leftRetract =
        p.apply(
            Create.timestamped(_shuffle(inputs.getFirst()))
                .withCoder(
                    RetractElement.Coder.of(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))));

    PCollection<RetractElement<KV<Integer, String>>> rightRetract =
        p.apply(
            Create.timestamped(_shuffle(inputs.getSecond()))
                .withCoder(
                    RetractElement.Coder.of(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))));

    KeyedRetractPCollection<String, KV<String, Integer>> lhs =
        RetractPCollection.fromRetractedElements(
                leftRetract,
                e -> e,
                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
            .keyed(KV::getKey, TypeDescriptors.strings());
    KeyedRetractPCollection<Integer, KV<Integer, String>> rhs =
        RetractPCollection.fromRetractedElements(
                rightRetract,
                e -> e,
                TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
            .keyed(KV::getKey, TypeDescriptors.integers());

    PCollection<
            RetractElement<KV<KV<String, Integer>, KV<KV<String, Integer>, KV<Integer, String>>>>>
        joined =
            RetractMostRecentJoin.join(
                    lhs, rhs, KV::getValue, KV::getKey, TypeDescriptors.integers(), cleanupDuration)
                .unwrapped();
    PCollection<String> res =
        joined
            .apply(ReduceToLatest.of())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        e ->
                            String.format(
                                "%s:%d::%s:%d:%d:%s",
                                e.getKey().getKey(),
                                e.getKey().getValue(),
                                e.getValue().getKey().getKey(),
                                e.getValue().getKey().getValue(),
                                e.getValue().getValue().getKey(),
                                e.getValue().getValue().getValue())));

    PAssert.that(res).containsInAnyOrder(computeOutputs(inputs));
    p.run();
  }

  private List<String> computeOutputs(
      Pair<
              List<TimestampedValue<RetractElement<KV<String, Integer>>>>,
              List<TimestampedValue<RetractElement<KV<Integer, String>>>>>
          inputs) {

    List<String> res = new ArrayList<>();
    List<TimestampedValue<KV<String, Integer>>> leftLatest = collectLatest(inputs.getFirst());
    List<TimestampedValue<KV<Integer, String>>> rightLatest = collectLatest(inputs.getSecond());
    for (TimestampedValue<KV<Integer, String>> right : rightLatest) {
      // for each right find all left with same number
      KV<Integer, String> rightKv = right.getValue();
      leftLatest.stream()
          .filter(l -> l.getValue().getValue().equals(rightKv.getKey()))
          .forEach(
              l -> {
                KV<String, Integer> leftKv = l.getValue();
                res.add(
                    String.format(
                        "%s:%d::%s:%d:%d:%s",
                        leftKv.getKey(),
                        rightKv.getKey(),
                        leftKv.getKey(),
                        leftKv.getValue(),
                        rightKv.getKey(),
                        rightKv.getValue()));
              });
    }
    return res;
  }

  static <K, V> List<TimestampedValue<KV<K, V>>> collectLatest(
      List<TimestampedValue<RetractElement<KV<K, V>>>> values) {

    Map<K, Pair<SequentialInstant, V>> current = new HashMap<>();
    for (TimestampedValue<RetractElement<KV<K, V>>> e : values) {
      RetractElement<KV<K, V>> value = e.getValue();
      KV<K, V> kv = value.getValue();
      SequentialInstant thisInstant =
          new SequentialInstant(e.getTimestamp(), e.getValue().getSeqId());
      @Nullable Pair<SequentialInstant, V> currentValue = current.get(kv.getKey());
      if (currentValue == null || currentValue.getFirst().compareTo(thisInstant) < 0) {
        if (value.isAddition()) {
          current.put(kv.getKey(), Pair.of(thisInstant, kv.getValue()));
        } else {
          current.put(kv.getKey(), Pair.of(thisInstant, null));
        }
      }
    }
    return current.entrySet().stream()
        .filter(e -> e.getValue().getSecond() != null)
        .map(
            e ->
                TimestampedValue.of(
                    KV.of(e.getKey(), e.getValue().getSecond()),
                    e.getValue().getFirst().getTimestamp()))
        .collect(Collectors.toList());
  }

  private Pair<
          List<TimestampedValue<RetractElement<KV<String, Integer>>>>,
          List<TimestampedValue<RetractElement<KV<Integer, String>>>>>
      generateInputs(int count, int leftN, int rightN, Instant base) {

    List<String> leftKeys =
        IntStream.range(0, leftN)
            .mapToObj(i -> String.valueOf((char) ('a' + i)))
            .collect(Collectors.toList());
    List<Integer> rightKeys =
        IntStream.range(0, rightN).mapToObj(i -> i + 1).collect(Collectors.toList());
    List<TimestampedValue<RetractElement<KV<String, Integer>>>> left = new ArrayList<>();
    List<TimestampedValue<RetractElement<KV<Integer, String>>>> right = new ArrayList<>();
    Random random = new Random(count);
    Map<String, Integer> currentLeft = new HashMap<>();
    Map<Integer, String> currentRight = new HashMap<>();
    for (int i = 0; i < count; i++) {
      String leftKey = leftKeys.get(random.nextInt(leftN));
      Integer rightKey = rightKeys.get(random.nextInt(rightN));
      if (random.nextBoolean() && currentLeft.containsKey(leftKey)) {
        // retract
        left.add(
            TimestampedValue.of(
                RetractElement.ofDeletion(KV.of(leftKey, currentLeft.get(leftKey)), 0L),
                base.plus(i)));
      } else {
        @Nullable Integer oldValue = currentLeft.put(leftKey, rightKey);
        if (oldValue != null) {
          // retract the old value
          left.add(
              TimestampedValue.of(
                  RetractElement.ofDeletion(KV.of(leftKey, oldValue), 0L), base.plus(i)));
        }
        left.add(
            TimestampedValue.of(
                RetractElement.ofAddition(KV.of(leftKey, rightKey), 1L), base.plus(i)));
      }

      // regenerate keys
      leftKey = leftKeys.get(random.nextInt(leftN));
      rightKey = rightKeys.get(random.nextInt(rightN));

      if (random.nextBoolean() && currentRight.containsKey(rightKey)) {
        // retract
        right.add(
            TimestampedValue.of(
                RetractElement.ofDeletion(KV.of(rightKey, currentRight.get(rightKey)), 0L),
                base.plus(i)));
      } else {
        String oldValue = currentRight.put(rightKey, leftKey);
        if (oldValue != null) {
          // retract the old value
          right.add(
              TimestampedValue.of(
                  RetractElement.ofDeletion(KV.of(rightKey, oldValue), 0L), base.plus(i)));
        }
        right.add(
            TimestampedValue.of(
                RetractElement.ofAddition(KV.of(rightKey, leftKey), 1L), base.plus(i)));
      }
    }

    return Pair.of(left, right);
  }

  @SafeVarargs
  private <T> List<T> _shuffle(T... values) {
    return _shuffle(Arrays.asList(values));
  }

  private <T> List<T> _shuffle(List<T> values) {
    List<T> res = new ArrayList<>(values);
    Collections.shuffle(res);
    return res;
  }

  public <T> TimestampedValue<RetractElement<T>> _r(T val, Instant ts, boolean addition) {
    return _r(val, ts, 0L, addition);
  }

  public <T> TimestampedValue<RetractElement<T>> _r(
      T val, Instant ts, long seqId, boolean addition) {
    return TimestampedValue.of(
        addition ? RetractElement.ofAddition(val, seqId) : RetractElement.ofDeletion(val, seqId),
        ts);
  }

  private static class ReduceToLatest<K, V>
      extends PTransform<PCollection<RetractElement<KV<K, V>>>, PCollection<KV<K, V>>> {

    public static <K, V> ReduceToLatest<K, V> of() {
      return new ReduceToLatest<>();
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<RetractElement<KV<K, V>>> input) {
      RetractElement.Coder<KV<K, V>> inputCoder = (RetractElement.Coder<KV<K, V>>) input.getCoder();
      KvCoder<K, V> valueCoder = (KvCoder<K, V>) inputCoder.getValueCoder();
      PCollection<KV<K, RetractElement<V>>> mapped =
          input
              .apply(
                  MapElements.into(
                          TypeDescriptors.kvs(
                              valueCoder.getKeyCoder().getEncodedTypeDescriptor(),
                              new TypeDescriptor<RetractElement<V>>() {}.where(
                                  new TypeParameter<>() {},
                                  valueCoder.getValueCoder().getEncodedTypeDescriptor())))
                      .via(
                          r ->
                              KV.of(
                                  r.getValue().getKey(),
                                  r.isAddition()
                                      ? RetractElement.ofAddition(
                                          r.getValue().getValue(), r.getSeqId())
                                      : RetractElement.ofDeletion(
                                          r.getValue().getValue(), r.getSeqId()))))
              .setCoder(
                  KvCoder.of(
                      valueCoder.getKeyCoder(),
                      RetractElement.Coder.of(valueCoder.getValueCoder())));
      return mapped.apply(ParDo.of(new ReduceToLatestFn(valueCoder))).setCoder(valueCoder);
    }

    private class ReduceToLatestFn extends DoFn<KV<K, RetractElement<V>>, KV<K, V>> {

      @StateId("cache")
      private final StateSpec<ValueState<KV<SequentialInstant, V>>> cache;

      public ReduceToLatestFn(KvCoder<K, V> inputCoder) {
        this.cache =
            StateSpecs.value(
                KvCoder.of(
                    new SequentialInstant.Coder(), NullableCoder.of(inputCoder.getValueCoder())));
      }

      @ProcessElement
      public void process(
          @Element KV<K, RetractElement<V>> elem,
          @Timestamp Instant ts,
          @StateId("cache") ValueState<KV<SequentialInstant, V>> cache) {

        KV<SequentialInstant, V> cached = cache.read();
        SequentialInstant thisInstant = new SequentialInstant(ts, elem.getValue().getSeqId());
        if (cached == null || cached.getKey().compareTo(thisInstant) < 0) {
          cache.write(
              KV.of(thisInstant, elem.getValue().isAddition() ? elem.getValue().getValue() : null));
        }
      }

      @OnWindowExpiration
      public void flush(
          @Key K key,
          @StateId("cache") ValueState<KV<SequentialInstant, V>> cache,
          OutputReceiver<KV<K, V>> output) {

        KV<SequentialInstant, V> read = Objects.requireNonNull(cache.read());
        if (read.getValue() != null) {
          output.output(KV.of(key, read.getValue()));
        }
      }
    }
  }
}
