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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class RetractInnerJoinTest {

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
            RetractInnerJoin.join(lhs, rhs, KV::getValue, KV::getKey, TypeDescriptors.integers())
                .unwrappedValues();
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
}
