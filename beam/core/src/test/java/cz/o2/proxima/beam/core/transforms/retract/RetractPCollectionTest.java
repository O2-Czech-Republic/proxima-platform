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

import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class RetractPCollectionTest {

  @Test
  public void testSimplePipeline() {
    Pipeline p = Pipeline.create();
    PCollection<String> input = p.apply(Create.of("a", "b", "c"));
    RetractPCollection<String> retractPColl = input.apply(IntoRetraction.of());
    KeyedRetractPCollection<Character, String> keyed =
        retractPColl.keyed(s -> s.charAt(0), TypeDescriptors.characters());
    PCollection<String> res =
        keyed
            .unwrapped()
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.characters(), TypeDescriptors.strings()))
                    .via(
                        e -> {
                          assert e != null;
                          return e.getValue();
                        }))
            .apply(MapElements.into(TypeDescriptors.strings()).via(KV::getValue));
    PAssert.that(res).containsInAnyOrder("a", "b", "c");
    assertNotNull(p.run());
  }

  private static class IntoRetraction<T> extends PTransform<PCollection<T>, RetractPCollection<T>> {

    public static <T> IntoRetraction<T> of() {
      return new IntoRetraction<>();
    }

    @Override
    public RetractPCollection<T> expand(PCollection<T> input) {
      return RetractPCollection.wrap(input, e -> 0L, e -> false);
    }
  }

  private static class FromRetraction<T> extends PTransform<RetractPCollection<T>, PCollection<T>> {

    static <T> FromRetraction<T> of() {
      return new FromRetraction<>();
    }

    @Override
    public PCollection<T> expand(RetractPCollection<T> input) {

      return input
          .unwrapped()
          .apply(MapElements.into(input.getDescriptor()).via(v -> v.getValue()));
    }
  }
}
