/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.tools.groovy.util.Closures;
import cz.o2.proxima.util.Pair;
import groovy.lang.GString;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.groovy.runtime.GStringImpl;
import org.junit.Test;

public class GStringSerializerTest {

  @Test
  public void testPipelineWithGString() {
    Pipeline p = Pipeline.create();
    PCollection<GStringImpl> input =
        p.apply(Create.of(new GStringImpl(new Object[] {}, new String[] {"str"})));
    BeamStream<GStringImpl> stream = BeamStream.wrap(input);

    try {
      @SuppressWarnings("unchecked")
      List<Pair<Integer, GString>> result =
          stream
              .map(Closures.from(this, e -> Pair.of(1, (GString) e)))
              .windowAll()
              .groupReduce(
                  Closures.from(this, pair -> ((Pair<Integer, GString>) pair).getFirst()),
                  Closures.from(
                      this,
                      (window, values) ->
                          ((List<Pair<Integer, GString>>) values)
                              .stream()
                              .map(Pair::getSecond)
                              .collect(Collectors.toList())))
              .collect();
      assertEquals(
          Collections.singletonList(Pair.of(1, "str")),
          result
              .stream()
              .map(pair -> Pair.of(pair.getFirst(), pair.getSecond().toString()))
              .collect(Collectors.toList()));
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }
  }
}
