/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamTest;
import cz.o2.proxima.tools.groovy.TestStreamProvider;
import java.util.List;
import org.apache.beam.sdk.transforms.Create;

public class BeamStreamTest extends StreamTest {

  public BeamStreamTest() {
    super(provider());
  }

  static TestStreamProvider provider() {
    return new TestStreamProvider() {
      @Override
      public <T> Stream<T> of(List<T> values) {
        return new BeamStream<>(true, pipeline ->
          pipeline.apply(Create.of(values)));
      }
    };
  }

}
