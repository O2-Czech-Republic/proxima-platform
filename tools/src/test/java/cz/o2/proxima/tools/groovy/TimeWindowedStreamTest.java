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
package cz.o2.proxima.tools.groovy;

import cz.o2.proxima.tools.groovy.util.Closures;

/** Test suite for {@link WindowedStream} with time window. */
public abstract class TimeWindowedStreamTest extends AbstractWindowedStreamTest {

  protected TimeWindowedStreamTest(TestStreamProvider provider) {
    super(provider);
  }

  @SuppressWarnings("unchecked")
  @Override
  <T> WindowedStream<T> intoSingleWindow(Stream<T> stream) {
    return stream.assignEventTime(Closures.from(this, tmp -> 1L)).timeWindow(1000L);
  }
}
