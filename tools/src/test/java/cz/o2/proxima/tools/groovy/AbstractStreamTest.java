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
package cz.o2.proxima.tools.groovy;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Base class for tests of all stream classes. */
abstract class AbstractStreamTest implements Serializable {

  private static final long serialVersionUID = 1L;

  final transient TestStreamProvider provider;

  protected AbstractStreamTest(TestStreamProvider provider) {
    this.provider = provider;
  }

  @SafeVarargs
  final <T> Stream<T> stream(T... items) {
    return provider.of(Arrays.stream(items).collect(Collectors.toList()));
  }
}
