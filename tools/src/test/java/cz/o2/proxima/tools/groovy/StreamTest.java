/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.tools.groovy.util.Closures;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Test;

/** Test suite for {@link Stream}. */
public abstract class StreamTest extends AbstractStreamTest {

  protected StreamTest(TestStreamProvider provider) {
    super(provider);
  }

  @Test
  public void testStreamFilter() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Integer> result = stream.filter(wrap(arg -> (int) arg % 2 == 0, Boolean.class)).collect();
    assertUnorderedEquals(result, 2, 4);
  }

  @Test
  public void testStreamMap() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Integer> result = stream.map(wrap(arg -> (int) arg + 1, Integer.class)).collect();
    assertUnorderedEquals(result, 2, 3, 4, 5);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamWithWindow() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Pair> result =
        stream
            .windowAll()
            .withWindow()
            .map(
                wrap(
                    arg -> Pair.of("window", ((Pair<Object, Integer>) arg).getSecond()),
                    Pair.class))
            .collect();
    assertUnorderedEquals(
        result,
        Pair.of("window", 1),
        Pair.of("window", 2),
        Pair.of("window", 3),
        Pair.of("window", 4));
  }

  @Test
  public void testStreamUnion() {
    Stream<Integer> stream1 = stream(1, 2);
    Stream<Integer> stream2 = stream(3, 4);
    Set<Integer> result = stream1.union(stream2).collect().stream().collect(Collectors.toSet());
    assertEquals(Sets.newHashSet(1, 2, 3, 4), result);
  }

  @Test
  public void testCollect() {
    Stream<Integer> stream1 = stream(1, 2);
    Stream<Integer> stream2 = stream(3, 4);
    Set<Integer> result = stream1.union(stream2).collect().stream().collect(Collectors.toSet());
    assertEquals(Sets.newHashSet(1, 2, 3, 4), result);
  }

  @SafeVarargs
  final <T> void assertUnorderedEquals(List<T> input, T... elements) {
    assertEquals(elementCounts(Arrays.stream(elements)), elementCounts(input.stream()));
  }

  private <T> Map<T, Integer> elementCounts(java.util.stream.Stream<T> input) {
    return input.collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(e -> 1)));
  }

  <T> Closure<T> wrap(Factory<T> f, Class<? extends T> cls) {
    return JavaTypedClosure.wrap(Closures.from(this, f), cls);
  }

  <T> Closure<T> wrap(UnaryFunction<Object, T> f, Class<? extends T> cls) {
    return JavaTypedClosure.wrap(Closures.from(this, f), cls);
  }

  <T> Closure<T> wrapArray(UnaryFunction<Object[], T> f, Class<? extends T> cls) {
    return JavaTypedClosure.wrap(Closures.fromArray(this, f), cls);
  }

  <T> Closure<T> wrap(BiFunction<Object, Object, T> f, Class<? extends T> cls) {
    return JavaTypedClosure.wrap(Closures.from(this, f), cls);
  }
}
