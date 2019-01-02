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
package cz.o2.proxima.tools.groovy;

import com.google.common.collect.Sets;
import groovy.lang.Closure;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test suite for {@link Stream}.
 */
public class StreamTest extends AbstractStreamTest {

  @Test
  public void testStreamFilter() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    stream
        .filter(new Closure<Boolean>(this) {
          @Override
          public Boolean call(Object... args) {
            return (int) args[0] % 2 == 0;
          }
        })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(2, 4), result);
  }

  @Test
  public void testStreamMap() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    stream
        .map(new Closure<Integer>(this) {
          @Override
          public Integer call(Object... args) {
            return (int) args[0] + 1;
          }
        })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(2, 3, 4, 5), result);
  }

  @Test
  public void testStreamUnion() {
    Stream<Integer> stream1 = Stream.wrap(executor(), builder(1, 2), () -> { });
    Stream<Integer> stream2 = Stream.wrap(executor(), builder(3, 4), () -> { });
    Set<Object> result = new HashSet<>();
    stream1.union(stream2)
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Sets.newHashSet(1, 2, 3, 4), result);
  }

  @Test
  public void testCollect() {
    Stream<Integer> stream1 = Stream.wrap(executor(), builder(1, 2), () -> { });
    Stream<Integer> stream2 = Stream.wrap(executor(), builder(3, 4), () -> { });
    Set<Integer> result = stream1.union(stream2)
        .collect()
        .stream().collect(Collectors.toSet());
    assertEquals(Sets.newHashSet(1, 2, 3, 4), result);
  }


}
