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
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Abstract base class for windowed streams test.
 */
public abstract class AbstractWindowedStreamTest extends StreamTest {

  protected AbstractWindowedStreamTest(TestStreamProvider provider) {
    super(provider);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllReduce() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Pair<Integer, Integer>> result = intoSingleWindow(stream)
        .reduce(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            }, Integer.class),
            1,
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            }, Integer.class))
        .collect();
    assertUnorderedEquals(result, Pair.of(0, 11));
  }

  @Test
  public void testWindowAllReduceWithValue() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Pair<Integer, Integer>> result =  intoSingleWindow(stream)
        .reduce(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            }, Integer.class),
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument + 1;
              }
            }, Integer.class),
            1,
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            }, Integer.class))
        .collect();
    assertUnorderedEquals(result, Pair.of(0, 15));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllGroupReduce() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Pair<Integer, List>> result = intoSingleWindow(stream)
        .groupReduce(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            }, Integer.class),
            wrap(new Closure<List>(this) {
              @Override
              public List call(Object... arguments) {
                return Arrays.stream(arguments).collect(Collectors.toList());
              }
            }, List.class))
        .collect();
    assertEquals(1, result.size());
    assertEquals(
        Sets.newHashSet(1, 2, 3, 4),
        Sets.newHashSet(((Iterable) result.get(0).getSecond().get(1))));
  }

  @Test
  public void testWindowAllCombine() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Pair<Integer, Integer>> result = intoSingleWindow(stream)
        .combine(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            }, Integer.class),
            0, /* if this is non-zero element, then the result is undefined */
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            }, Integer.class))
        .collect();

    assertUnorderedEquals(result, Pair.of(0, 10));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllCombineWithValue() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Pair<Integer, Integer>> result =  intoSingleWindow(stream)
        .combine(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            }, Integer.class),
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument + 1;
              }
            }, Integer.class),
            0, /* if this is non-zero element, then the result is undefined */
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            }, Integer.class))
        .collect();
    assertEquals(Arrays.asList(Pair.of(0, 14)), result);
  }

  @Test
  public void testWindowAllCountByKey() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Pair<Integer, Long>> result = intoSingleWindow(stream)
        .countByKey(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            }, Integer.class))
        .collect();
    assertEquals(Arrays.asList(Pair.of(0, 4L)), result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllAverage() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Double> result = intoSingleWindow(stream)
        .average(
            wrap(new Closure<Double>(this) {
              @Override
              public Double call(Object argument) {
                return (double) ((int) argument + 1);
              }
            }, Double.class))
        .collect();
    assertEquals(Arrays.asList(14 / 4.), result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllAverageByKey() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    Set<Pair<Integer, Double>> result = intoSingleWindow(stream)
        .averageByKey(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument % 2;
              }
            }, Integer.class),
            wrap(new Closure<Double>(this) {
              @Override
              public Double call(Object argument) {
                return (double) ((int) argument + 1);
              }
            }, Double.class))
        .collect()
        .stream()
        .collect(Collectors.toSet());

    assertEquals(Sets.newHashSet(
        Pair.of(0, 4.0),
        Pair.of(1, 3.0)),
        result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJoin() {
    Stream<Integer> stream1 = stream(1, 2, 3, 4);
    Stream<Integer> stream2 = stream(3, 4);
    Closure<Integer> keyExtractor = wrap(new Closure<Integer>(this) {
      @Override
      public Integer call(Object argument) {
        return (int) argument % 2;
      }
    }, Integer.class);

    List<Pair<Integer, Integer>> result = intoSingleWindow(stream1)
        .join(intoSingleWindow(stream2), keyExtractor, keyExtractor)
        .collect();

    assertUnorderedEquals(
        result,
        Pair.of(2, 4),
        Pair.of(4, 4),
        Pair.of(1, 3),
        Pair.of(3, 3));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLeftJoin() {
    Stream<Integer> stream1 = stream(3);
    Stream<Integer> stream2 = stream(1, 2, 3, 4);
    Closure<Integer> keyExtractor = wrap(new Closure<Integer>(this) {

      @Override
      public Integer call(Object argument) {
        return (int) argument % 2;
      }

    }, Integer.class);
    Set<Object> result = intoSingleWindow(stream1)
        .leftJoin(    intoSingleWindow(stream2), keyExtractor, keyExtractor)
        .collect()
        .stream()
        .collect(Collectors.toSet());

    assertEquals(Sets.newHashSet(
        Pair.of(3, 3),
        Pair.of(3, 1)),
        result);
  }

  @Test
  public void testWindowAllSorted() {
    Stream<Integer> stream = stream(4, 3, 2, 1);
    List<Comparable<Integer>> result = intoSingleWindow(stream)
        .sorted()
        .collect();

    assertEquals(
        Arrays.asList(1, 2, 3, 4),
        result);
  }

  @Test
  public void testWindowAllSortedWithComparator() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Integer> result = intoSingleWindow(stream)
        .sorted(wrap(new Closure<Integer>(this) {
          @Override
          public Integer call(Object... args) {
            // reversed
            return Integer.compare((int) args[1], (int) args[0]);
          }
        }, Integer.class))
        .collect();

    assertEquals(
        Arrays.asList(4, 3, 2, 1),
        result);
  }

  @Test
  public void testWindowAllCount() {
    Stream<Integer> stream = stream(4, 3, 2, 1);
    List<Long> result = intoSingleWindow(stream)
        .count()
        .collect();

    assertUnorderedEquals(result, 4L);
  }

  @Test
  public void testWindowAllSum() {
    Stream<Integer> stream = stream(4, 3, 2, 1);
    List<Double> result = intoSingleWindow(stream)
        .sum(wrap(new Closure<Double>(this) {
          @Override
          public Double call(Object argument) {
            return (double) (int) argument;
          }
        }, Double.class))
        .collect();

    assertUnorderedEquals(
        result,
        10.0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllSumByKey() {
    Stream<Integer> stream = stream(4, 3, 2, 1);
    Set<Pair<Integer, Double>> result = intoSingleWindow(stream)
        .sumByKey(
            wrap(new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument % 2;
              }
            }, Integer.class),
            wrap(new Closure<Double>(this) {
              @Override
              public Double call(Object argument) {
                return (double) (int) argument;
              }
            }, Double.class))
        .collect()
        .stream()
        .collect(Collectors.toSet());

    assertEquals(Sets.newHashSet(
        Pair.of(0, 6.0),
        Pair.of(1, 4.0)),
        result);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testStreamWindowAllDontAffectStatelessOperations() {
    Stream<Integer> stream = stream(1, 2, 3, 4);
    List<Integer> result = intoSingleWindow(stream)
        .filter(wrap(new Closure<Boolean>(this) {
          @Override
          public Boolean call(Object... args) {
            return (int) args[0] % 2 == 0;
          }
        }, Boolean.class))
        .collect();
    assertUnorderedEquals(result, 2, 4);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllDistinct() {
    Stream<Integer> stream = stream(4, 3, 2, 1, 1, 2, 3);
    Set<Object> result = intoSingleWindow(stream)
        .distinct()
        .collect()
        .stream()
        .collect(Collectors.toSet());
    assertEquals(
        Sets.newHashSet(1, 2, 3, 4),
        result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllDistinctWithMapper() {
    Stream<Object> stream = stream("4", "3", "2", "1", "1.", "2.", "3.");
    Set<Object> result = intoSingleWindow(stream)
        .distinct(wrap(new Closure<Integer>(this) {
          @Override
          public Integer call(Object argument) {
            return Integer.valueOf(argument.toString().substring(0, 1));
          }
        }, Integer.class))
        .map(wrap(new Closure<String>(this) {
          @Override
          public String call(Object argument) {
            return argument.toString().substring(0, 1);
          }
        }, String.class))
        .collect()
        .stream()
        .collect(Collectors.toSet());

    assertEquals(
        Sets.newHashSet("1", "2", "3", "4"),
        result);
  }


  /**
   * Pack this stream into single window by whatever strategy chosen.
   */
  abstract <T> WindowedStream<T> intoSingleWindow(Stream<T> stream);

}
