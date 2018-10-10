/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.util.Pair;
import groovy.lang.Closure;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Abstract base class for windowed streams test.
 */
abstract class AbstractWindowedStreamTest extends StreamTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllReduce() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .reduce(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            },
            1,
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(Pair.of(0, 11)), result);
  }

  @Test
  public void testWindowAllReduceWithValue() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .reduce(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            },
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument + 1;
              }
            },
            1,
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(Pair.of(0, 15)), result);
  }

  @Test
  public void testWindowAllFlatReduce() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .flatReduce(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            },
            new Closure<Object>(this) {
              @Override
              public Object call(Object... arguments) {
                return Arrays.asList(arguments);
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    // this is because how groovy closures works for different types
    assertEquals(2, result.size());
    assertEquals(Pair.of(0, Arrays.asList(1, 2, 3, 4)), result.get(1));
  }

  @Test
  public void testWindowAllCombine() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .combine(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            },
            0, /* if this is non-zero element, then the result is undefined */
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(Pair.of(0, 10)), result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllCombineWithValue() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .combine(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            },
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument + 1;
              }
            },
            0, /* if this is non-zero element, then the result is undefined */
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object... args) {
                return (int) args[0] + (int) args[1];
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(Pair.of(0, 14)), result);
  }

  @Test
  public void testWindowAllCountByKey() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .countByKey(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return 0;
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(Pair.of(0, 4L)), result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllAverage() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .average(
            new Closure<Double>(this) {
              @Override
              public Double call(Object argument) {
                return (double) ((int) argument + 1);
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(14 / 4.), result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllAverageByKey() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    Set<Object> result = new HashSet<>();
    intoSingleWindow(stream)
        .averageByKey(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument % 2;
              }
            },
            new Closure<Double>(this) {
              @Override
              public Double call(Object argument) {
                return (double) ((int) argument + 1);
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add((Pair<Integer, Double>) argument);
            return null;
          }
        });
    assertEquals(Sets.newHashSet(
        Pair.of(0, 4.0),
        Pair.of(1, 3.0)),
        result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJoin() {
    Stream<Integer> stream1 = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    Stream<Integer> stream2 = Stream.wrap(executor(), builder(3, 4), () -> { });
    Closure<Integer> keyExtractor = new Closure<Integer>(this) {

      @Override
      public Integer call(Object argument) {
        return (int) argument % 2;
      }

    };
    Set<Object> result = new HashSet<>();
    intoSingleWindow(stream1)
        .join(intoSingleWindow(stream2), keyExtractor, keyExtractor)
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Sets.newHashSet(
        Pair.of(2, 4),
        Pair.of(4, 4),
        Pair.of(1, 3),
        Pair.of(3, 3)),
        result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLeftJoin() {
    Stream<Integer> stream1 = Stream.wrap(executor(), builder(3), () -> { });
    Stream<Integer> stream2 = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    Closure<Integer> keyExtractor = new Closure<Integer>(this) {

      @Override
      public Integer call(Object argument) {
        return (int) argument % 2;
      }

    };
    Set<Object> result = new HashSet<>();
    intoSingleWindow(stream1)
        .leftJoin(    intoSingleWindow(stream2), keyExtractor, keyExtractor)
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Sets.newHashSet(
        Pair.of(3, 3),
        Pair.of(3, 1)),
        result);
  }

  @Test
  public void testWindowAllSorted() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(4, 3, 2, 1), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .sorted()
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(
        Arrays.asList(1, 2, 3, 4),
        result);
  }

  @Test
  public void testWindowAllSortedWithComparator() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .sorted(new Closure<Integer>(this) {
          @Override
          public Integer call(Object... args) {
            // reversed
            return Integer.compare((int) args[1], (int) args[0]);
          }
        })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(
        Arrays.asList(4, 3, 2, 1),
        result);
  }

  @Test
  public void testWindowAllCount() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(4, 3, 2, 1), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .count()
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(
        Arrays.asList(4L),
        result);
  }

  @Test
  public void testWindowAllSum() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(4, 3, 2, 1), () -> { });
    List<Object> result = new ArrayList<>();
    intoSingleWindow(stream)
        .sum(new Closure<Double>(this) {
          @Override
          public Double call(Object argument) {
            return (double) (int) argument;
          }
        })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(
        Arrays.asList(10.0),
        result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllSumByKey() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(4, 3, 2, 1), () -> { });
    Set<Object> result = new HashSet<>();
    intoSingleWindow(stream)
        .sumByKey(
            new Closure<Integer>(this) {
              @Override
              public Integer call(Object argument) {
                return (int) argument % 2;
              }
            },
            new Closure<Double>(this) {
              @Override
              public Double call(Object argument) {
                return (double) (int) argument;
              }
            })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(Sets.newHashSet(
        Pair.of(0, 6.0),
        Pair.of(1, 4.0)),
        result);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testStreamWindowAllDontAffectStatelessOperations() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(1, 2, 3, 4), () -> { });
    List<Integer> result = new ArrayList<>();
    intoSingleWindow(stream)
        .filter(new Closure<Boolean>(this) {
          @Override
          public Boolean call(Object... args) {
            return (int) args[0] % 2 == 0;
          }
        })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add((int) argument);
            return null;
          }
        });
    assertEquals(Arrays.asList(2, 4), result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllDistinct() {
    Stream<Integer> stream = Stream.wrap(executor(), builder(
        4, 3, 2, 1, 1, 2, 3), () -> { });
    Set<Object> result = new HashSet<>();
    intoSingleWindow(stream)
        .distinct()
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(
        Sets.newHashSet(1, 2, 3, 4),
        result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWindowAllDistinctWithMapper() {
    Stream<Object> stream = Stream.wrap(executor(), builder(
        4, 3, 2, 1, "1", "2", "3"), () -> { });
    Set<Object> result = new HashSet<>();
    intoSingleWindow(stream)
        .distinct(new Closure<Integer>(this) {
          @Override
          public Integer call(Object argument) {
            return Integer.valueOf(argument.toString());
          }
        })
        .map(new Closure<String>(this) {
          @Override
          public String call(Object argument) {
            return argument.toString();
          }
        })
        .forEach(new Closure<Void>(this) {
          @Override
          public Void call(Object argument) {
            result.add(argument);
            return null;
          }
        });
    assertEquals(
        Sets.newHashSet("1", "2", "3", "4"),
        result);
  }


  /**
   * Pack this stream into single window by whatever strategy chosen.
   */
  abstract <T, W extends Windowing<T, ?>> WindowedStream<T, W> intoSingleWindow(
      Stream<T> stream);

}
