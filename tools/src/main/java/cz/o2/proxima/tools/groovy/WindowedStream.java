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

import com.google.common.collect.Lists;
import cz.o2.proxima.tools.io.TypedIngest;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.Sort;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import groovy.lang.Closure;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A stream that is windowed.
 */
public class WindowedStream<T> extends Stream<T> {

  @SuppressWarnings("unchecked")
  private static class JoinedWindowing<L, R> implements Windowing<Either<L, R>, Window> {

    private final  Windowing left;
    private final Windowing right;

    private JoinedWindowing(WindowedStream left, WindowedStream right) {
      this.left = left.windowing;
      this.right = right.windowing;
    }

    @Override
    public Iterable<Window> assignWindowsToElement(WindowedElement<?, Either<L, R>> el) {
      if (el.getElement().isLeft()) {
        return (Iterable) left.assignWindowsToElement(
            new WindowedElement() {
              @Override
              public Window getWindow() {
                return el.getWindow();
              }

              @Override
              public long getTimestamp() {
                return el.getTimestamp();
              }

              @Override
              public Object getElement() {
                return el.getElement().left();
              }
            });
      }

      return (Iterable) right.assignWindowsToElement(
          new WindowedElement() {
            @Override
            public Window getWindow() {
              return el.getWindow();
            }

            @Override
            public long getTimestamp() {
              return el.getTimestamp();
            }

            @Override
            public Object getElement() {
              return el.getElement().right();
            }
          });
    }

    @Override
    public Trigger<Window> getTrigger() {
      return new Trigger<Window>() {
        @Override
        public boolean isStateful() {
          return left.getTrigger().isStateful() || right.getTrigger().isStateful();
        }

        @Override
        public Trigger.TriggerResult onElement(
            long time, Window window, TriggerContext ctx) {

          return toResult(
              left.getTrigger().onElement(time, window, ctx),
              right.getTrigger().onElement(time, window, ctx));

        }

        private Trigger.TriggerResult toResult(
            Trigger.TriggerResult leftTrigger,
            Trigger.TriggerResult rightTrigger) {

          if ((leftTrigger.isFlush() || rightTrigger.isFlush()) && (leftTrigger.isPurge() || rightTrigger.isPurge())) {
            return Trigger.TriggerResult.FLUSH_AND_PURGE;
          }
          if (leftTrigger.isFlush() || rightTrigger.isFlush()) {
            return Trigger.TriggerResult.FLUSH;
          }
          if (leftTrigger.isPurge() || rightTrigger.isPurge()) {
            return Trigger.TriggerResult.PURGE;
          }
          return Trigger.TriggerResult.NOOP;
        }

        @Override
        public Trigger.TriggerResult onTimer(long time, Window window, TriggerContext ctx) {
          return toResult(
              left.getTrigger().onTimer(time, window, ctx),
              right.getTrigger().onTimer(time, window, ctx));

        }

        @Override
        public void onClear(Window window, TriggerContext ctx) {
          left.getTrigger().onClear(window, ctx);
          right.getTrigger().onClear(window, ctx);
        }

        @Override
        public void onMerge(Window window, TriggerContext.TriggerMergeContext ctx) {
          left.getTrigger().onMerge(window, ctx);
          right.getTrigger().onMerge(window, ctx);
        }

      };
    }
  }

  final Windowing<T, Window> windowing;

  public WindowedStream(
      Executor executor,
      DatasetBuilder<T> dataset,
      Windowing<T, Window> windowing,
      Runnable terminatingOperationCall) {

    super(executor, dataset, terminatingOperationCall);
    this.windowing = windowing;
  }


  @Override
  @SuppressWarnings("unchecked")
  <X> WindowedStream<X> descendant(DatasetBuilder<X> dataset) {
    return new WindowedStream(
        executor,
        dataset,
        windowing,
        terminatingOperationCall);
  }

  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initialValue,
      Closure<V> reducer) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> valueDehydrated = valueExtractor.dehydrate();
    Closure<V> reducerDehydrated = reducer.dehydrate();
    return (WindowedStream) descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .valueBy(valueDehydrated::call)
          .reduceBy((Iterable<V> in) -> {
            V current = initialValue;
            for (V v : in) {
              current = reducerDehydrated.call(current, v);
            }
            return current;
          })
          .windowBy(windowing)
          .output();
    });
  }


  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor,
      V initialValue,
      Closure<V> reducer) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> reducerDehydrated = reducer.dehydrate();
    return (WindowedStream) descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .reduceBy((Iterable<T> in) -> {
            V current = initialValue;
            for (T v : in) {
              current = reducerDehydrated.call(current, v);
            }
            return current;
          })
          .windowBy(windowing)
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <T> WindowedStream<TypedIngest<T>> reduceToLatest() {
    return descendant(() -> {
      Dataset<TypedIngest<T>> input = (Dataset<TypedIngest<T>>) dataset.build();
      Dataset<Pair<Pair<String, String>, TypedIngest<T>>> reduced = ReduceByKey.of(input)
          .keyBy(i -> Pair.of(i.getKey(), i.getAttribute()))
          .combineBy(values ->
              StreamSupport.stream(values.spliterator(), false)
                  .collect(Collectors.maxBy((a, b) -> Long.compare(a.getStamp(), b.getStamp())))
                  .get())
          .output();
      return MapElements.of(reduced)
          .using(Pair::getSecond)
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>> flatReduce(
      Closure<K> keyExtractor,
      Closure<V> listReduce) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> reducerDehydrated = listReduce.dehydrate();

    return descendant(() -> {
        return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .reduceBy((Iterable<T> in, Collector<V> ctx) -> {
            List<V> ret = (List<V>) reducerDehydrated.call(
                ctx.getWindow(), Lists.newArrayList(in));
            ret.forEach(elem -> ctx.collect(elem));
          })
          .windowBy(windowing)
          .output();
        });
  }

  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>> combine(
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initial,
      Closure<V> combine) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> valueDehydrated = valueExtractor.dehydrate();
    Closure<V> combineDehydrated = combine.dehydrate();
    return descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .valueBy(valueDehydrated::call)
          .combineBy((Iterable<V> iter) -> {
            V ret = initial;
            for (V v : iter) {
              ret = combineDehydrated.call(ret, v);
            }
            return ret;
          })
          .windowBy(windowing)
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <K> WindowedStream<Pair<K, T>> combine(
      Closure<K> keyExtractor,
      T initial,
      Closure<T> combine) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<T> combineDehydrated = combine.dehydrate();
    return descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .combineBy((Iterable<T> iter) -> {
            T ret = initial;
            for (T v : iter) {
              ret = combineDehydrated.call(ret, v);
            }
            return ret;
          })
          .windowBy(windowing)
          .output();
    });
  }

  public <K> WindowedStream<Pair<K, Long>> countByKey(Closure<K> keyExtractor) {
    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    return descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .valueBy(e -> 1L)
          .combineBy(Sums.ofLongs())
          .windowBy(windowing)
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <KEY, RIGHT> WindowedStream<Pair<T, RIGHT>> join(
      WindowedStream<RIGHT> right,
      Closure<?> leftKey,
      Closure<?> rightKey) {

    Closure<?> leftKeyDehydrated = leftKey.dehydrate();
    Closure<?> rightKeyDehydrated = rightKey.dehydrate();
    return descendant(() -> {
      Dataset<Pair<Object, Pair<T, RIGHT>>> joined = Join.of(dataset.build(), right.dataset.build())
          .by(leftKeyDehydrated::call, rightKeyDehydrated::call)
          .using((T l, RIGHT r, Collector<Pair<T, RIGHT>> ctx) -> {
            ctx.collect(Pair.of(l, r));
          })
          .windowBy(new JoinedWindowing<>(this, right))
          .output();
      return MapElements.of(joined)
          .using(Pair::getSecond)
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <KEY, RIGHT> WindowedStream<Pair<T, RIGHT>> leftJoin(
      WindowedStream<RIGHT> right,
      Closure<?> leftKey,
      Closure<?> rightKey) {

    Closure<?> leftKeyDehydrated = leftKey.dehydrate();
    Closure<?> rightKeyDehydrated = rightKey.dehydrate();
    return descendant(() -> {
      Dataset<Pair<Object, Pair<T, RIGHT>>> joined = Join.of(dataset.build(), right.dataset.build())
          .by(leftKeyDehydrated::call, rightKeyDehydrated::call)
          .using((T l, RIGHT r, Collector<Pair<T, RIGHT>> ctx) -> {
            ctx.collect(Pair.of(l, r));
          })
          .outer()
          .windowBy(new JoinedWindowing<>(this, right))
          .output();
      return MapElements.of(joined)
          .using(Pair::getSecond)
          .output();
    });
  }


  @SuppressWarnings("unchecked")
  public <S extends Comparable<S>> WindowedStream<T> sorted(Closure<?> toComparable) {
    Closure<?> dehydrated = toComparable.dehydrate();
    return descendant(() -> {
      return Sort.of(dataset.build())
          .by(e -> (S) dehydrated.call(e))
          .setNumPartitions(1)
          .output();
    });
  }


  public WindowedStream<Long> count() {
    return descendant(() -> {
      Dataset<Pair<Byte, Long>> counted = ReduceByKey.of(dataset.build())
          .keyBy(e -> (byte) 0)
          .valueBy(e -> 1L)
          .combineBy(Sums.ofLongs())
          .windowBy(windowing)
          .output();
      return MapElements.of(counted)
          .using(Pair::getSecond)
          .output();
    });
  }

  public WindowedStream<T> distinct() {
    return descendant(() -> {
      return Distinct.of(dataset.build())
          .windowBy(windowing)
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<T> distinct(Closure<?> mapper) {
    Closure<?> dehydrated = mapper.dehydrate();
    return descendant(() -> {
      return (Dataset<T>) Distinct.of(dataset.build())
          .mapped(dehydrated::call)
          .windowBy(windowing)
          .output();
    });
  }


}
