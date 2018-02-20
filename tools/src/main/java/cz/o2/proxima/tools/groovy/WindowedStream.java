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

import cz.o2.proxima.tools.io.TypedStreamElement;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.LeftJoin;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import groovy.lang.Closure;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * A stream that is windowed.
 */
public class WindowedStream<T, W extends Windowing> extends Stream<T> {

  @SuppressWarnings("unchecked")
  private static class JoinedWindowing<L, R, W extends Window<W>> implements Windowing<Either<L, R>, W> {

    private final  Windowing left;
    private final Windowing right;
    private long earlyEmitting = -1L;
    private long earlyEmitStamp = -1L;

    private JoinedWindowing(WindowedStream left, WindowedStream right) {
      this.left = left.windowing;
      this.right = right.windowing;
    }

    @Override
    public Iterable<W> assignWindowsToElement(WindowedElement<?, Either<L, R>> el) {
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
    public Trigger<W> getTrigger() {
      return new Trigger<W>() {
        @Override
        public boolean isStateful() {
          return left.getTrigger().isStateful() || right.getTrigger().isStateful();
        }

        @Override
        public Trigger.TriggerResult onElement(
            long time, Window window, TriggerContext ctx) {

          if (earlyEmitting > 0 && earlyEmitStamp < 0) {
            ctx.registerTimer(earlyEmitStamp = time + earlyEmitting, window);
          }
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
        public Trigger.TriggerResult onTimer(long time, W window, TriggerContext ctx) {
          Trigger.TriggerResult res = toResult(
              left.getTrigger().onTimer(time, window, ctx),
              right.getTrigger().onTimer(time, window, ctx));
          if (time == earlyEmitStamp) {
            ctx.registerTimer(time + earlyEmitStamp, window);
            return toResult(Trigger.TriggerResult.FLUSH, res);
          }
          return res;

        }

        @Override
        public void onClear(W window, TriggerContext ctx) {
          left.getTrigger().onClear(window, ctx);
          right.getTrigger().onClear(window, ctx);
        }

        @Override
        public void onMerge(W window, TriggerContext.TriggerMergeContext ctx) {
          left.getTrigger().onMerge(window, ctx);
          right.getTrigger().onMerge(window, ctx);
        }

      };
    }

    private void setEarlyEmitting(Duration earlyEmitting) {
      this.earlyEmitting = earlyEmitting.toMillis();
    }
  }

  final W windowing;
  final BinaryFunction<W, Duration, W> earlyEmittingConsumer;
  @Nullable
  Duration earlyEmitting;

  public WindowedStream(
      Executor executor,
      DatasetBuilder<T> dataset,
      W windowing,
      Runnable terminatingOperationCall,
      BinaryFunction<W, Duration, W> earlyEmitting) {

    super(executor, dataset, terminatingOperationCall);
    this.windowing = Objects.requireNonNull(windowing);
    this.earlyEmittingConsumer = Objects.requireNonNull(earlyEmitting);
  }



  @Override
  @SuppressWarnings("unchecked")
  <X> WindowedStream<X, W> descendant(DatasetBuilder<X> dataset) {
    return new WindowedStream(
        executor,
        dataset,
        windowing,
        terminatingOperationCall,
        earlyEmittingConsumer);
  }

  <X, W1 extends Windowing> WindowedStream<X, W1> descendant(
      DatasetBuilder<X> dataset,
      W1 windowing,
      BinaryFunction<W1, Duration, W1> earlyEmitting) {

    return new WindowedStream<>(
        executor,
        dataset,
        windowing,
        terminatingOperationCall,
        earlyEmitting);
  }

  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>, W> reduce(
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
          .reduceBy((java.util.stream.Stream<V> in) -> {
            V current = initialValue;
            return in.reduce(current, (a, b) -> reducerDehydrated.call(a, b));
          })
          .windowBy(withEmitting())
          .output();
    });
  }


  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>, W> reduce(
      Closure<K> keyExtractor,
      V initialValue,
      Closure<V> reducer) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> reducerDehydrated = reducer.dehydrate();
    return (WindowedStream) descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .reduceBy((java.util.stream.Stream<T> in) -> {
            V current = initialValue;
            Iterable<T> iter = () -> in.iterator();
            for (T v : iter) {
              current = reducerDehydrated.call(current, v);
            }
            return current;
          })
          .windowBy(withEmitting())
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <T> WindowedStream<TypedStreamElement<T>, W> reduceToLatest() {
    return descendant(() -> {
      Dataset<TypedStreamElement<T>> input = (Dataset<TypedStreamElement<T>>) dataset.build();
      return ReduceByKey.of(input)
          .keyBy(i -> Pair.of(i.getKey(), i.getAttribute()))
          .combineBy(values ->
              StreamSupport.stream(values.spliterator(), false)
                  .collect(Collectors.maxBy((a, b) -> {
                    return Long.compare(a.getStamp(), b.getStamp());
                  }))
                  .get())
          .outputValues();
    });
  }

  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>, W> flatReduce(
      Closure<K> keyExtractor,
      Closure<V> listReduce) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<V> reducerDehydrated = listReduce.dehydrate();

    return descendant(() -> {
        return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .reduceBy((java.util.stream.Stream<T> in, Collector<V> ctx) -> {
            List<V> ret = (List<V>) reducerDehydrated.call(
                ctx.getWindow(), in.collect(Collectors.toList()));
            ret.forEach(elem -> ctx.collect(elem));
          })
          .windowBy(withEmitting())
          .output();
        });
  }

  @SuppressWarnings("unchecked")
  public <K, V> WindowedStream<Pair<K, V>, W> combine(
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
          .combineBy((java.util.stream.Stream<V> in) -> {
            return in.reduce(initial, (a, b) -> combineDehydrated.call(a, b));
          })
          .windowBy(withEmitting())
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <K> WindowedStream<Pair<K, T>, W> combine(
      Closure<K> keyExtractor,
      T initial,
      Closure<T> combine) {

    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    Closure<T> combineDehydrated = combine.dehydrate();
    return descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .combineBy((java.util.stream.Stream<T> in) -> {
            return in.reduce(initial, (a, b) -> combineDehydrated.call(a, b));
          })
          .windowBy(withEmitting())
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <K> WindowedStream<Pair<K, Long>, W> countByKey(Closure<K> keyExtractor) {
    Closure<K> keyDehydrated = keyExtractor.dehydrate();
    return descendant(() -> {
      return ReduceByKey.of(dataset.build())
          .keyBy(keyDehydrated::call)
          .valueBy(e -> 1L)
          .combineBy(Sums.ofLongs())
          .windowBy(withEmitting())
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public <KEY, RIGHT> WindowedStream<Pair<T, RIGHT>, JoinedWindowing> join(
      WindowedStream<RIGHT, ?> right,
      Closure<?> leftKey,
      Closure<?> rightKey) {

    Closure<?> leftKeyDehydrated = leftKey.dehydrate();
    Closure<?> rightKeyDehydrated = rightKey.dehydrate();
    JoinedWindowing windowing = new JoinedWindowing<>(this, right);
    return descendant(() -> {
      Dataset<Pair<Object, Pair<T, RIGHT>>> joined;
      joined = Join.of(dataset.build(), right.dataset.build())
          .by(leftKeyDehydrated::call, rightKeyDehydrated::call)
          .using((T l, RIGHT r, Collector<Pair<T, RIGHT>> ctx) -> {
            ctx.collect(Pair.of(l, r));
          })
          .windowBy(withEmitting())
          .output();
      return MapElements.of(joined)
          .using(Pair::getSecond)
          .output();
    },
    windowing,
    (w, d) -> {
      this.setEarlyEmitting(d);
      right.setEarlyEmitting(d);
      w.setEarlyEmitting(d);
      return w;
    });
  }

  @SuppressWarnings("unchecked")
  public <KEY, RIGHT> WindowedStream<Pair<T, RIGHT>, JoinedWindowing> leftJoin(
      WindowedStream<RIGHT, ?> right,
      Closure<?> leftKey,
      Closure<?> rightKey) {

    Closure<?> leftKeyDehydrated = leftKey.dehydrate();
    Closure<?> rightKeyDehydrated = rightKey.dehydrate();
    JoinedWindowing windowing = new JoinedWindowing<>(this, right);
    return descendant(() -> {
      Dataset<Pair<Object, Pair<T, RIGHT>>> joined = LeftJoin.of(dataset.build(), right.dataset.build())
          .by(leftKeyDehydrated::call, rightKeyDehydrated::call)
          .using((T l, Optional<RIGHT> r, Collector<Pair<T, RIGHT>> ctx) -> {
            ctx.collect(Pair.of(l, r.orElse(null)));
          })
          .windowBy(withEmitting())
          .output();
      return MapElements.of(joined)
          .using(Pair::getSecond)
          .output();
    },
    windowing,
    (w, d) -> {
      this.setEarlyEmitting(d);
      right.setEarlyEmitting(d);
      w.setEarlyEmitting(d);
      return w;
    });
  }


  @SuppressWarnings("unchecked")
  public <S extends Comparable<S>> WindowedStream<S, W> sorted(Closure<Integer> toComparable) {
    Closure<Integer> dehydrated = toComparable.dehydrate();
    return descendant(() -> {
      return ReduceWindow.of((Dataset<S>) dataset.build())
          .reduceBy((java.util.stream.Stream<S> in, cz.seznam.euphoria.core.client.io.Collector<S> ctx) -> {
            in.forEach(ctx::collect);
          })
          .withSortedValues((a, b) -> dehydrated.call(a, b))
          .output();
    });
  }


  @SuppressWarnings("unchecked")
  public WindowedStream<Long, W> count() {
    return descendant(() -> {
      Dataset<Pair<Byte, Long>> counted = ReduceByKey.of(dataset.build())
          .keyBy(e -> (byte) 0)
          .valueBy(e -> 1L)
          .combineBy(Sums.ofLongs())
          .windowBy(withEmitting())
          .output();
      return MapElements.of(counted)
          .using(Pair::getSecond)
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<T, W> distinct() {
    return descendant(() -> {
      return Distinct.of(dataset.build())
          .windowBy(withEmitting())
          .output();
    });
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<T, W> distinct(Closure<?> mapper) {
    Closure<?> dehydrated = mapper.dehydrate();
    return descendant(() -> {
      return (Dataset<T>) Distinct.of(dataset.build())
          .mapped(dehydrated::call)
          .windowBy(withEmitting())
          .output();
    });
  }

  public WindowedStream<T, W> withEarlyEmitting(long duration) {
    this.earlyEmitting = Duration.ofMillis(duration);
    return this;
  }

  W withEmitting() {
    if (earlyEmitting != null) {
      return earlyEmittingConsumer.apply(windowing, earlyEmitting);
    }
    return windowing;
  }

  private void setEarlyEmitting(Duration earlyEmitting) {
    this.earlyEmitting = earlyEmitting;
  }

}
