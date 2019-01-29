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

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;

/**
 * A stream that is windowed.
 */
public interface WindowedStream<T> extends Stream<T> {

  <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initialValue,
      Closure<V> reducer);

  <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor,
      V initialValue,
      Closure<V> reducer);

  WindowedStream<StreamElement> reduceToLatest();

  <K, V> WindowedStream<Pair<K, V>> flatReduce(
      Closure<K> keyExtractor,
      Closure<V> listReduce);

  <K, V> WindowedStream<Pair<K, V>> combine(
      Closure<K> keyExtractor,
      Closure<V> valueExtractor,
      V initial,
      Closure<V> combine);

  <K> WindowedStream<Pair<K, T>> combine(
      Closure<K> keyExtractor,
      T initial,
      Closure<T> combine);

  <K> WindowedStream<Pair<K, Long>> countByKey(Closure<K> keyExtractor);

  WindowedStream<Double> average(Closure<Double> valueExtractor);

  <K> WindowedStream<Pair<K, Double>> averageByKey(
      Closure<K> keyExtractor,
      Closure<Double> valueExtractor);

  <LEFT, RIGHT> WindowedStream<Pair<T, RIGHT>> join(
      WindowedStream<RIGHT> right,
      Closure<LEFT> leftKey,
      Closure<RIGHT> rightKey);

  <LEFT, RIGHT> WindowedStream<Pair<T, RIGHT>> leftJoin(
      WindowedStream<RIGHT> right,
      Closure<LEFT> leftKey,
      Closure<RIGHT> rightKey);

  WindowedStream<T> sorted(Closure<Integer> compareFn);

  WindowedStream<Comparable<T>> sorted();

  WindowedStream<Long> count();

  WindowedStream<Double> sum(Closure<Double> valueExtractor);

  <K> WindowedStream<Pair<K, Double>> sumByKey(
      Closure<K> keyExtractor,
      Closure<Double> valueExtractor);

  WindowedStream<T> distinct();

  WindowedStream<T> distinct(Closure<?> mapper);

  WindowedStream<T> withEarlyEmitting(long duration);

}
