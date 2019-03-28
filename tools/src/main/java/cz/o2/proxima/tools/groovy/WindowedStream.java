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
import groovy.transform.stc.ClosureParams;
import groovy.transform.stc.FromString;

/**
 * A stream that is windowed.
 */
public interface WindowedStream<T> extends Stream<T> {

  /**
   * Reduce stream via given reducer.
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initialValue zero element
   * @param reducer the reduce function
   * @return reduced stream
   */
  <K, V> WindowedStream<Pair<K, V>> reduce(
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<V> valueExtractor,
      V initialValue,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> reducer);

  /**
   * Reduce stream via given reducer.
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param initialValue zero element
   * @param reducer the reduce function
   * @return reduced stream
   */
  <K, V> WindowedStream<Pair<K, V>> reduce(
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> keyExtractor,
      V initialValue,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> reducer);

  /**
   * Reduce stream to latest values only.
   * @return reduced stream
   */
  WindowedStream<StreamElement> reduceToLatest();

  /**
   * Reduce stream with reduce function taking list of values.
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param listReduce the reduce function taking list of elements
   * @return reduced stream
   */
  <K, V> WindowedStream<Pair<K, V>> groupReduce(
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = {"Object, List<T>"})
          Closure<V> listReduce);

  /**
   * Apply combine transform to stream.
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initial zero element
   * @param combine combine function
   * @return the new stream
   */
  <K, V> WindowedStream<Pair<K, V>> combine(
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<V> valueExtractor,
      V initial,
      @ClosureParams(value = FromString.class, options = "V, V")
          Closure<V> combine);

  /**
   * Apply combine transform to stream.
   * @param <K> key type
   * @param keyExtractor extractor of key
   * @param initial zero element
   * @param combine combine function
   * @return the new stream
   */
  <K> WindowedStream<Pair<K, T>> combine(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      T initial,
      @ClosureParams(value = FromString.class, options = "T, T") Closure<T> combine);

  /**
   * Count elements of stream by key.
   * @param <K> key type
   * @param keyExtractor extractor of key
   * @return stream with elements counted
   */
  <K> WindowedStream<Pair<K, Long>> countByKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor);

  /**
   * Average elements of stream.
   * @param valueExtractor extractor of double value to be averaged
   * @return the stream with average values
   */
  WindowedStream<Double> average(
      @ClosureParams(value = FromString.class, options = "T")
          Closure<Double> valueExtractor);

  /**
   * Average elements of stream by key.
   * @param <K> key type
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of double value
   * @return stream with avverage values per key
   */
  <K> WindowedStream<Pair<K, Double>> averageByKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<Double> valueExtractor);

  /**
   * Join with other stream.
   * @param <K> type of join key
   * @param <OTHER> type of other stream
   * @param right the right stream
   * @param leftKey extractor applied on left stream
   * @param rightKey extractor applied on right stream
   * @return joined stream
   */
  <K, OTHER> WindowedStream<Pair<T, OTHER>> join(
      WindowedStream<OTHER> right,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> leftKey,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> rightKey);


  /**
   * Left join with other stream.
   * @param <K> type of join key
   * @param <OTHER> type of other stream
   * @param right the right stream
   * @param leftKey extractor applied on left stream
   * @param rightKey extractor applied on right stream
   * @return joined stream
   */
  <K, OTHER> WindowedStream<Pair<T, OTHER>> leftJoin(
      WindowedStream<OTHER> right,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> leftKey,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<K> rightKey);

  /**
   * Sort stream.
   * @param compareFn comparison function
   * @return sorted stram
   */
  WindowedStream<T> sorted(
      @ClosureParams(value = FromString.class, options = "T")
          Closure<Integer> compareFn);

  /**
   * Sort stream consisting of {@link Comparable}s.
   * @return sorted stream
   */
  WindowedStream<Comparable<T>> sorted();

  /**
   * Count elements.
   * @return stream with element counts
   */
  WindowedStream<Long> count();

  /**
   * Sum elements.
   * @param valueExtractor extractor of double value
   * @return stream with sums
   */
  WindowedStream<Double> sum(
      @ClosureParams(value = FromString.class, options = "T")
          Closure<Double> valueExtractor);

  /**
   * Sum elements by key.
   * @param <K> type of key
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of double value
   * @return stream with sums per key
   */
  <K> WindowedStream<Pair<K, Double>> sumByKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<Double> valueExtractor);

  /**
   * Output distinct elements.
   * @return stream with distinct elements
   */
  WindowedStream<T> distinct();

  /**
   * Output distinct elements through given mapper.
   * @param mapper map values by given function before comparison
   * @return distinct stream
   */
  WindowedStream<T> distinct(
      @ClosureParams(value = FromString.class, options = "T") Closure<?> mapper);

  /**
   * Specify early emitting for windowed operations
   * @param duration the duration (in processing time) of the early emitting
   * @return stream with early emitting specified
   */
  WindowedStream<T> withEarlyEmitting(long duration);

  /**
   * Specify allowed lateness for windowed operations.
   * @param lateness the allowed lateness
   * @return stream with allowed lateness specified
   */
  WindowedStream<T> withAllowedLateness(long lateness);

}
