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

import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import groovy.transform.stc.ClosureParams;
import groovy.transform.stc.FromString;
import javax.annotation.Nullable;

/** A stream that is windowed. */
public interface WindowedStream<T> extends Stream<T> {

  /**
   * Reduce stream via given reducer.
   *
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initialValue zero element
   * @param reducer the reduce function
   * @return reduced stream
   */
  default <K, V> WindowedStream<Pair<K, V>> reduce(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      V initialValue,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> reducer) {

    return reduce(null, keyExtractor, valueExtractor, initialValue, reducer);
  }

  /**
   * Reduce stream via given reducer.
   *
   * @param <K> key type
   * @param <V> value type
   * @param name name of the reduce operator
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initialValue zero element
   * @param reducer the reduce function
   * @return reduced stream
   */
  <K, V> WindowedStream<Pair<K, V>> reduce(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      V initialValue,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> reducer);

  /**
   * Reduce stream via given reducer.
   *
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param initialValue zero element
   * @param reducer the reduce function
   * @return reduced stream
   */
  default <K, V> WindowedStream<Pair<K, V>> reduce(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      V initialValue,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> reducer) {

    return reduce(null, keyExtractor, initialValue, reducer);
  }

  /**
   * Reduce stream via given reducer.
   *
   * @param <K> key type
   * @param <V> value type
   * @param name name of the reduce operator
   * @param keyExtractor extractor of key
   * @param initialValue zero element
   * @param reducer the reduce function
   * @return reduced stream
   */
  <K, V> WindowedStream<Pair<K, V>> reduce(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      V initialValue,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> reducer);

  /**
   * Reduce stream to latest values only.
   *
   * @return reduced stream
   */
  default WindowedStream<StreamElement> reduceToLatest() {
    return reduceToLatest(null);
  }

  /**
   * Reduce stream to latest values only.
   *
   * @param name name of the reduce operator
   * @return reduced stream
   */
  WindowedStream<StreamElement> reduceToLatest(@Nullable String name);

  /**
   * Reduce stream with reduce function taking list of values.
   *
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param listReduce the reduce function taking list of elements
   * @return reduced stream
   */
  default <K, V> WindowedStream<Pair<K, V>> groupReduce(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(
              value = FromString.class,
              options = {"Object, List<T>"})
          Closure<Iterable<V>> listReduce) {

    return groupReduce(null, keyExtractor, listReduce);
  }

  /**
   * Reduce stream with reduce function taking list of values.
   *
   * @param <K> key type
   * @param <V> value type
   * @param name name of the group reduce operator
   * @param keyExtractor extractor of key
   * @param listReduce the reduce function taking list of elements
   * @return reduced stream
   */
  <K, V> WindowedStream<Pair<K, V>> groupReduce(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(
              value = FromString.class,
              options = {"Object, List<T>"})
          Closure<Iterable<V>> listReduce);

  /**
   * Apply combine transform to stream.
   *
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initial zero element
   * @param combine combine function
   * @return the new stream
   */
  default <K, V> WindowedStream<Pair<K, V>> combine(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      V initial,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> combine) {

    return combine(null, keyExtractor, valueExtractor, initial, combine);
  }

  /**
   * Apply combine transform to stream.
   *
   * @param <K> key type
   * @param <V> value type
   * @param name name of the combine operator
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initial zero element
   * @param combine combine function
   * @return the new stream
   */
  <K, V> WindowedStream<Pair<K, V>> combine(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      V initial,
      @ClosureParams(value = FromString.class, options = "V, V") Closure<V> combine);

  /**
   * Apply combine transform to stream.
   *
   * @param <K> key type
   * @param keyExtractor extractor of key
   * @param initial zero element
   * @param combine combine function
   * @return the new stream
   */
  default <K> WindowedStream<Pair<K, T>> combine(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      T initial,
      @ClosureParams(value = FromString.class, options = "T, T") Closure<T> combine) {

    return combine(null, keyExtractor, initial, combine);
  }

  /**
   * Apply combine transform to stream.
   *
   * @param <K> key type
   * @param name name of the combine operator
   * @param keyExtractor extractor of key
   * @param initial zero element
   * @param combine combine function
   * @return the new stream
   */
  <K> WindowedStream<Pair<K, T>> combine(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      T initial,
      @ClosureParams(value = FromString.class, options = "T, T") Closure<T> combine);

  /**
   * Count elements of stream by key.
   *
   * @param <K> key type
   * @param keyExtractor extractor of key
   * @return stream with elements counted
   */
  default <K> WindowedStream<Pair<K, Long>> countByKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor) {

    return countByKey(null, keyExtractor);
  }

  /**
   * Count elements of stream by key.
   *
   * @param <K> key type
   * @param name name of the countByKey operator
   * @param keyExtractor extractor of key
   * @return stream with elements counted
   */
  <K> WindowedStream<Pair<K, Long>> countByKey(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor);

  /**
   * Average elements of stream.
   *
   * @param valueExtractor extractor of double value to be averaged
   * @return the stream with average values
   */
  default WindowedStream<Double> average(
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor) {

    return average(null, valueExtractor);
  }

  /**
   * Average elements of stream.
   *
   * @param name name of the average operator
   * @param valueExtractor extractor of double value to be averaged
   * @return the stream with average values
   */
  WindowedStream<Double> average(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor);

  /**
   * Average elements of stream by key.
   *
   * @param <K> key type
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of double value
   * @return stream with average values per key
   */
  default <K> WindowedStream<Pair<K, Double>> averageByKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor) {

    return averageByKey(null, keyExtractor, valueExtractor);
  }

  /**
   * Average elements of stream by key.
   *
   * @param <K> key type
   * @param name name of the averageByKey operator
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of double value
   * @return stream with average values per key
   */
  <K> WindowedStream<Pair<K, Double>> averageByKey(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor);

  /**
   * Join with other stream.
   *
   * @param <K> type of join key
   * @param <OTHER> type of other stream
   * @param right the right stream
   * @param leftKey extractor applied on left stream
   * @param rightKey extractor applied on right stream
   * @return joined stream
   */
  default <K, OTHER> WindowedStream<Pair<T, OTHER>> join(
      WindowedStream<OTHER> right,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> leftKey,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> rightKey) {

    return join(null, right, leftKey, rightKey);
  }

  /**
   * Join with other stream.
   *
   * @param <K> type of join key
   * @param <OTHER> type of other stream
   * @param name name of the join operator
   * @param right the right stream
   * @param leftKey extractor applied on left stream
   * @param rightKey extractor applied on right stream
   * @return joined stream
   */
  <K, OTHER> WindowedStream<Pair<T, OTHER>> join(
      @Nullable String name,
      WindowedStream<OTHER> right,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> leftKey,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> rightKey);

  /**
   * Left join with other stream.
   *
   * @param <K> type of join key
   * @param <OTHER> type of other stream
   * @param right the right stream
   * @param leftKey extractor applied on left stream
   * @param rightKey extractor applied on right stream
   * @return joined stream
   */
  default <K, OTHER> WindowedStream<Pair<T, OTHER>> leftJoin(
      WindowedStream<OTHER> right,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> leftKey,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> rightKey) {

    return leftJoin(null, right, leftKey, rightKey);
  }

  /**
   * Left join with other stream.
   *
   * @param <K> type of join key
   * @param <OTHER> type of other stream
   * @param name name of the join operator
   * @param right the right stream
   * @param leftKey extractor applied on left stream
   * @param rightKey extractor applied on right stream
   * @return joined stream
   */
  <K, OTHER> WindowedStream<Pair<T, OTHER>> leftJoin(
      @Nullable String name,
      WindowedStream<OTHER> right,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> leftKey,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> rightKey);

  /**
   * Sort stream.
   *
   * @param compareFn comparison function
   * @return sorted stram
   */
  default WindowedStream<T> sorted(
      @ClosureParams(value = FromString.class, options = "T") Closure<Integer> compareFn) {

    return sorted(null, compareFn);
  }

  /**
   * Sort stream.
   *
   * @param name name of the sort operator
   * @param compareFn comparison function
   * @return sorted stram
   */
  WindowedStream<T> sorted(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<Integer> compareFn);

  /**
   * Sort stream consisting of {@link Comparable}s.
   *
   * @return sorted stream
   */
  default WindowedStream<Comparable<T>> sorted() {
    return sorted((String) null);
  }

  /**
   * Sort stream consisting of {@link Comparable}s.
   *
   * @param name name of the sort operator
   * @return sorted stream
   */
  WindowedStream<Comparable<T>> sorted(@Nullable String name);

  /**
   * Count elements.
   *
   * @return stream with element counts
   */
  default WindowedStream<Long> count() {
    return count(null);
  }

  /**
   * Count elements.
   *
   * @param name name of the count operator
   * @return stream with element counts
   */
  WindowedStream<Long> count(@Nullable String name);

  /**
   * Sum elements.
   *
   * @param valueExtractor extractor of double value
   * @return stream with sums
   */
  default WindowedStream<Double> sum(
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor) {

    return sum(null, valueExtractor);
  }

  /**
   * Sum elements.
   *
   * @param name name of the sum operator
   * @param valueExtractor extractor of double value
   * @return stream with sums
   */
  WindowedStream<Double> sum(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor);

  /**
   * Sum elements by key.
   *
   * @param <K> type of key
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of double value
   * @return stream with sums per key
   */
  default <K> WindowedStream<Pair<K, Double>> sumByKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor) {

    return sumByKey(null, keyExtractor, valueExtractor);
  }

  /**
   * Sum elements by key.
   *
   * @param <K> type of key
   * @param name name of the sumByKey operator
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of double value
   * @return stream with sums per key
   */
  <K> WindowedStream<Pair<K, Double>> sumByKey(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<Double> valueExtractor);

  /**
   * Output distinct elements.
   *
   * @return stream with distinct elements
   */
  default WindowedStream<T> distinct() {
    return distinct((String) null);
  }

  /**
   * Output distinct elements.
   *
   * @param name name of the distinct operator
   * @return stream with distinct elements
   */
  WindowedStream<T> distinct(@Nullable String name);

  /**
   * Output distinct elements through given mapper.
   *
   * @param mapper map values by given function before comparison
   * @return distinct stream
   */
  default WindowedStream<T> distinct(
      @ClosureParams(value = FromString.class, options = "T") Closure<?> mapper) {

    return distinct(null, mapper);
  }

  /**
   * Output distinct elements through given mapper.
   *
   * @param name name of the distinct operator
   * @param mapper map values by given function before comparison
   * @return distinct stream
   */
  WindowedStream<T> distinct(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<?> mapper);

  /**
   * Specify early emitting for windowed operations
   *
   * @param duration the duration (in processing time) of the early emitting
   * @return stream with early emitting specified
   */
  WindowedStream<T> withEarlyEmitting(long duration);

  /**
   * Specify allowed lateness for windowed operations.
   *
   * @param lateness the allowed lateness
   * @return stream with allowed lateness specified
   */
  WindowedStream<T> withAllowedLateness(long lateness);

  // overrides of Stream methods with fixed return types

  @Override
  default <X> WindowedStream<X> flatMap(Closure<Iterable<X>> mapper) {
    return flatMap(null, mapper);
  }

  @Override
  <X> WindowedStream<X> flatMap(@Nullable String name, Closure<Iterable<X>> mapper);

  @Override
  default <X> WindowedStream<X> map(Closure<X> mapper) {
    return map(null, mapper);
  }

  @Override
  <X> WindowedStream<X> map(@Nullable String name, Closure<X> mapper);

  @Override
  default WindowedStream<T> filter(Closure<Boolean> predicate) {
    return filter(null, predicate);
  }

  @Override
  WindowedStream<T> filter(@Nullable String name, Closure<Boolean> predicate);

  @Override
  default WindowedStream<T> assignEventTime(Closure<Long> assigner) {
    return assignEventTime(null, assigner);
  }

  @Override
  WindowedStream<T> assignEventTime(@Nullable String name, Closure<Long> assigner);

  @Override
  default WindowedStream<Pair<Object, T>> withWindow() {
    return withWindow(null);
  }

  @Override
  WindowedStream<Pair<Object, T>> withWindow(@Nullable String name);

  @Override
  default WindowedStream<Pair<T, Long>> withTimestamp() {
    return withTimestamp(null);
  }

  @Override
  WindowedStream<Pair<T, Long>> withTimestamp(@Nullable String name);

  @Override
  default WindowedStream<T> asUnbounded() {
    return this;
  }

  @Override
  <V> WindowedStream<StreamElement> asStreamElements(
      RepositoryProvider repoProvider,
      EntityDescriptor entity,
      Closure<CharSequence> keyExtractor,
      Closure<CharSequence> attributeExtractor,
      Closure<V> valueExtractor,
      Closure<Long> timeExtractor);
}
