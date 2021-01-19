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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import groovy.transform.CompileStatic;
import groovy.transform.stc.ClosureParams;
import groovy.transform.stc.FromString;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/** A stream abstraction with fluent style methods. */
@CompileStatic
public interface Stream<T> {

  /**
   * Remap the stream.
   *
   * @param <X> type parameter
   * @param mapper mapper returning iterable of values to be flattened into output
   * @return the remapped stream
   */
  default <X> Stream<X> flatMap(
      @ClosureParams(value = FromString.class, options = "T") Closure<Iterable<X>> mapper) {

    return flatMap(null, mapper);
  }

  /**
   * Remap the stream.
   *
   * @param <X> type parameter
   * @param name name of the operation
   * @param mapper mapper returning iterable of values to be flattened into output
   * @return the remapped stream
   */
  <X> Stream<X> flatMap(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<Iterable<X>> mapper);

  /**
   * Remap the stream.
   *
   * @param <X> type parameter
   * @param mapper the mapping closure
   * @return remapped stream
   */
  default <X> Stream<X> map(
      @ClosureParams(value = FromString.class, options = "T") Closure<X> mapper) {

    return map(null, mapper);
  }

  /**
   * Remap the stream.
   *
   * @param <X> type parameter
   * @param name stable name of the mapping operator
   * @param mapper the mapping closure
   * @return remapped stream
   */
  <X> Stream<X> map(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<X> mapper);

  /**
   * Filter stream based on predicate
   *
   * @param predicate the predicate to filter on
   * @return filtered stream
   */
  default Stream<T> filter(
      @ClosureParams(value = FromString.class, options = "T") Closure<Boolean> predicate) {

    return filter(null, predicate);
  }

  /**
   * Filter stream based on predicate
   *
   * @param name name of the filter operator
   * @param predicate the predicate to filter on
   * @return filtered stream
   */
  Stream<T> filter(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<Boolean> predicate);

  /**
   * Assign event time to elements.
   *
   * @param assigner assigner of event time
   * @return stream with elements assigned event time
   */
  default Stream<T> assignEventTime(
      @ClosureParams(value = FromString.class, options = "T") Closure<Long> assigner) {

    return assignEventTime(null, assigner);
  }

  /**
   * Assign event time to elements.
   *
   * @param name name of the assign event time operator
   * @param assigner assigner of event time
   * @return stream with elements assigned event time
   */
  Stream<T> assignEventTime(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<Long> assigner);

  /**
   * Add window to each element in the stream.
   *
   * @return stream of pairs with window
   */
  default Stream<Pair<Object, T>> withWindow() {
    return withWindow(null);
  }

  /**
   * Add window to each element in the stream.
   *
   * @param name stable name of the mapping operator
   * @return stream of pairs with window
   */
  Stream<Pair<Object, T>> withWindow(@Nullable String name);

  /**
   * Add timestamp to each element in the stream.
   *
   * @return stream of pairs with timestamp
   */
  default Stream<Pair<T, Long>> withTimestamp() {
    return withTimestamp(null);
  }

  /**
   * Add timestamp to each element in the stream.
   *
   * @param name stable name of mapping operator
   * @return stream of pairs with timestamp
   */
  Stream<Pair<T, Long>> withTimestamp(@Nullable String name);

  /** Print all elements to console. */
  void print();

  /**
   * Collect stream as list. Note that this will result on OOME if this is unbounded stream.
   *
   * @return the stream collected as list.
   */
  List<T> collect();

  /**
   * Test if this is bounded stream.
   *
   * @return {@code true} if this is bounded stream, {@code false} otherwise
   */
  boolean isBounded();

  /**
   * Process this stream as it was unbounded stream.
   *
   * <p>This is a no-op if {@link #isBounded} returns {@code false}, otherwise it turns the stream
   * into being processed as unbounded, although being bounded.
   *
   * <p>This is an optional operation and might be ignored if not supported by underlying
   * implementation.
   *
   * @return Stream viewed as unbounded stream, if supported
   */
  default Stream<T> asUnbounded() {
    return this;
  }

  /**
   * Convert elements to {@link StreamElement}s.
   *
   * @param <V> type of value
   * @param repoProvider provider of {@link Repository}
   * @param entity the entity of elements
   * @param keyExtractor extractor of keys
   * @param attributeExtractor extractor of attributes
   * @param valueExtractor extractor of values
   * @param timeExtractor extractor of time
   * @return stream with {@link StreamElement}s inside
   */
  <V> Stream<StreamElement> asStreamElements(
      RepositoryProvider repoProvider,
      EntityDescriptor entity,
      @ClosureParams(value = FromString.class, options = "T") Closure<CharSequence> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<CharSequence> attributeExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<Long> timeExtractor);

  /**
   * Persist this stream to replication.
   *
   * @param repoProvider provider of {@link Repository}.
   * @param replicationName name of replication to persist stream to
   * @param target target of the replication
   */
  void persistIntoTargetReplica(
      RepositoryProvider repoProvider, String replicationName, String target);

  /**
   * Persist this stream to specific family.
   *
   * <p>Note that the type of the stream has to be already {@link StreamElement StreamElements} to
   * be persisted to the specified family. The family has to accept given {@link
   * AttributeDescriptor} of the {@link StreamElement}.
   *
   * @param repoProvider provider of {@link Repository}.
   * @param targetFamilyname name of target family to persist the stream into
   */
  default void persistIntoTargetFamily(RepositoryProvider repoProvider, String targetFamilyname) {
    persistIntoTargetFamily(repoProvider, targetFamilyname, 10);
  }

  /**
   * Persist this stream to specific family.
   *
   * <p>Note that the type of the stream has to be already {@link StreamElement StreamElements} to
   * be persisted to the specified family. The family has to accept given {@link
   * AttributeDescriptor} of the {@link StreamElement}.
   *
   * @param repoProvider provider of {@link Repository}.
   * @param targetFamilyname name of target family to persist the stream into
   * @param parallelism parallelism to use when target family is bulk attribute family
   */
  void persistIntoTargetFamily(
      RepositoryProvider repoProvider, String targetFamilyname, int parallelism);

  /**
   * Persist this stream as attribute of entity
   *
   * @param <V> type of value extracted
   * @param repoProvider provider of repository
   * @param entity the entity to store the stream to
   * @param keyExtractor extractor of key for elements
   * @param attributeExtractor extractor for attribute for elements
   * @param valueExtractor extractor of values for elements
   * @param timeExtractor extractor of event time
   */
  <V> void persist(
      RepositoryProvider repoProvider,
      EntityDescriptor entity,
      @ClosureParams(value = FromString.class, options = "T") Closure<CharSequence> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T")
          Closure<CharSequence> attributeExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<Long> timeExtractor);

  /**
   * Directly write this stream to repository. Note that the stream has to contain {@link
   * StreamElement}s (e.g. created by {@link #asStreamElements}.
   *
   * @param repoProvider provider of repository
   */
  void write(RepositoryProvider repoProvider);

  /**
   * Create time windowed stream.
   *
   * @param millis duration of tumbling window
   * @return time windowed stream
   */
  WindowedStream<T> timeWindow(long millis);

  /**
   * Create sliding time windowed stream.
   *
   * @param millis duration of the window
   * @param slide duration of the slide
   * @return sliding time windowed stream
   */
  WindowedStream<T> timeSlidingWindow(long millis, long slide);

  /**
   * Create session windowed stream.
   *
   * @param <K> type of key
   * @param keyExtractor extractor of key
   * @param gapDuration duration of the gap between elements per key
   * @return session windowed stream
   */
  <K> WindowedStream<Pair<K, T>> sessionWindow(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      long gapDuration);

  /**
   * Group all elements into single window.
   *
   * @return globally windowed stream.
   */
  WindowedStream<T> windowAll();

  /**
   * Merge two streams together.
   *
   * @param other the other stream(s)
   * @return merged stream
   */
  default Stream<T> union(Stream<T> other) {
    return union(Arrays.asList(other));
  }

  /**
   * Merge two streams together.
   *
   * @param name name of the union operator
   * @param other the other stream(s)
   * @return merged stream
   */
  default Stream<T> union(@Nullable String name, Stream<T> other) {
    return union(name, Arrays.asList(other));
  }

  /**
   * Merge multiple streams together.
   *
   * @param streams other streams
   * @return merged stream
   */
  default Stream<T> union(List<Stream<T>> streams) {
    return union(null, streams);
  }

  /**
   * Merge multiple streams together.
   *
   * @param name name of the union operator
   * @param streams other streams
   * @return merged stream
   */
  Stream<T> union(@Nullable String name, List<Stream<T>> streams);

  /**
   * Transform this stream using stateful processing.
   *
   * @param <K> type of key
   * @param <S> type of value state
   * @param <V> type of intermediate value
   * @param <O> type of output value
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initialState closure providing initial state value for key
   * @param outputFn function for outputting values (when function returns {@code null} the output
   *     is discarded
   * @param stateUpdate update (accumulation) function for the state the output is discarded
   * @return the statefully reduced stream
   */
  default <K, S, V, O> Stream<Pair<K, O>> reduceValueStateByKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      @ClosureParams(value = FromString.class, options = "K") Closure<S> initialState,
      @ClosureParams(value = FromString.class, options = "S, V") Closure<O> outputFn,
      @ClosureParams(value = FromString.class, options = "S, V") Closure<S> stateUpdate) {

    return reduceValueStateByKey(
        null, keyExtractor, valueExtractor, initialState, outputFn, stateUpdate);
  }

  /**
   * Transform this stream using stateful processing.
   *
   * @param <K> type of key
   * @param <S> type of value state
   * @param <V> type of intermediate value
   * @param <O> type of output value
   * @param name optional name of the stateful operation
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initialState closure providing initial state value for key
   * @param stateUpdate update (accumulation) function for the state
   * @param outputFn function for outputting values (when function returns {@code null} the output
   *     is discarded
   * @return the statefully reduced stream
   */
  <K, S, V, O> Stream<Pair<K, O>> reduceValueStateByKey(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      @ClosureParams(value = FromString.class, options = "K") Closure<S> initialState,
      @ClosureParams(value = FromString.class, options = "S, V") Closure<O> outputFn,
      @ClosureParams(value = FromString.class, options = "S, V") Closure<S> stateUpdate);

  /**
   * Transform this stream to another stream by applying combining transform in global window
   * emitting results after each element added. That means that the following holds: * the new
   * stream will have exactly the same number of elements as the original stream minus late elements
   * dropped * streaming semantics need to define allowed lateness, which will incur real time
   * processing delay * batch semantics use sort per key
   *
   * @param <K> key type
   * @param <V> value type
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initialValue closure providing initial value of state for key
   * @param combiner combiner of values to final value
   * @return the integrated stream
   */
  default <K, V> Stream<Pair<K, V>> integratePerKey(
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      @ClosureParams(value = FromString.class, options = "K") Closure<V> initialValue,
      @ClosureParams(value = FromString.class, options = "V,V") Closure<V> combiner) {

    return integratePerKey(null, keyExtractor, valueExtractor, initialValue, combiner);
  }

  /**
   * Transform this stream to another stream by applying combining transform in global window
   * emitting results after each element added. That means that the following holds: * the new
   * stream will have exactly the same number of elements as the original stream minus late elements
   * dropped * streaming semantics need to define allowed lateness, which will incur real time
   * processing delay * batch semantics use sort per key
   *
   * @param <K> key type
   * @param <V> value type
   * @param name optional name of the transform
   * @param keyExtractor extractor of key
   * @param valueExtractor extractor of value
   * @param initialValue closure providing initial value of state for key
   * @param combiner combiner of values to final value
   * @return the integrated stream
   */
  <K, V> Stream<Pair<K, V>> integratePerKey(
      @Nullable String name,
      @ClosureParams(value = FromString.class, options = "T") Closure<K> keyExtractor,
      @ClosureParams(value = FromString.class, options = "T") Closure<V> valueExtractor,
      @ClosureParams(value = FromString.class, options = "K") Closure<V> initialValue,
      @ClosureParams(value = FromString.class, options = "V,V") Closure<V> combiner);
}
