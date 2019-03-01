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

import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;

import java.util.List;

/**
 * A stream abstraction with fluent style methods.
 */
public interface Stream<T> {

  /**
   * Remap the stream.
   * @param <X> type parameter
   * @param mapper the mapping closure
   * @return remapped stream
   */
  <X> Stream<X> map(Closure<X> mapper);

  /**
   * Filter stream based on predicate
   * @param predicate the predicate to filter on
   * @return filtered stream
   */
  Stream<T> filter(Closure<Boolean> predicate);

  /**
   * Assign event time to elements.
   * @param assigner assigner of event time
   * @return stream with elements assigned event time
   */
  Stream<T> assignEventTime(Closure<Long> assigner);

  /**
   * Add window to each element in the stream.
   * @return stream of pairs with window
   */
  Stream<Pair<Object, T>> withWindow();

  /**
   * Print all elements to console.
   */
  void print();

  /**
   * Collect stream as list.
   * Note that this will result on OOME if this is unbounded stream.
   * @return the stream collected as list.
   */
  List<T> collect();

  /**
   * Test if this is bounded stream.
   * @return {@code true} if this is bounded stream, {@code false} otherwise
   */
  boolean isBounded();

  /**
   * Convert elements to {@link StreamElement}s.
   * @param repoProvider provider of {@link Repository}
   * @param entity the entity of elements
   * @param keyExtractor extractor of keys
   * @param attributeExtractor extractor of attributes
   * @param valueExtractor extractor of values
   * @param timeExtractor extractor of time
   * @return stream with {@link StreamElement}s inside
   */
  Stream<StreamElement> asStreamElements(
      RepositoryProvider repoProvider, EntityDescriptor entity,
      Closure<String> keyExtractor, Closure<String> attributeExtractor,
      Closure<T> valueExtractor, Closure<Long> timeExtractor);


  /**
   * Persist this stream to replication.
   * @param repoProvider provider of {@link Repository}.
   * @param replicationName name of replication to persist stream to
   * @param target target of the replication
   */
  void persistIntoTargetReplica(
      RepositoryProvider repoProvider,
      String replicationName,
      String target);

  /**
   * Persist this stream as attribute of entity
   * @param repoProvider provider of repository
   * @param entity the entity to store the stream to
   * @param keyExtractor extractor of key for elements
   * @param attributeExtractor extractor for attribute for elements
   * @param valueExtractor extractor of values for elements
   * @param timeExtractor extractor of event time
   */
  void persist(
      RepositoryProvider repoProvider,
      EntityDescriptor entity,
      Closure<String> keyExtractor,
      Closure<String> attributeExtractor,
      Closure<T> valueExtractor,
      Closure<Long> timeExtractor);

  /**
   * Directly write this stream to repository.
   * Note that the stream has to contain {@link StreamElement}s (e.g. created by
   * {@link #asStreamElements}.
   * @param repoProvider provider of repository
   */
  void write(RepositoryProvider repoProvider);


  /**
   * Create time windowed stream.
   * @param millis duration of tumbling window
   * @return time windowed stream
   */
  WindowedStream<T> timeWindow(long millis);

  /**
   * Create sliding time windowed stream.
   * @param millis duration of the window
   * @param slide duration of the slide
   * @return sliding time windowed stream
   */
  WindowedStream<T> timeSlidingWindow(long millis, long slide);

  /**
   * Create session windowed stream.
   * @param <K> type of key
   * @param keyExtractor extractor of key
   * @param gapDuration duration of the gap between elements per key
   * @return session windowed stream
   */
  <K> WindowedStream<Pair<K, T>> sessionWindow(
      Closure<K> keyExtractor,
      long gapDuration);

  /**
   * Group all elements into single window.
   * @return globally windowed stream.
   */
  WindowedStream<T> windowAll();

  /**
   * Merge two streams together.
   * @param other the other stream
   * @return merged stream
   */
  Stream<T> union(Stream<T> other);

}
