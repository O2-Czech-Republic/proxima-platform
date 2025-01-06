/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.randomaccess;

import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.ContextProvider;
import cz.o2.proxima.direct.core.DirectDataOperator;
import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Reader of data stored in random access storage. Every class that implements both {@code
 * AbstractAttributeWriter} and {@code RandomAccessReader} can be used to access data stored at that
 * attribute family by random access. The data can be either get by pair (key, attribute) or scanned
 * through by a mask (key, attributePrefix) for attributes that are wildcard attributes.
 */
@Stable
public interface RandomAccessReader extends Closeable {

  /** {@link Serializable} factory for {@link RandomAccessReader}. */
  @FunctionalInterface
  interface Factory<T extends RandomAccessReader> extends UnaryFunction<Repository, T> {}

  /** A wraper for a get request. */
  @Value
  class GetRequest<T> {

    public static <T> GetRequest<T> of(
        String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

      return new GetRequest<>(key, attribute, desc, stamp);
    }

    String key;
    String attribute;
    AttributeDescriptor<T> desc;
    long stamp;
  }

  /** A facade for fetching multiple {@link KeyValue KeyValues} in single request. */
  interface MultiFetch {

    /**
     * Add new {@link GetRequest} to the multi fetch.
     *
     * @param <T> type of attribute
     * @param request the request to add
     * @return this
     */
    <T> MultiFetch get(GetRequest<T> request);

    /**
     * Perform the multi fetch and pass results through the provided consumer
     *
     * @param consumer consumer to receive results of the multifetch
     */
    void fetch(Consumer<Pair<GetRequest<?>, Optional<KeyValue<?>>>> consumer);
  }

  /**
   * Create a new builder that is able to construct {@link RandomAccessReader} from multiple readers
   * responsible for reading from various attribute families.
   *
   * @param repo the {@link Repository}
   * @param context direct translation context provider (e.g. {@link DirectDataOperator})
   * @return new builder for multi random access reader
   */
  static MultiAccessBuilder newBuilder(Repository repo, ContextProvider context) {
    return new MultiAccessBuilder(repo, context.getContext());
  }

  /** Type of listing (either listing entities of entity attributes). */
  enum Listing {
    ENTITY,
    ATTRIBUTE
  }

  /**
   * Construct {@code Offset} from string (representing either key of the entity or attribute). The
   * returned offset represents the first element that is <b>following</b> the given {@code key}, in
   * case of equality.
   *
   * @param type the type of the key
   * @param key the key of entity or attribute
   * @return offset representation of the key
   */
  RandomOffset fetchOffset(Listing type, String key);

  /**
   * Retrieve data stored under given (key, attribute) pair (if any).
   *
   * @param <T> value type
   * @param key key of the entity
   * @param desc the attribute to search for (not wildcard)
   * @return optional {@link KeyValue} if present
   */
  default <T> Optional<KeyValue<T>> get(String key, AttributeDescriptor<T> desc) {
    return get(key, desc.getName(), desc, System.currentTimeMillis());
  }

  /**
   * Retrieve data stored under given (key, attribute) pair (if any).
   *
   * @param <T> value type
   * @param key key of the entity
   * @param desc the attribute to search for (not wildcard)
   * @param stamp timestamp to relatively to which retrieve the data
   * @return optional {@link KeyValue} if present
   */
  default <T> Optional<KeyValue<T>> get(String key, AttributeDescriptor<T> desc, long stamp) {
    return get(key, desc.getName(), desc, stamp);
  }

  /**
   * Retrieve data stored under given (key, attribute) pair (if any).
   *
   * @param <T> value type
   * @param key key of the entity
   * @param attribute name of the attribute
   * @param desc the attribute to search for
   * @return optional {@link KeyValue} if present
   */
  default <T> Optional<KeyValue<T>> get(String key, String attribute, AttributeDescriptor<T> desc) {
    return get(key, attribute, desc, System.currentTimeMillis());
  }

  /**
   * Get single {@link KeyValue} using {@link GetRequest}.
   *
   * @param <T> value type
   * @param request the request
   * @return optional {@link KeyValue} if present
   */
  default <T> Optional<KeyValue<T>> get(GetRequest<T> request) {
    return get(request.getKey(), request.getAttribute(), request.getDesc(), request.getStamp());
  }

  /**
   * Retrieve data stored under given (key, attribute) pair (if any).
   *
   * @param <T> value type
   * @param key key of the entity
   * @param attribute name of the attribute
   * @param desc the attribute to search for
   * @param stamp timestamp to relatively to which retrieve the data
   * @return optional {@link KeyValue} if present
   */
  <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp);

  /**
   * Return a facade {@link MultiFetch} object that can be used to queue multiple requests to the
   * storage and fetch them at once using {@link MultiFetch#fetch(Consumer)}.
   *
   * @return the {@link MultiFetch}
   */
  default MultiFetch multiFetch() {
    return new MultiFetch() {

      final List<GetRequest<?>> requests = new ArrayList<>();

      @Override
      public <T> MultiFetch get(GetRequest<T> request) {
        requests.add(request);
        return this;
      }

      @SuppressWarnings({"rawtypes", "unchecked"})
      @Override
      public void fetch(Consumer<Pair<GetRequest<?>, Optional<KeyValue<?>>>> consumer) {
        requests.forEach(
            r -> consumer.accept(Pair.of(r, RandomAccessReader.this.get((GetRequest) r))));
      }
    };
  }

  /**
   * Scan all data stored per given key.
   *
   * @param key the key whose {@link KeyValue}s to scan
   * @param consumer consumer to use for scanning
   */
  default void scanWildcardAll(String key, Consumer<KeyValue<?>> consumer) {
    scanWildcardAll(key, System.currentTimeMillis(), consumer);
  }

  /**
   * Scan all data stored per given key.
   *
   * @param key the key whose {@link KeyValue}s to scan
   * @param stamp timestamp to relatively to which retrieve the data
   * @param consumer consumer to use for scanning
   */
  default void scanWildcardAll(String key, long stamp, Consumer<KeyValue<?>> consumer) {
    scanWildcardAll(key, null, stamp, -1, consumer);
  }

  /**
   * Scan all data stored per given key.
   *
   * @param key the key whose {@link KeyValue}s to scan
   * @param offset offset to start from (next key value will be returned)
   * @param limit how many elements to process at most
   * @param consumer consumer to use for scanning
   */
  default void scanWildcardAll(
      String key, @Nullable RandomOffset offset, int limit, Consumer<KeyValue<?>> consumer) {

    scanWildcardAll(key, offset, System.currentTimeMillis(), limit, consumer);
  }

  /**
   * Scan all data stored per given key.
   *
   * @param key the key whose {@link KeyValue}s to scan
   * @param offset offset to start from (next key value will be returned)
   * @param stamp timestamp to relatively to which retrieve the data
   * @param limit how many elements to process at most
   * @param consumer consumer to use for scanning
   */
  void scanWildcardAll(
      String key,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<?>> consumer);

  /**
   * List data stored for a particular wildcard attribute.
   *
   * @param <T> value type
   * @param key key of the entity
   * @param wildcard wildcard attribute to scan
   * @param consumer the consumer to stream data to
   */
  default <T> void scanWildcard(
      String key, AttributeDescriptor<T> wildcard, Consumer<KeyValue<T>> consumer) {

    scanWildcard(key, wildcard, null, System.currentTimeMillis(), -1, consumer);
  }

  /**
   * List data stored for a particular wildcard attribute.
   *
   * @param <T> value type
   * @param key key of the entity
   * @param wildcard wildcard attribute to scan
   * @param stamp timestamp to relatively to which retrieve the data
   * @param consumer the consumer to stream data to
   */
  default <T> void scanWildcard(
      String key, AttributeDescriptor<T> wildcard, long stamp, Consumer<KeyValue<T>> consumer) {

    scanWildcard(key, wildcard, null, stamp, -1, consumer);
  }

  /**
   * List data stored for a particular wildcard attribute.
   *
   * @param <T> value type
   * @param key key of the entity
   * @param wildcard wildcard attribute to scan
   * @param offset name of attribute (including the prefix) to start from
   * @param limit maximal number of items to consume
   * @param consumer the consumer to stream data to
   */
  default <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      int limit,
      Consumer<KeyValue<T>> consumer) {

    scanWildcard(key, wildcard, offset, System.currentTimeMillis(), limit, consumer);
  }

  /**
   * List data stored for a particular wildcard attribute.
   *
   * @param <T> value type
   * @param key key of the entity
   * @param wildcard wildcard attribute to scan
   * @param offset name of attribute (including the prefix) to start from
   * @param stamp timestamp to relatively to which retrieve the data
   * @param limit maximal number of items to consume *
   * @param consumer the consumer to stream data to
   */
  <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer);

  /**
   * List all entity keys.
   *
   * @param consumer consumer that will receive keys of entities in the random access storage
   */
  default void listEntities(Consumer<Pair<RandomOffset, String>> consumer) {
    listEntities(null, Integer.MAX_VALUE, consumer);
  }

  /**
   * List all entity keys with offset and limit.
   *
   * @param offset offset of the entities
   * @param limit limit for number of results
   * @param consumer consumer of results
   */
  void listEntities(
      @Nullable RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer);

  /**
   * Retrieve entity associated with this reader.
   *
   * @return entity associated with this reader
   */
  EntityDescriptor getEntityDescriptor();

  /**
   * Convert instance of this reader to {@link Factory} suitable for serialization.
   *
   * @return the {@link Factory} representing this reader
   */
  Factory<?> asFactory();
}
