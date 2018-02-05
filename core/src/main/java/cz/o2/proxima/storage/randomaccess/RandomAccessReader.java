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
package cz.o2.proxima.storage.randomaccess;

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.util.Pair;
import java.io.Closeable;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Reader of data stored in random access storage.
 * Every class that implements both {@code AbstractAttributeWriter} and
 * {@code RandomAccessReader} can be used to access data stored at that
 * attribute family by random access. The data can be either get by
 * pair (key, attribute) or scanned through by a mask (key, attributePrefix)
 * for attributes that are wildcard attributes.
 */
public interface RandomAccessReader extends Closeable, Serializable {

  /** Type of listing (either listing entities of entity attributes). */
  enum Listing {
    ENTITY,
    ATTRIBUTE
  }

  /**
   * Construct {@code Offset} from string
   * (representing either key of the entity or attribute).
   * @param type the type of the key
   * @param key the key of entity or attribute
   * @return offset representation of the key
   */
  RandomOffset fetchOffset(Listing type, String key);

  /**
   * Retrieve data stored under given (key, attribute) pair (if any).
   * @param <T> value type
   * @param key key of the entity
   * @param desc the attribute to search for (not wildcard)
   * @return optional {@link KeyValue} if present
   */
  default <T> Optional<KeyValue<T>> get(
      String key,
      AttributeDescriptor<T> desc) {

    return get(key, desc.getName(), desc);
  }


  /**
   * Retrieve data stored under given (key, attribute) pair (if any).
   * @param <T> value type
   * @param key key of the entity
   * @param attribute name of the attribute
   * @param desc the attribute to search for
   * @return optional {@link KeyValue} if present
   */
  <T> Optional<KeyValue<T>> get(
      String key,
      String attribute,
      AttributeDescriptor<T> desc);


  /**
   * List data stored for a particular wildcard attribute.
   * @param <T> value type
   * @param key key of the entity
   * @param wildcard wildcard attribute to scan
   * @param consumer the consumer to stream data to
   */
  default <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      Consumer<KeyValue<T>> consumer) {

    scanWildcard(key, wildcard, null, -1, consumer);
  }


  /**
   * List data stored for a particular wildcard attribute.
   * @param <T> value type
   * @param key key of the entity
   * @param wildcard wildcard attribute to scan
   * @param offset name of attribute (including the prefix) to start from
   * @param limit maximal number of items to consume
   * @param consumer the consumer to stream data to
   */
  <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      int limit,
      Consumer<KeyValue<T>> consumer);

  /**
   * List all entity keys.
   * @param consumer consumer that will receive keys of entities in the
   * random access storage
   */
  default void listEntities(Consumer<Pair<RandomOffset, String>> consumer) {
    listEntities(null, Integer.MAX_VALUE, consumer);
  }


  /**
   * List all entity keys with offset and limit.
   * @param offset offset of the entities
   * @param limit limit for number of results
   * @param consumer consumer of results
   */
  void listEntities(
      @Nullable RandomOffset offset,
      int limit,
      Consumer<Pair<RandomOffset, String>> consumer);


  /**
   * Retrieve entity associated with this reader.
   * @return entity associated with this reader
   */
  EntityDescriptor getEntityDescriptor();
  
}
