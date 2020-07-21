/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.randomaccess;

import com.google.common.collect.Iterables;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** A builder for {@link RandomAccessReader} reading from multiple attribute families. */
@Stable
@Slf4j
public class MultiAccessBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private static class SequentialOffset implements RandomOffset {

    private static final long serialVersionUID = 1L;

    private final Map<RandomAccessReader, RandomOffset> offsetMap;

    SequentialOffset(Map<RandomAccessReader, RandomOffset> offsetMap) {
      this.offsetMap = offsetMap;
    }

    SequentialOffset(SequentialOffset copy) {
      this.offsetMap = new HashMap<>(copy.offsetMap);
    }

    SequentialOffset update(RandomAccessReader reader, RandomOffset offset) {
      offsetMap.put(reader, offset);
      return this;
    }
  }

  private transient Repository repo;
  private final Context context;
  private final Map<AttributeDescriptor<?>, RandomAccessReader.Factory<?>> attrMapToFactory;

  MultiAccessBuilder(Repository repo, Context context) {
    this.repo = Objects.requireNonNull(repo);
    this.attrMapToFactory = new HashMap<>();
    this.context = context;
  }

  /**
   * Add specified attributes to be read with given reader.
   *
   * @param reader the reader to use to read attributes
   * @param attrs the attributes to read with specified reader
   * @return this
   */
  public MultiAccessBuilder addAttributes(
      RandomAccessReader reader, AttributeDescriptor<?>... attrs) {

    for (AttributeDescriptor<?> a : attrs) {
      attrMapToFactory.put(a, reader.asFactory());
    }
    return this;
  }

  /**
   * Add specified family to be read with given reader.
   *
   * @param family family to access with the built reader
   * @return this
   */
  public MultiAccessBuilder addFamily(AttributeFamilyDescriptor family) {

    RandomAccessReader reader =
        context
            .resolveRequired(family)
            .getRandomAccessReader()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Family " + family + " has no random access reader"));

    family.getAttributes().forEach(a -> attrMapToFactory.put(a, reader.asFactory()));
    return this;
  }

  /**
   * Create {@link RandomAccessReader} for attributes and/or families specified in this builder.
   *
   * @return {@link RandomAccessReader} capable of reading from multiple attribute families.
   */
  public RandomAccessReader build() {
    final Map<AttributeDescriptor<?>, RandomAccessReader> attrMapToReader =
        materializeReaders(repo);
    final EntityDescriptor entity = getSingleEntityOrNull(attrMapToReader);

    return new RandomAccessReader() {

      @Override
      public RandomOffset fetchOffset(RandomAccessReader.Listing type, String key) {

        if (type == Listing.ENTITY) {
          throw new UnsupportedOperationException(
              "Please use specific attribute family to scan entities.");
        }
        Map<RandomAccessReader, RandomOffset> offsets =
            attrMapToReader
                .values()
                .stream()
                .distinct()
                .map(ra -> Pair.of(ra, ra.fetchOffset(type, key)))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
        return new SequentialOffset(offsets);
      }

      @Override
      public <T> Optional<KeyValue<T>> get(
          String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

        return Optional.ofNullable(attrMapToReader.get(desc))
            .map(ra -> ra.get(key, attribute, desc, stamp))
            .orElseGet(
                () -> {
                  log.warn("Missing family for attribute {} in MultiAccessBuilder", desc);
                  return Optional.empty();
                });
      }

      @SuppressWarnings("unchecked")
      @Override
      public void scanWildcardAll(
          String key, RandomOffset offset, long stamp, int limit, Consumer<KeyValue<?>> consumer) {

        AtomicInteger missing = new AtomicInteger(limit);
        SequentialOffset soff = (SequentialOffset) offset;
        final AtomicReference<SequentialOffset> current = new AtomicReference<>();
        if (soff != null) {
          current.set(new SequentialOffset(soff));
        } else {
          Map<RandomAccessReader, RandomOffset> m = new HashMap<>();
          attrMapToReader.values().stream().distinct().forEach(ra -> m.put(ra, null));
          current.set(new SequentialOffset(m));
        }
        current
            .get()
            .offsetMap
            .entrySet()
            .forEach(
                e ->
                    e.getKey()
                        .scanWildcardAll(
                            key,
                            e.getValue(),
                            stamp,
                            missing.get(),
                            kv -> {
                              missing.decrementAndGet();
                              current.set(
                                  new SequentialOffset(current.get())
                                      .update(e.getKey(), kv.getOffset()));
                              KeyValue<?> mapped =
                                  KeyValue.of(
                                      kv.getEntityDescriptor(),
                                      (AttributeDescriptor) kv.getAttributeDescriptor(),
                                      kv.getKey(),
                                      kv.getAttribute(),
                                      current.get(),
                                      kv.getValue(),
                                      kv.getValue(),
                                      kv.getStamp());
                              consumer.accept(mapped);
                            }));
      }

      @Override
      public <T> void scanWildcard(
          String key,
          AttributeDescriptor<T> wildcard,
          RandomOffset offset,
          long stamp,
          int limit,
          Consumer<KeyValue<T>> consumer) {

        Optional.ofNullable(attrMapToReader.get(wildcard))
            .ifPresent(ra -> ra.scanWildcard(key, wildcard, offset, stamp, limit, consumer));
      }

      @Override
      public void listEntities(
          RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

        throw new UnsupportedOperationException(
            "Not supported. Please select specific family to list entities from.");
      }

      @Override
      public EntityDescriptor getEntityDescriptor() {
        if (entity != null) {
          return entity;
        }
        throw new IllegalArgumentException(
            "Multiple options. This is compound reader that can work " + "on multiple entities.");
      }

      @Override
      public Factory<?> asFactory() {
        return repo -> {
          MultiAccessBuilder builder = new MultiAccessBuilder(repo, context);
          attrMapToFactory.forEach(
              (attr, factory) -> builder.addAttributes(factory.apply(repo), attr));
          return builder.build();
        };
      }

      @Override
      public void close() {
        attrMapToReader.values().forEach(this::closeQuietly);
      }

      private void closeQuietly(RandomAccessReader c) {
        try {
          c.close();
        } catch (IOException ex) {
          log.warn("Failed to close {}", c, ex);
        }
      }
    };
  }

  private Map<AttributeDescriptor<?>, RandomAccessReader> materializeReaders(Repository repo) {
    return attrMapToFactory
        .entrySet()
        .stream()
        .map(e -> Pair.of(e.getKey(), e.getValue().apply(repo)))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private @Nullable EntityDescriptor getSingleEntityOrNull(
      Map<AttributeDescriptor<?>, RandomAccessReader> attrMap) {

    Set<EntityDescriptor> entities =
        attrMap
            .values()
            .stream()
            .map(RandomAccessReader::getEntityDescriptor)
            .collect(Collectors.toSet());
    if (entities.size() == 1) {
      return Objects.requireNonNull(Iterables.getOnlyElement(entities));
    }
    log.debug(
        "Attribute map {} contains multiple entities. Some functionality "
            + "of this multi access reader might be limited.",
        attrMap);
    return null;
  }
}
