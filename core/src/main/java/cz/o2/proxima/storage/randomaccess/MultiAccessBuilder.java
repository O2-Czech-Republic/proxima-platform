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

import com.google.common.collect.Iterables;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.util.Pair;
import cz.seznam.euphoria.core.util.ExceptionUtils;
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

/**
 * A builder for {@link RandomAccessReader} reading from multiple attribute families.
 */
@Stable
@Slf4j
public class MultiAccessBuilder implements Serializable {

  private static class SequentialOffset implements RandomOffset {

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

  private final Map<AttributeDescriptor<?>, RandomAccessReader> attrMap;

  MultiAccessBuilder() {
    this.attrMap = new HashMap<>();
  }

  /**
   * Add specified attributes to be read with given reader.
   * @param reader the reader to use to read attributes
   * @param attrs the attributes to read with specified reader
   * @return this
   */
  public MultiAccessBuilder addAttributes(
      RandomAccessReader reader, AttributeDescriptor<?>... attrs) {

    for (AttributeDescriptor<?> a : attrs) {
      attrMap.put(a, reader);
    }
    return this;
  }

  /**
   * Add specified family to be read with given reader.
   * @param family family to access with the built reader
   * @return this
   */
  public MultiAccessBuilder addFamily(
      AttributeFamilyDescriptor family) {
    RandomAccessReader reader = family.getRandomAccessReader().orElseThrow(
        () -> new IllegalArgumentException(
            "Family " + family + " has no random access reader"));
    family.getAttributes().forEach(a -> attrMap.put(a, reader));
    return this;
  }

  /**
   * Create {@link RandomAccessReader} for attributes and/or families
   * specified in this builder.
   * @return {@link RandomAccessReader} capable of reading from multiple
   * attribute families.
   */
  public RandomAccessReader build() {

    final EntityDescriptor entity = getSingleEntityOrNull(attrMap);

    return new RandomAccessReader() {

      @Override
      public RandomOffset fetchOffset(
          RandomAccessReader.Listing type, String key) {

        if (type == Listing.ENTITY) {
          throw new UnsupportedOperationException(
              "Please use specific attribute family to scan entities.");
        }
        Map<RandomAccessReader, RandomOffset> offsets = attrMap.values()
            .stream()
            .distinct()
            .map(ra -> Pair.of(ra, ra.fetchOffset(type, key)))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
        return new SequentialOffset(offsets);
      }

      @Override
      public <T> Optional<KeyValue<T>> get(
          String key, String attribute, AttributeDescriptor<T> desc) {

        return Optional.ofNullable(attrMap.get(desc))
            .map(ra -> ra.get(key, attribute, desc))
            .orElseGet(() -> {
              log.warn("Missing family for attribute {} in MultiAccessBuilder", desc);
              return Optional.empty();
            });
      }

      @SuppressWarnings("unchecked")
      @Override
      public void scanWildcardAll(
          String key, RandomOffset offset, int limit,
          Consumer<KeyValue<?>> consumer) {

        AtomicInteger missing = new AtomicInteger(limit);
        SequentialOffset soff = (SequentialOffset) offset;
        final AtomicReference<SequentialOffset> current = new AtomicReference<>();
        if (soff != null) {
          current.set(new SequentialOffset(soff));
        } else {
          Map<RandomAccessReader, RandomOffset> m = new HashMap<>();
          attrMap.values()
              .stream()
              .distinct()
              .forEach(ra -> m.put(ra, null));
          current.set(new SequentialOffset(m));
        }
        for (Map.Entry<RandomAccessReader, RandomOffset> e : current.get().offsetMap.entrySet()) {
          e.getKey().scanWildcardAll(key, e.getValue(), missing.get(), kv -> {
            missing.decrementAndGet();
            current.set(new SequentialOffset(current.get())
                .update(e.getKey(), kv.getOffset()));
            KeyValue mapped = KeyValue.of(
                kv.getEntityDescriptor(), (AttributeDescriptor) kv.getAttrDescriptor(),
                kv.getKey(), kv.getAttribute(),
                current.get(), (Object) kv.getValue(), kv.getValueBytes(), kv.getStamp());
            consumer.accept(mapped);
          });
        }
      }

      @Override
      public <T> void scanWildcard(
          String key, AttributeDescriptor<T> wildcard,
          RandomOffset offset, int limit, Consumer<KeyValue<T>> consumer) {

        Optional.ofNullable(attrMap.get(wildcard))
            .ifPresent(ra -> ra.scanWildcard(key, wildcard, offset, limit, consumer));
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
            "Multiple options. This is compound reader that can work on multiple entities.");
      }

      @Override
      public void close() throws IOException {
        attrMap.values().forEach(
            ExceptionUtils.unchecked(RandomAccessReader::close));
      }

    };
  }

  private @Nullable EntityDescriptor getSingleEntityOrNull(
      Map<AttributeDescriptor<?>, RandomAccessReader> attrMap) {

    Set<EntityDescriptor> entities = attrMap.values().stream()
        .map(RandomAccessReader::getEntityDescriptor)
        .collect(Collectors.toSet());
    if (entities.size() == 1) {
      return Objects.requireNonNull(Iterables.getOnlyElement(entities));
    }
    log.debug(
        "Attribute map {} contains multiple entities. Some functionality "
            + "of this multi access reader might be limited.", attrMap);
    return null;
  }

}
