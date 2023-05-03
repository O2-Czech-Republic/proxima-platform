/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.io;

import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** A random access reader for console. */
@Slf4j
public class ConsoleRandomReader implements AutoCloseable {

  final DirectDataOperator direct;
  final EntityDescriptor entityDesc;
  final Map<String, RandomOffset> listEntityOffsets;
  final Map<AttributeDescriptor<?>, RandomAccessReader> readers = new HashMap<>();

  public ConsoleRandomReader(EntityDescriptor desc, DirectDataOperator direct) {
    this.direct = direct;
    this.entityDesc = desc;
    this.listEntityOffsets = new HashMap<>();
  }

  @SuppressWarnings("unchecked")
  public <T> KeyValue<T> get(String key, String attribute) {
    AttributeDescriptor<Object> desc =
        entityDesc
            .findAttribute(attribute)
            .orElseThrow(() -> new IllegalArgumentException("Unknown attribute " + attribute));

    RandomAccessReader reader = readerFor(desc);
    return (KeyValue<T>) reader.get(key, attribute, desc).orElse(null);
  }

  public List<KeyValue> list(String key, String prefix) {
    return list(key, prefix, null);
  }

  public List<KeyValue> list(String key, String prefix, @Nullable String offset) {
    List<KeyValue> ret = new ArrayList<>();
    list(key, prefix, offset, -1, ret::add);
    return ret;
  }

  public List<KeyValue> list(String key, String prefix, @Nullable String offset, int limit) {
    List<KeyValue> ret = new ArrayList<>();
    list(key, prefix, offset, limit, ret::add);
    return ret;
  }

  public void list(
      String key,
      String prefix,
      @Nullable String offset,
      int limit,
      Consumer<KeyValue<?>> consumer) {

    boolean isPrefix = prefix.contains(".");
    Preconditions.checkArgument(!isPrefix || offset == null);
    AttributeDescriptor<Object> desc =
        entityDesc
            .findAttribute(isPrefix ? prefix : prefix + ".*")
            .orElseThrow(() -> new IllegalArgumentException("Unknown attribute " + prefix + ".*"));

    RandomAccessReader reader = readerFor(desc);
    RandomOffset off =
        offset == null ? null : reader.fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, offset);
    cz.o2.proxima.functional.Consumer<KeyValue<Object>> scanConsumer =
        isPrefix
            ? kv -> {
              if (kv.getAttribute().startsWith(prefix)) {
                consumer.accept(kv);
              }
            }
            : consumer::accept;
    reader.scanWildcard(key, desc, off, limit, scanConsumer);
  }

  public void listKeys(Consumer<Pair<RandomOffset, String>> consumer) {
    RandomAccessReader reader = getRandomAccessForListKeys(entityDesc);
    reader.listEntities(
        p -> {
          RandomOffset off = p.getFirst();
          listEntityOffsets.put(p.getSecond(), off);
          consumer.accept(p);
        });
  }

  public List<Pair<RandomOffset, String>> listKeys(String start, int limit) {
    RandomAccessReader reader = getRandomAccessForListKeys(entityDesc);
    List<Pair<RandomOffset, String>> ret = new ArrayList<>();
    reader.listEntities(
        listEntityOffsets.get(start),
        limit,
        p -> {
          listEntityOffsets.put(p.getSecond(), p.getFirst());
          ret.add(p);
        });
    return ret;
  }

  private RandomAccessReader getRandomAccessForListKeys(EntityDescriptor entityDesc) {
    return entityDesc.getAllAttributes(true).stream()
        .flatMap(a -> direct.getFamiliesForAttribute(a).stream())
        .filter(af -> af.getDesc().getAccess().isListPrimaryKey())
        .flatMap(af -> af.getAttributes().stream())
        .findAny()
        .map(this::readerFor)
        .orElseThrow(
            () -> new IllegalArgumentException("Missing list-keys family for " + entityDesc));
  }

  @Override
  public void close() {
    log.debug("Closing readers {} for entity {}", readers.values(), entityDesc);
    readers.values().forEach(ExceptionUtils.uncheckedConsumer(RandomAccessReader::close)::accept);
    readers.clear();
  }

  @Nonnull
  private RandomAccessReader readerFor(AttributeDescriptor<?> desc) {
    RandomAccessReader res = readers.get(desc);
    if (res != null) {
      return res;
    }
    Pair<List<AttributeDescriptor<?>>, RandomAccessReader> randomAccessForAttributes =
        direct.getFamiliesForAttribute(desc).stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .map(af -> Pair.of(af.getAttributes(), Optionals.get(af.getRandomAccessReader())))
            .orElseThrow(
                () -> new IllegalStateException("Cannot find random-access family for " + desc));
    randomAccessForAttributes
        .getFirst()
        .forEach(attr -> readers.put(attr, randomAccessForAttributes.getSecond()));
    return Objects.requireNonNull(readers.get(desc));
  }
}
