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
package cz.o2.proxima.tools.io;

import com.google.common.io.Closeables;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.Pair;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/** A random access reader for console. */
public class ConsoleRandomReader implements Closeable {

  final EntityDescriptor entityDesc;
  final Map<AttributeDescriptor, RandomAccessReader> attrToReader;
  final Map<String, RandomOffset> listEntityOffsets;

  public ConsoleRandomReader(EntityDescriptor desc, Repository repo, DirectDataOperator direct) {

    this.entityDesc = desc;
    this.attrToReader = new HashMap<>();
    this.listEntityOffsets = new HashMap<>();

    desc.getAllAttributes()
        .forEach(
            f -> {
              Optional<AttributeFamilyDescriptor> randomFamily;
              randomFamily =
                  repo.getFamiliesForAttribute(f)
                      .stream()
                      .filter(af -> af.getAccess().canRandomRead())
                      .findAny();
              if (randomFamily.isPresent()) {
                attrToReader.put(
                    f, direct.resolveRequired(randomFamily.get()).getRandomAccessReader().get());
              }
            });
  }

  @SuppressWarnings("unchecked")
  public <T> KeyValue<T> get(String key, String attribute) {

    AttributeDescriptor<Object> desc =
        entityDesc
            .findAttribute(attribute)
            .orElseThrow(() -> new IllegalArgumentException("Unknown attribute " + attribute));

    RandomAccessReader reader = attrToReader.get(desc);
    if (reader == null) {
      throw new IllegalArgumentException("Attribute " + attribute + " has no random access reader");
    }
    return (KeyValue<T>) reader.get(key, attribute, desc).orElse(null);
  }

  @SuppressWarnings("unchecked")
  public List<KeyValue> list(String key, String prefix) {
    return list(key, prefix, null);
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
  public void list(
      String key, String prefix, @Nullable String offset, int limit, Consumer<KeyValue> consumer) {

    AttributeDescriptor desc =
        entityDesc
            .findAttribute(prefix + ".*")
            .orElseThrow(() -> new IllegalArgumentException("Unknown attribute " + prefix + ".*"));

    RandomAccessReader reader = attrToReader.get(desc);
    if (reader == null) {
      throw new IllegalArgumentException("Attribute " + prefix + " has no random access reader");
    }
    RandomOffset off =
        offset == null ? null : reader.fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, offset);
    reader.scanWildcard(key, desc, off, limit, consumer::accept);
  }

  public void listKeys(Consumer<Pair<RandomOffset, String>> consumer) {
    RandomAccessReader reader =
        attrToReader
            .values()
            .stream()
            .findAny()
            .orElseThrow(
                () -> new IllegalStateException("Have no random readers for entity " + entityDesc));

    reader.listEntities(
        p -> {
          RandomOffset off = p.getFirst();
          listEntityOffsets.put(p.getSecond(), off);
          consumer.accept(p);
        });
  }

  public List<Pair<RandomOffset, String>> listKeys(String start, int limit) {
    RandomAccessReader reader =
        attrToReader
            .values()
            .stream()
            .findAny()
            .orElseThrow(
                () -> new IllegalStateException("Have no random readers for entity " + entityDesc));
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

  @Override
  public void close() {
    attrToReader
        .values()
        .forEach(
            r -> {
              try {
                Closeables.close(r, false);
              } catch (IOException ex) {
                throw new RuntimeException(ex);
              }
            });
    attrToReader.clear();
  }
}
