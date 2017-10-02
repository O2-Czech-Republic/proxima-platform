/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader.Offset;
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

/**
 * A random access reader for console.
 */
public class ConsoleRandomReader implements Closeable {

  final EntityDescriptor entityDesc;
  final Map<AttributeDescriptor, RandomAccessReader> attrToReader;
  final Map<String, Offset> listAttrOffsets;
  final Map<String, Offset> listEntityOffsets;

  public ConsoleRandomReader(EntityDescriptor desc, Repository repo) {

    this.entityDesc = desc;
    this.attrToReader = new HashMap<>();
    this.listAttrOffsets = new HashMap<>();
    this.listEntityOffsets = new HashMap<>();

    desc.getAllAttributes().forEach(f -> {
      Optional<AttributeFamilyDescriptor<?>> randomFamily;
      randomFamily = repo.getFamiliesForAttribute(f)
          .stream()
          .filter(af -> af.getAccess().isListPrimaryKey()
              && af.getAccess().canRandomRead())
          .findAny();
      if (randomFamily.isPresent()) {
        attrToReader.put(f, randomFamily.get().getRandomAccessReader().get());
      }
    });
  }

  public KeyValue<?> get(String key, String attribute) {

    AttributeDescriptor desc = entityDesc.findAttribute(attribute)
        .orElseThrow(() -> new IllegalArgumentException(
            "Unknown attribute " + attribute));

    RandomAccessReader reader = attrToReader.get(desc);
    if (reader == null) {
      throw new IllegalArgumentException("Attribute " + attribute
          + " has no random access reader");
    }
    return reader.get(key, attribute, desc).orElse(null);
  }


  public List<KeyValue<?>> list(String key, String prefix) {
    List<KeyValue<?>> ret = new ArrayList<>();
    list(key, prefix, null, -1, ret::add);
    return ret;
  }


  public List<KeyValue<?>> list(
      String key,
      String prefix,
      @Nullable String offset,
      int limit) {

    List<KeyValue<?>> ret = new ArrayList<>();
    list(key, prefix, offset, limit, ret::add);
    return ret;
  }


  public void list(
      String key,
      String prefix,
      @Nullable String offset,
      int limit,
      Consumer<KeyValue<?>> consumer) {

    AttributeDescriptor desc = entityDesc.findAttribute(prefix + ".*")
        .orElseThrow(() -> new IllegalArgumentException(
            "Unknown attribute " + prefix + ".*"));

    RandomAccessReader reader = attrToReader.get(desc);
    if (reader == null) {
      throw new IllegalArgumentException(
          "Attribute " + prefix + " has no random access reader");
    }
    Offset off = offset == null ? null : listAttrOffsets.get(offset);
    reader.scanWildcard(key, desc,
        off,
        limit, kv -> {
          if (off == null) {
            listAttrOffsets.put(kv.getAttribute(), kv.getOffset());
          }
          consumer.accept(kv);
        });
  }


  public void listKeys(Consumer<Pair<Offset, String>> consumer) {
    RandomAccessReader reader = attrToReader.values().stream()
        .findAny().orElseThrow(
        () -> new IllegalStateException(
            "Have no random readers for entity " + entityDesc));

    reader.listEntities(p -> {
      Offset off = p.getFirst();
      listEntityOffsets.put(p.getSecond(), off);
      consumer.accept(p);
    });
  }


  public List<Pair<Offset, String>> listKeys(String start, int limit) {
    RandomAccessReader reader = attrToReader.values().stream()
        .findAny().orElseThrow(
        () -> new IllegalStateException(
            "Have no random readers for entity " + entityDesc));
    List<Pair<Offset, String>> ret = new ArrayList<>();
    reader.listEntities(listEntityOffsets.get(start), limit, p -> {
      listEntityOffsets.put(p.getSecond(), p.getFirst());
      ret.add(p);
    });
    return ret;
  }

  @Override
  public void close() {
    attrToReader.values().forEach(r -> {
      try {
        Closeables.close(r, false);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
    attrToReader.clear();
  }

}
