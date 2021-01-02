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
package cz.o2.proxima.direct.view;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** A cache for data based on timestamp. */
@Slf4j
class TimeBoundedVersionedCache implements Serializable {

  private static final long serialVersionUID = 1L;

  @Value
  private static class Payload {
    @Nullable Object data;
    boolean overridable;
  }

  private final EntityDescriptor entity;

  private final long keepDuration;

  private final Map<String, NavigableMap<String, NavigableMap<Long, Payload>>> cache;

  TimeBoundedVersionedCache(EntityDescriptor entity, long keepDuration) {
    this.entity = entity;
    this.keepDuration = keepDuration;
    this.cache = new LinkedHashMap<>();
  }

  @Nullable
  synchronized Pair<Long, Object> get(String key, String attribute, long stamp) {

    NavigableMap<String, NavigableMap<Long, Payload>> attrMap;
    attrMap = cache.get(key);
    if (attrMap != null) {
      NavigableMap<Long, Payload> valueMap = attrMap.get(attribute);
      if (valueMap != null) {
        Map.Entry<Long, Payload> floorEntry = valueMap.floorEntry(stamp);
        if (floorEntry != null) {
          return Pair.of(floorEntry.getKey(), floorEntry.getValue().getData());
        }
      }
    }
    return null;
  }

  @VisibleForTesting
  NavigableMap<String, NavigableMap<Long, Payload>> get(String key) {
    return cache.get(key);
  }

  void scan(
      String key,
      String prefix,
      long stamp,
      UnaryFunction<String, String> parentRecordExtractor,
      BiFunction<String, Pair<Long, Object>, Boolean> consumer) {

    scan(key, prefix, prefix, stamp, parentRecordExtractor, consumer);
  }

  synchronized void scan(
      String key,
      String prefix,
      String offset,
      long stamp,
      UnaryFunction<String, String> parentRecordExtractor,
      BiFunction<String, Pair<Long, Object>, Boolean> consumer) {

    NavigableMap<String, NavigableMap<Long, Payload>> attrMap;
    attrMap = cache.get(key);
    if (attrMap == null) {
      return;
    }
    String lastParent = null;
    Pair<Long, Object> parentEntry = null;
    long parentTombstoneStamp = stamp;
    for (Map.Entry<String, NavigableMap<Long, Payload>> e : attrMap.tailMap(offset).entrySet()) {

      if (e.getKey().startsWith(prefix)) {
        if (!e.getKey().equals(offset)) {
          if (lastParent == null || !e.getKey().startsWith(lastParent)) {
            lastParent = parentRecordExtractor.apply(e.getKey());
            parentEntry = lastParent == null ? null : get(key, lastParent, stamp);
            boolean isDelete = parentEntry != null && parentEntry.getSecond() == null;
            parentTombstoneStamp = isDelete ? parentEntry.getFirst() : -1;
          }
          Map.Entry<Long, Payload> floorEntry = e.getValue().floorEntry(stamp);
          if (floorEntry != null
              && parentTombstoneStamp < floorEntry.getKey()
              && !consumer.apply(
                  e.getKey(), Pair.of(floorEntry.getKey(), floorEntry.getValue().getData()))) {
            return;
          }
        }
      } else {
        return;
      }
    }
  }

  synchronized int findPosition(String key) {
    if (key.isEmpty()) {
      return 0;
    }
    int ret = 0;
    for (String k : cache.keySet()) {
      if (k.equals(key)) {
        break;
      }
      ret++;
    }
    return ret;
  }

  synchronized void keys(int offset, int limit, Consumer<String> keyConsumer) {
    int toTake = limit;
    int toSkip = offset;
    for (String key : cache.keySet()) {
      if (toSkip-- <= 0) {
        if (toTake != 0) {
          keyConsumer.accept(key);
          toTake--;
        } else {
          break;
        }
      }
    }
  }

  synchronized boolean put(
      String key, String attribute, long stamp, boolean overwrite, @Nullable Object value) {

    AtomicBoolean updated = new AtomicBoolean();
    cache.compute(
        key,
        (k, attrMap) -> {
          if (attrMap == null) {
            attrMap = new TreeMap<>();
          }
          NavigableMap<Long, Payload> valueMap =
              attrMap.computeIfAbsent(attribute, tmp -> new TreeMap<>());
          if (valueMap.isEmpty() || valueMap.firstKey() - keepDuration < stamp) {
            final Payload oldPayload = valueMap.get(stamp);
            if (overwrite || oldPayload == null || oldPayload.overridable) {
              logPayloadUpdateIfNecessary(key, attribute, stamp, value);
              Payload newPayload = new Payload(value, !overwrite);
              valueMap.put(stamp, newPayload);
              updated.set(!newPayload.equals(oldPayload));
            }
          }
          long first;
          while ((first = valueMap.firstKey()) + keepDuration < stamp) {
            valueMap.remove(first);
          }
          return attrMap;
        });
    return updated.get();
  }

  private void logPayloadUpdateIfNecessary(
      String key, String attribute, long stamp, @Nullable Object value) {

    if (log.isDebugEnabled()) {
      AttributeDescriptor<Object> attrDesc = entity.findAttribute(attribute).orElse(null);
      if (attrDesc != null) {
        log.debug(
            "Caching attribute {} for key {} at {} with payload {}",
            attribute,
            key,
            stamp,
            value == null ? "(null)" : attrDesc.getValueSerializer().getLogString(value));
      } else {
        log.warn("Failed to find attribute descriptor {} in {}", attribute, entity);
      }
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), keepDuration, entity);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TimeBoundedVersionedCache) {
      return super.equals(o)
          && entity.equals(((TimeBoundedVersionedCache) o).entity)
          && ((TimeBoundedVersionedCache) o).keepDuration == keepDuration;
    }
    return false;
  }

  public synchronized void clear() {
    cache.clear();
  }
}
