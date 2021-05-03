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

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.view.TimeBoundedVersionedCache.Payload;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.Pair;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/** Test suite for {@link TimeBoundedVersionedCache}. */
public class TimeBoundedVersionedCacheTest {

  Repository repo = Repository.of(ConfigFactory.load("test-reference.conf"));
  EntityDescriptor entity =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalArgumentException("Missing entity gateway"));
  long now = System.currentTimeMillis();

  @Test
  public void testCachePutGet() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test"));
    assertEquals(Pair.of(now, new Payload("test", 1L, true)), cache.get("key", "attribute", now));
    assertEquals(
        Pair.of(now, new Payload("test", 1L, true)), cache.get("key", "attribute", now + 1));
    assertNull(cache.get("key", "attribute", now - 1));
  }

  @Test
  public void testCachePutGetWithCorrectAttribute() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "armed", now, 1L, false, "test"));
    assertEquals(Pair.of(now, new Payload("test", 1L, true)), cache.get("key", "armed", now));
    assertEquals(Pair.of(now, new Payload("test", 1L, true)), cache.get("key", "armed", now + 1));
    assertNull(cache.get("key", "armed", now - 1));
  }

  @Test
  public void testMultiCacheWithinTimeout() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test1"));
    now += 30_000L;
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test2"));
    assertEquals(Pair.of(now, new Payload("test2", 1L, true)), cache.get("key", "attribute", now));
    assertEquals(
        Pair.of(now, new Payload("test2", 1L, true)), cache.get("key", "attribute", now + 1));
    assertEquals(
        Pair.of(now - 30_000L, new Payload("test1", 1L, true)),
        cache.get("key", "attribute", now - 1));
    assertEquals(2, cache.get("key").get("attribute").size());
  }

  @Test
  public void testMultiCacheWithinTimeoutOverwrite() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test1"));
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test2"));
    assertEquals(Pair.of(now, new Payload("test2", 1L, true)), cache.get("key", "attribute", now));
    assertTrue(cache.put("key", "attribute", now, 1L, true, "test3"));
    assertEquals(Pair.of(now, new Payload("test3", 1L, false)), cache.get("key", "attribute", now));
    assertFalse(cache.put("key", "attribute", now, 1L, false, "test4"));
    assertEquals(Pair.of(now, new Payload("test3", 1L, false)), cache.get("key", "attribute", now));
  }

  @Test
  public void testMultiCacheWithinTimeoutReversed() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test1"));
    now -= 30_000L;
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test2"));
    assertEquals(Pair.of(now, new Payload("test2", 1L, true)), cache.get("key", "attribute", now));
    assertEquals(
        Pair.of(now, new Payload("test2", 1L, true)), cache.get("key", "attribute", now + 1));
    assertEquals(
        Pair.of(now + 30_000L, new Payload("test1", 1L, true)),
        cache.get("key", "attribute", now + 31_000L));
    assertNull(cache.get("key", "attribute", now - 1));
    assertEquals(2, cache.get("key").get("attribute").size());
  }

  @Test
  public void testMultiCacheOverTimeout() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test1"));
    now += 120_000L;
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test2"));
    assertEquals(Pair.of(now, new Payload("test2", 1L, true)), cache.get("key", "attribute", now));
    assertEquals(
        Pair.of(now, new Payload("test2", 1L, true)), cache.get("key", "attribute", now + 1));
    assertNull(cache.get("key", "attribute", now - 1));
    assertEquals(1, cache.get("key").get("attribute").size());
  }

  @Test
  public void testMultiCacheOverTimeoutReversed() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "attribute", now, 1L, false, "test1"));
    now -= 120_000L;
    assertFalse(cache.put("key", "attribute", now, 1L, false, "test2"));
    assertEquals(1, cache.get("key").get("attribute").size());
  }

  @Test
  public void testGetWithDelete() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "attribute.suffix", now, 1L, false, null));
    assertTrue(cache.put("key", "attribute.suffix2", now, 1L, false, "value"));
    assertNull(cache.get("key", "attribute.", now + 1));
  }

  @Test
  public void testMultiCacheScan() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "a.1", now, 1L, false, "test1"));
    now += 1;
    assertTrue(cache.put("key", "a.2", now, 1L, false, "test2"));
    assertTrue(cache.put("key", "a.3", now, 1L, false, "test3"));
    Map<String, Pair<Long, Payload>> scanned = new HashMap<>();
    cache.scan(
        "key",
        "a.",
        now,
        k -> null,
        (k, v) -> {
          scanned.put(k, v);
          return true;
        });
    assertEquals(3, scanned.size());

    now += 1;
    assertTrue(cache.put("key", "a.2", now, 1L, false, null));
    scanned.clear();
    cache.scan(
        "key",
        "a.",
        now,
        k -> null,
        (k, v) -> {
          scanned.put(k, v);
          return true;
        });
    assertEquals(3, scanned.size());

    scanned.clear();
    cache.scan(
        "key",
        "a.",
        "a.2",
        now,
        k -> null,
        (k, v) -> {
          scanned.put(k, v);
          return true;
        });
    assertEquals(1, scanned.size());
    assertNotNull(scanned.get("a.3"));
  }

  @Test
  public void testMultiCacheScanWithTombstoneDelete() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    assertTrue(cache.put("key", "a.1", now, 1L, false, "test1"));
    now += 1;
    assertTrue(cache.put("key", "a.2", now, 2L, false, "test2"));
    now += 1;
    // tombstone prefix delete
    assertTrue(cache.put("key", "a.", now, 3L, false, null));
    now += 1;
    assertTrue(cache.put("key", "a.3", now, 4L, false, "test3"));
    Map<String, Pair<Long, Payload>> scanned = new HashMap<>();
    cache.scan(
        "key",
        "a.",
        now,
        k -> "a.",
        (k, v) -> {
          scanned.put(k, v);
          return true;
        });
    assertEquals(1, scanned.size());

    now += 1;
    assertTrue(cache.put("key", "a.2", now, 1L, false, null));
    scanned.clear();
    cache.scan(
        "key",
        "a.",
        now,
        k -> "a.",
        (k, v) -> {
          scanned.put(k, v);
          return true;
        });
    assertEquals(2, scanned.size());

    scanned.clear();
    cache.scan(
        "key",
        "a.",
        "a.2",
        now,
        k -> "a.",
        (k, v) -> {
          scanned.put(k, v);
          return true;
        });
    assertEquals(1, scanned.size());
    assertNotNull(scanned.get("a.3"));

    scanned.clear();
    cache.scan(
        "key",
        "a.",
        now - 3,
        k -> "a.",
        (k, v) -> {
          scanned.put(k, v);
          return true;
        });
    assertEquals(2, scanned.size());
  }
}
