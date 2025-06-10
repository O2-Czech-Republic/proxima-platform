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
package cz.o2.proxima.direct.core.view;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.view.TimeBoundedVersionedCache.Payload;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

/** Test suite for {@link TimeBoundedVersionedCache}. */
public class TimeBoundedVersionedCacheTest {

  Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf"));
  EntityDescriptor entity = Optionals.get(repo.findEntity("gateway"));
  long now = System.currentTimeMillis();

  @Test
  public void testCachePutGet() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute", now, 1L, "test");
    assertTrue(cache.put(element, false));
    assertEquals(Pair.of(now, new Payload(element, true)), cache.get("key", "attribute", now));
    assertEquals(Pair.of(now, new Payload(element, true)), cache.get("key", "attribute", now + 1));
    assertNull(cache.get("key", "attribute", now - 1));
  }

  @Test
  public void testCacheProxyPutGet() {
    entity = Optionals.get(repo.findEntity("proxied"));
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    String payload = "test";
    StreamElement element = _el("key", "_e.test", now, 1L, payload);
    assertTrue(cache.put(element, false));
    assertEquals(Pair.of(now, new Payload(element, true)), cache.get("key", "_e.test", now));
    assertEquals(Pair.of(now, new Payload(element, true)), cache.get("key", "_e.test", now + 1));
    assertNull(cache.get("key", "_e.test", now - 1));
  }

  @Test
  public void testCachePutGetWithCorrectAttribute() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "armed", now, 1L, "test");
    assertTrue(cache.put(element, false));
    assertEquals(Pair.of(now, new Payload(element, true)), cache.get("key", "armed", now));
    assertEquals(Pair.of(now, new Payload(element, true)), cache.get("key", "armed", now + 1));
    assertNull(cache.get("key", "armed", now - 1));
  }

  @Test
  public void testMultiCacheWithinTimeout() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute", now, 1L, "test1");
    assertTrue(cache.put(element, false));
    now += 30_000L;
    StreamElement element2 = _el("key", "attribute", now, 1L, "test1");
    assertTrue(cache.put(element2, false));
    assertEquals(Pair.of(now, new Payload(element2, true)), cache.get("key", "attribute", now));
    assertEquals(Pair.of(now, new Payload(element2, true)), cache.get("key", "attribute", now + 1));
    assertEquals(
        Pair.of(now - 30_000L, new Payload(element, true)), cache.get("key", "attribute", now - 1));
    assertEquals(2, cache.get("key").get("attribute").size());
  }

  @Test
  public void testMultiCacheWithinTimeoutOverwrite() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute", now, 1L, "test1");
    StreamElement element2 = _el("key", "attribute", now, 1L, "test2");
    StreamElement element3 = _el("key", "attribute", now, 1L, "test3");
    StreamElement element4 = _el("key", "attribute", now, 1L, "test4");
    assertTrue(cache.put(element, false));
    assertTrue(cache.put(element2, false));
    assertEquals(Pair.of(now, new Payload(element2, true)), cache.get("key", "attribute", now));
    assertTrue(cache.put(element3, true));
    assertEquals(Pair.of(now, new Payload(element3, false)), cache.get("key", "attribute", now));
    assertFalse(cache.put(element4, false));
    assertEquals(Pair.of(now, new Payload(element3, false)), cache.get("key", "attribute", now));
  }

  @Test
  public void testMultiCacheWithinTimeoutReversed() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute", now, 1L, "test1");
    assertTrue(cache.put(element, false));
    now -= 30_000L;
    StreamElement element2 = _el("key", "attribute", now, 1L, "test2");
    assertTrue(cache.put(element2, false));
    assertEquals(Pair.of(now, new Payload(element2, true)), cache.get("key", "attribute", now));
    assertEquals(Pair.of(now, new Payload(element2, true)), cache.get("key", "attribute", now + 1));
    assertEquals(
        Pair.of(now + 30_000L, new Payload(element, true)),
        cache.get("key", "attribute", now + 31_000L));
    assertNull(cache.get("key", "attribute", now - 1));
    assertEquals(2, cache.get("key").get("attribute").size());
  }

  @Test
  public void testMultiCacheOverTimeout() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute", now, 1L, "test1");
    assertTrue(cache.put(element, false));
    now += 120_000L;
    StreamElement element2 = _el("key", "attribute", now, 1L, "test2");
    assertTrue(cache.put(element2, false));
    assertEquals(Pair.of(now, new Payload(element2, true)), cache.get("key", "attribute", now));
    assertEquals(Pair.of(now, new Payload(element2, true)), cache.get("key", "attribute", now + 1));
    assertNull(cache.get("key", "attribute", now - 1));
    assertEquals(1, cache.get("key").get("attribute").size());
  }

  @Test
  public void testMultiCacheOverTimeoutReversed() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute", now, 1L, "test1");
    assertTrue(cache.put(element, false));
    now -= 120_000L;
    StreamElement element2 = _el("key", "attribute", now, 1L, "test2");
    assertFalse(cache.put(element2, false));
    assertEquals(1, cache.get("key").get("attribute").size());
  }

  @Test
  public void testGetWithDelete() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute.*", "attribute.suffix", now, 1L, null);
    StreamElement element2 = _el("key", "attribute.*", "attribute.suffix2", now, 1L, "value");
    assertTrue(cache.put(element, false));
    assertTrue(cache.put(element2, false));
    assertNull(cache.get("key", "attribute.", now + 1));
  }

  @Test
  public void testMultiCacheScan() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "a.*", "a.1", now, 1L, "test1");
    StreamElement element2 = _el("key", "a.*", "a.2", now, 1L, "test2");
    StreamElement element3 = _el("key", "a.*", "a.3", now, 1L, "test3");
    StreamElement element4 = _el("key", "a.*", "a.2", now, 1L, null);
    assertTrue(cache.put(element, false));
    now += 1;
    assertTrue(cache.put(element2, false));
    assertTrue(cache.put(element3, false));
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
    assertTrue(cache.put(element4, false));
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
    StreamElement element = _el("key", "a.*", "a.1", now, 1L, "test1");
    assertTrue(cache.put(element, false));
    now += 1;
    StreamElement element2 = _el("key", "a.*", "a.2", now, 2L, "test2");
    assertTrue(cache.put(element2, false));
    now += 1;
    StreamElement element3 = _el("key", "a.*", "a.", now, 3L, null);
    // tombstone prefix delete
    assertTrue(cache.put(element3, false));
    now += 1;
    StreamElement element4 = _el("key", "a.*", "a.3", now, 4L, "test3");
    assertTrue(cache.put(element4, false));
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
    StreamElement element5 = _el("key", "a.*", "a.2", now, 1L, null);
    assertTrue(cache.put(element5, false));
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

  @Test
  public void testGetWithClearStaleRecords() {
    TimeBoundedVersionedCache cache = new TimeBoundedVersionedCache(entity, 60_000L);
    StreamElement element = _el("key", "attribute.suffix", now, 1L, "value1");
    StreamElement element2 = _el("key", "attribute.suffix", now + 1, 2L, "value2");
    assertTrue(cache.put(element, false));
    assertTrue(cache.put(element2, false));
    assertEquals(
        "value1",
        cache.get("key", "attribute.suffix", now).getSecond().getData().getParsed().get());
    cache.clearStaleRecords(now + 1);
    assertNull(cache.get("key", "attribute.suffix", now));
    assertEquals(
        "value2",
        cache.get("key", "attribute.suffix", now + 1).getSecond().getData().getParsed().get());
  }

  private StreamElement _el(String key, String attribute, long now, long seqId, String value) {

    return _el(key, attribute, attribute, now, seqId, value);
  }

  private StreamElement _el(
      String key, String name, String attribute, long now, long seqId, String value) {

    AttributeDescriptor<String> desc =
        AttributeDescriptor.newBuilder(repo)
            .setName(name)
            .setEntity("entity")
            .setSchemeUri(URI.create("string:///"))
            .build();
    EntityDescriptor entityDescriptor =
        EntityDescriptor.newBuilder().setName("entity").addAttribute(desc).build();
    if (value != null) {
      if (seqId > 0) {
        return StreamElement.upsert(
            entityDescriptor,
            desc,
            seqId,
            key,
            attribute,
            now,
            value.getBytes(StandardCharsets.UTF_8));
      }
      return StreamElement.upsert(
          entityDescriptor,
          desc,
          UUID.randomUUID().toString(),
          key,
          attribute,
          now,
          value.getBytes(StandardCharsets.UTF_8));
    }
    if (attribute.equals(desc.toAttributePrefix())
        || attribute.equals(desc.toAttributePrefix() + "*")) {
      if (seqId > 0) {
        return StreamElement.deleteWildcard(entityDescriptor, desc, seqId, key, now);
      }
      return StreamElement.deleteWildcard(
          entityDescriptor, desc, UUID.randomUUID().toString(), key, now);
    }
    if (seqId > 0) {
      return StreamElement.delete(entityDescriptor, desc, seqId, key, attribute, now);
    }
    return StreamElement.delete(
        entityDescriptor, desc, UUID.randomUUID().toString(), key, attribute, now);
  }
}
