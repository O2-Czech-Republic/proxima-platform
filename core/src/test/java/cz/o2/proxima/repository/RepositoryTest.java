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
package cz.o2.proxima.repository;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.transform.EventDataToDummy;
import cz.o2.proxima.view.PartitionedCachedView;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test repository config parsing.
 */
public class RepositoryTest {

  final Repository repo = Repository.Builder.of(ConfigFactory.load().resolve()).build();

  @Test
  public void testConfigParsing() throws IOException {
    assertTrue("Entity event should have been parsed",
        repo.findEntity("event").isPresent());
    assertTrue("Entity gateway should have been parsed",
        repo.findEntity("gateway").isPresent());

    EntityDescriptor event = repo.findEntity("event").get();
    assertEquals("event", event.getName());
    assertEquals("data", event.findAttribute("data").get().getName());
    assertEquals("bytes", event.findAttribute("data").get().getSchemeURI().getScheme());
    assertNotNull(event.findAttribute("data").get().getValueSerializer());

    EntityDescriptor gateway = repo.findEntity("gateway").get();
    assertEquals("gateway", gateway.getName());
    assertEquals("bytes:///",
        gateway.findAttribute("armed").get().getSchemeURI().toString());
    assertEquals("fail:whenever",
        gateway.findAttribute("fail").get().getSchemeURI().toString());
    assertEquals("bytes:///",
        gateway.findAttribute("bytes").get().getSchemeURI().toString());

    assertEquals(1, repo.getTransformations().size());
    TransformationDescriptor transform = repo.getTransformations().values().iterator().next();
    assertEquals(PassthroughFilter.class, transform.getFilter().getClass());
    assertEquals(event, transform.getEntity());
    assertEquals(
        Arrays.asList(event.findAttribute("data").get()),
        transform.getAttributes());
    assertEquals(EventDataToDummy.class, transform.getTransformation().getClass());
  }

  @Test(timeout = 5000)
  public void testProxyWrite() throws UnsupportedEncodingException, InterruptedException {
    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<AttributeFamilyDescriptor> families = repo
        .getFamiliesForAttribute(target);
    Set<AttributeFamilyDescriptor> proxiedFamilies = repo
        .getFamiliesForAttribute(source);
    assertEquals(
        families.stream()
          .map(a -> "proxy::event.*::" + a.getName())
          .collect(Collectors.toList()),
        proxiedFamilies.stream()
          .map(a -> a.getName())
          .collect(Collectors.toList()));

    // verify that writing to attribute event.abc ends up as _e.abc
    CountDownLatch latch = new CountDownLatch(2);
    proxiedFamilies.iterator().next().getCommitLogReader().get().observe("dummy", new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, LogObserver.OffsetCommitter confirm) {
        assertEquals("test", new String(ingest.getValue()));
        assertEquals("event.abc", ingest.getAttribute());
        assertEquals(source, ingest.getAttributeDescriptor());
        latch.countDown();
        return false;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });

    source.getWriter().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.abc", System.currentTimeMillis(), "test".getBytes("UTF-8")),
        (s, exc) -> {
          latch.countDown();
        });

    latch.await();

    KeyValue<?> kv = families.iterator().next()
        .getRandomAccessReader().get().get("key", "_e.abc", target)
        .orElseGet(() -> {
          fail("Missing _e.abc stored");
          return null;
        });

    assertEquals("test", new String((byte[]) kv.getValue()));

  }

  @Test
  public void testProxyRandomGet() throws UnsupportedEncodingException, InterruptedException {
    Repository repo = Repository.Builder.of(ConfigFactory.load().resolve()).build();
    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<AttributeFamilyDescriptor> proxiedFamilies = repo
        .getFamiliesForAttribute(source);

    // verify that writing to attribute event.abc ends up as _e.abc
    source.getWriter().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.abc", System.currentTimeMillis(), "test".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    KeyValue<?> kv = proxiedFamilies.iterator().next()
        .getRandomAccessReader().get().get("key", "event.abc", target)
        .orElseGet(() -> {
          fail("Missing event.abc stored");
          return null;
        });

    assertEquals("test", new String((byte[]) kv.getValue()));
    assertEquals(source, kv.getAttrDescriptor());
    assertEquals("event.abc", kv.getAttribute());
    assertEquals("key", kv.getKey());
  }

  @Test
  public void testProxyScan() throws UnsupportedEncodingException, InterruptedException {
    Repository repo = Repository.Builder.of(ConfigFactory.load().resolve()).build();
    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<AttributeFamilyDescriptor> proxiedFamilies = repo
        .getFamiliesForAttribute(source);

    source.getWriter().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.abc", System.currentTimeMillis(), "test".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    source.getWriter().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.def", System.currentTimeMillis(), "test2".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });


    List<KeyValue<?>> kvs = new ArrayList<>();
    proxiedFamilies.iterator().next()
        .getRandomAccessReader().get().scanWildcard("key", source, kvs::add);

    assertEquals(2, kvs.size());
    assertEquals("test", new String((byte[]) kvs.get(0).getValue()));
    assertEquals(source, kvs.get(0).getAttrDescriptor());
    assertEquals("event.abc", kvs.get(0).getAttribute());
    assertEquals("key", kvs.get(0).getKey());

    assertEquals("test2", new String((byte[]) kvs.get(1).getValue()));
    assertEquals(source, kvs.get(1).getAttrDescriptor());
    assertEquals("event.def", kvs.get(1).getAttribute());
    assertEquals("key", kvs.get(1).getKey());
  }

  @Test
  public void testProxyCachedView() throws UnsupportedEncodingException {
    Repository repo = Repository.Builder.of(ConfigFactory.load().resolve()).build();
    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    PartitionedCachedView view = repo.getFamiliesForAttribute(source).stream()
        .filter(af -> af.getAccess().canCreatePartitionedCachedView())
        .findAny()
        .flatMap(af -> af.getPartitionedCachedView())
        .orElseThrow(() -> new IllegalStateException("Missing cached view for " + source));
    RandomAccessReader reader = repo.getFamiliesForAttribute(target).stream()
        .filter(af -> af.getAccess().canRandomRead())
        .findAny()
        .flatMap(af -> af.getRandomAccessReader())
        .orElseThrow(() -> new IllegalStateException("Missing random reader for " + target));
    view.assign(Arrays.asList(() -> 0));
    StreamElement update = StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.def", System.currentTimeMillis(), "test2".getBytes("UTF-8"));
    assertFalse(reader.get("key", target.toAttributePrefix() + "def", target).isPresent());
    view.write(update, (succ, exc) -> { });
    assertTrue(reader.get("key", target.toAttributePrefix() + "def", target).isPresent());
    assertTrue(view.get("key", source.toAttributePrefix() + "def", source).isPresent());
  }

}
