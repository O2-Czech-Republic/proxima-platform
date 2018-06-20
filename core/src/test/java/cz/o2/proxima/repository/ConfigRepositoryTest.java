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

import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.transform.EventDataToDummy;
import cz.o2.proxima.view.PartitionedCachedView;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test repository config parsing.
 */
public class ConfigRepositoryTest {

  private final ConfigRepository repo;

  public ConfigRepositoryTest() {
    this.repo = ConfigRepository.Builder.of(
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve()).build();
  }

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
    assertEquals("bytes:byte[]",
        gateway.findAttribute("armed").get().getSchemeURI().toString());
    assertEquals("fail:whenever",
        gateway.findAttribute("fail").get().getSchemeURI().toString());
    assertEquals("bytes:byte[]",
        gateway.findAttribute("bytes").get().getSchemeURI().toString());

    assertEquals(1, repo.getTransformations().size());
    TransformationDescriptor transform = Iterables.getFirst(
        repo.getTransformations().values(), null);
    assertEquals(PassthroughFilter.class, transform.getFilter().getClass());
    assertEquals(event, transform.getEntity());
    assertEquals(
        Arrays.asList(event.findAttribute("data").get()),
        transform.getAttributes());
    assertEquals(EventDataToDummy.class, transform.getTransformation().getClass());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFamily() {
    ConfigRepository.Builder.of(
      ConfigFactory.load()
          .withFallback(ConfigFactory.load("test-reference.conf"))
          .withFallback(ConfigFactory.parseString(
              "attributeFamilies.invalid.invalid = true"))
          .resolve()).build();
  }

  @Test
  public void testInvalidDisabledFamily() {
    ConfigRepository.Builder.of(
      ConfigFactory.load()
          .withFallback(ConfigFactory.load("test-reference.conf"))
          .withFallback(ConfigFactory.parseString(
              "attributeFamilies.invalid.invalid = true\n"
            + "attributeFamilies.invalid.disabled = true"))
          .resolve()).build();
  }

  @Test(timeout = 5000)
  public void testProxyWrite()
      throws UnsupportedEncodingException, InterruptedException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<AttributeFamilyDescriptor> families = repo
        .getFamiliesForAttribute(target);
    Set<AttributeFamilyDescriptor> proxiedFamilies = repo
        .getFamiliesForAttribute(source);
    assertEquals(
        families.stream()
          .map(a -> "proxy::" + a.getName() + "::" + a.getName())
          .collect(Collectors.toList()),
        proxiedFamilies.stream()
          .map(a -> a.getName())
          .collect(Collectors.toList()));

    // verify that writing to attribute event.abc ends up as _e.abc
    CountDownLatch latch = new CountDownLatch(2);
    proxiedFamilies.iterator().next()
        .getCommitLogReader().get()
        .observe("dummy", new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter confirm) {
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

    repo.getWriter(source).get().write(StreamElement.update(
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
  public void testProxyRandomGet()
      throws UnsupportedEncodingException, InterruptedException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<AttributeFamilyDescriptor> proxiedFamilies = repo
        .getFamiliesForAttribute(source);

    // verify that writing to attribute event.abc ends up as _e.abc
    repo.getWriter(source).get().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.abc", System.currentTimeMillis(), "test".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    KeyValue<?> kv = proxiedFamilies.iterator().next()
        .getRandomAccessReader().get().get("key", "event.abc", source)
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
  public void testProxyScan()
      throws UnsupportedEncodingException, InterruptedException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<AttributeFamilyDescriptor> proxiedFamilies = repo
        .getFamiliesForAttribute(source);

    repo.getWriter(source).get().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.abc", System.currentTimeMillis(), "test".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    repo.getWriter(source).get().write(StreamElement.update(
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
    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    PartitionedCachedView view = repo.getFamiliesForAttribute(source).stream()
        .filter(af -> af.getAccess().canCreatePartitionedCachedView())
        .findAny()
        .flatMap(af -> af.getPartitionedCachedView())
        .orElseThrow(() -> new IllegalStateException(
            "Missing cached view for " + source));
    RandomAccessReader reader = repo.getFamiliesForAttribute(target).stream()
        .filter(af -> af.getAccess().canRandomRead())
        .findAny()
        .flatMap(af -> af.getRandomAccessReader())
        .orElseThrow(() -> new IllegalStateException(
            "Missing random reader for " + target));
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

  @Test
  public void testProxyObserve()
      throws InterruptedException, UnsupportedEncodingException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    CommitLogReader reader = repo.getFamiliesForAttribute(source)
        .stream()
        .filter(af -> af.getAccess().canReadCommitLog())
        .findAny()
        .flatMap(af -> af.getCommitLogReader())
        .get();
    List<StreamElement> read = new ArrayList<>();
    reader.observe("dummy", new LogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        read.add(ingest);
        committer.confirm();
        return true;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    }).waitUntilReady();

    repo.getWriter(source).get().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.abc", System.currentTimeMillis(), "test".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    repo.getWriter(source).get().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.def", System.currentTimeMillis(), "test2".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    assertEquals(2, read.size());
    assertEquals("test", new String((byte[]) read.get(0).getValue()));
    assertEquals(source, read.get(0).getAttributeDescriptor());
    assertEquals("event.abc", read.get(0).getAttribute());
    assertEquals("key", read.get(0).getKey());

    assertEquals("test2", new String((byte[]) read.get(1).getValue()));
    assertEquals(source, read.get(1).getAttributeDescriptor());
    assertEquals("event.def", read.get(1).getAttribute());
    assertEquals("key", read.get(1).getKey());
  }

  @Test
  public void testProxyObserveBulk()
      throws InterruptedException, UnsupportedEncodingException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    CommitLogReader reader = repo.getFamiliesForAttribute(source)
        .stream()
        .filter(af -> af.getAccess().canReadCommitLog())
        .findAny()
        .flatMap(af -> af.getCommitLogReader())
        .get();
    List<StreamElement> read = new ArrayList<>();
    reader.observeBulk("dummy", new BulkLogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        read.add(ingest);
        committer.confirm();
        return true;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    }).waitUntilReady();

    repo.getWriter(source).get().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.abc", System.currentTimeMillis(), "test".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    repo.getWriter(source).get().write(StreamElement.update(
        proxied,
        source, UUID.randomUUID().toString(),
        "key", "event.def", System.currentTimeMillis(), "test2".getBytes("UTF-8")),
        (s, exc) -> {
          assertTrue(s);
        });

    assertEquals(2, read.size());
    assertEquals("test", new String((byte[]) read.get(0).getValue()));
    assertEquals(source, read.get(0).getAttributeDescriptor());
    assertEquals("event.abc", read.get(0).getAttribute());
    assertEquals("key", read.get(0).getKey());

    assertEquals("test2", new String((byte[]) read.get(1).getValue()));
    assertEquals(source, read.get(1).getAttributeDescriptor());
    assertEquals("event.def", read.get(1).getAttribute());
    assertEquals("key", read.get(1).getKey());
  }


  @Test
  public void testRepositorySerializable() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(repo);
    oos.flush();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    // must not throw
    ConfigRepository clone = (ConfigRepository) ois.readObject();
    assertNotNull(clone.getConfig());
  }

  @Test
  public void testEntityFromOtherEntity() {
    assertTrue(repo.findEntity("replica").isPresent());
    assertEquals(7, repo.findEntity("replica").get().getAllAttributes().size());
  }

  @Test
  public void testReplicationAttributesCreation() {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .resolve());
    EntityDescriptor gateway = repo.findEntity("gateway").orElseThrow(
        () -> new AssertionError("Missing entity gateway"));
    // assert that we have created all necessary protected attributes
    assertTrue(gateway.findAttribute(
        "_gatewayReplication_inmemFirst$status", true).isPresent());
    assertTrue(gateway.findAttribute(
        "_gatewayReplication_inmemSecond$armed", true).isPresent());
    assertTrue(gateway.findAttribute(
        "_gatewayReplication_read$status", true).isPresent());
    assertTrue(gateway.findAttribute(
        "_gatewayReplication_write$device.*", true).isPresent());
    assertTrue(gateway.findAttribute(
        "_gatewayReplication_replicated$rule.*", true).isPresent());
    assertTrue(gateway.findAttribute(
        "_gatewayReplication_read$rule.*", true).isPresent());
    assertTrue(gateway.findAttribute(
        "_gatewayReplication_read$rule.*", true).get().isWildcard());
    assertTrue(gateway.findAttribute("status").isPresent());
    assertTrue(gateway.findAttribute("status").get().isPublic());
  }

  @Test
  public void testReplicationWriteObserve() throws InterruptedException {
    Config config = ConfigFactory.load()
        .withFallback(ConfigFactory.load("test-reference.conf"))
        .withFallback(ConfigFactory.load("test-replication.conf"))
        .resolve();
    repo.reloadConfig(true, config);
    EntityDescriptor gateway = repo
        .findEntity("gateway")
        .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed = gateway
        .findAttribute("armed")
        .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));

    // start replications
    repo.getTransformations().forEach(this::runTransformation);
    OnlineAttributeWriter writer = repo.getWriter(armed).get();
    writer.write(
        StreamElement.update(
            gateway, armed, "uuid", "gw", armed.getName(),
            System.currentTimeMillis(), new byte[] { 1, 2 }),
        (succ, exc) -> {
          assertTrue(succ);
        });
    // wait till write propagates
    TimeUnit.MILLISECONDS.sleep(300);
    RandomAccessReader reader = repo.getFamiliesForAttribute(armed)
        .stream()
        .filter(af -> af.getAccess().canRandomRead())
        .findAny()
        .flatMap(af -> af.getRandomAccessReader())
        .orElseThrow(() -> new IllegalStateException(
            "Missing random access reader for armed"));
    Optional<KeyValue<Object>> kv = reader.get("gw", armed);
    assertTrue(kv.isPresent());
    assertEquals(armed, kv.get().getAttrDescriptor());
  }

  @Test
  public void testReplicationWriteObserveReadLocal()
      throws InterruptedException {

    testReplicationWriteObserveInternal(
        ConfigFactory.load()
            .withFallback(ConfigFactory.parseString(
                "replications.gateway-replication.read = local"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .resolve(),
        true, true);
  }

  @Test
  public void testReplicationWriteObserveReadLocalWriteRemote()
      throws InterruptedException {

    testReplicationWriteObserveInternal(
        ConfigFactory.load()
            .withFallback(ConfigFactory.parseString(
                "replications.gateway-replication.read = local"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .resolve(),
        false, false);
  }

  private void testReplicationWriteObserveInternal(
      Config config,
      boolean localWrite,
      boolean expectNonEmpty) throws InterruptedException {

    repo.reloadConfig(true, config);
    EntityDescriptor gateway = repo
        .findEntity("gateway")
        .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed = gateway
        .findAttribute("armed")
        .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));
    AttributeDescriptor<Object> armedWrite = gateway
        .findAttribute(localWrite
            ? "_gatewayReplication_write$armed"
            : "_gatewayReplication_replicated$armed", true)
        .orElseThrow(() -> new IllegalStateException(
            "Missing write attribute for armed"));

    // observe stream
    CommitLogReader reader = repo.getFamiliesForAttribute(armed)
        .stream()
        .filter(af -> af.getAccess().canReadCommitLog())
        .findAny()
        .flatMap(af -> af.getCommitLogReader())
        .orElseThrow(() -> new IllegalStateException(
            "Missing commit log reader for armed"));
    List<StreamElement> observed = new ArrayList<>();
    reader.observe("dummy", new LogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        if (!expectNonEmpty) {
          fail("No input was expected.");
        }
        observed.add(ingest);
        return true;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    });

    // start replications
    repo.getTransformations().forEach(this::runTransformation);
    OnlineAttributeWriter writer = repo.getWriter(armedWrite).get();
    writer.write(
        StreamElement.update(
            gateway, armedWrite, "uuid", "gw", armedWrite.getName(),
            System.currentTimeMillis(), new byte[] { 1, 2 }),
        (succ, exc) -> {
          assertTrue(succ);
        });
    // wait till write propagates
    TimeUnit.MILLISECONDS.sleep(300);
    if (expectNonEmpty) {
      assertEquals(1, observed.size());
      assertEquals(armed, observed.get(0).getAttributeDescriptor());
    }
  }

  @SuppressWarnings("unchecked")
  private void testReplicationWriteObserveInternal(
      Config config) throws InterruptedException {

  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 2000)
  public void testReplicationWriteReadonlyObserve() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            // make the replication read-only
            .withFallback(ConfigFactory.parseString(
                "replications.gateway-replication.read-only = true"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .resolve());
    EntityDescriptor gateway = repo
        .findEntity("gateway")
        .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed = gateway
        .findAttribute("armed")
        .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));

    TimeUnit.MILLISECONDS.sleep(300);
    CommitLogReader reader = repo.getFamiliesForAttribute(armed)
        .stream()
        .filter(af -> af.getAccess().canReadCommitLog())
        .findAny()
        .flatMap(af -> af.getCommitLogReader())
        .orElseThrow(() -> new IllegalStateException(
            "Missing random access reader for armed"));
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe("dummy", new LogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        assertEquals(ingest.getAttributeDescriptor(), armed);
        latch.countDown();
        committer.confirm();
        return true;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    });
    OnlineAttributeWriter writer = repo.getWriter(armed).get();
    writer.write(
        StreamElement.update(
            gateway, armed, "uuid", "gw", armed.getName(),
            System.currentTimeMillis(), new byte[] { 1, 2 }),
        (succ, exc) -> {
          assertTrue(succ);
        });
    latch.await();
  }

  private void runTransformation(String name, TransformationDescriptor desc) {
    desc.getAttributes().stream()
        .flatMap(attr -> repo.getFamiliesForAttribute(attr)
            .stream()
            .filter(af -> af.getAccess().canReadCommitLog()))
        .collect(Collectors.toSet())
        .stream()
        .findAny()
        .get()
        .getCommitLogReader()
        .get()
        .observe(name, new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
            desc.getTransformation().apply(ingest, transformed -> {
              repo.getWriter(transformed.getAttributeDescriptor())
                  .get()
                  .write(transformed, committer::commit);
            });
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }

        });

  }

}
