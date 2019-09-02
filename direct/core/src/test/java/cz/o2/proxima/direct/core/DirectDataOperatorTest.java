/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core;

import static org.junit.Assert.*;

import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.AttributeProxyDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.TransformationDescriptor;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.EventDataToDummy;
import cz.o2.proxima.transform.Transformation;
import cz.o2.proxima.util.DummyFilter;
import cz.o2.proxima.util.TransformationRunner;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/** Test {@link DirectDataOperator}. */
@Slf4j
public class DirectDataOperatorTest {

  private final ConfigRepository repo;
  private final DirectDataOperator direct;

  public DirectDataOperatorTest() {
    this.repo =
        ConfigRepository.Builder.of(
                ConfigFactory.load()
                    .withFallback(ConfigFactory.load("test-reference.conf"))
                    .resolve())
            .build();
    this.direct = repo.asDataOperator(DirectDataOperator.class);
  }

  @Test
  public void testConfigParsing() throws IOException {
    assertTrue("Entity event should have been parsed", repo.findEntity("event").isPresent());
    assertTrue("Entity gateway should have been parsed", repo.findEntity("gateway").isPresent());

    EntityDescriptor event = repo.findEntity("event").get();
    assertEquals("event", event.getName());
    assertEquals("data", event.findAttribute("data").get().getName());
    assertEquals("bytes", event.findAttribute("data").get().getSchemeUri().getScheme());
    assertNotNull(event.findAttribute("data").get().getValueSerializer());

    EntityDescriptor gateway = repo.findEntity("gateway").get();
    assertEquals("gateway", gateway.getName());
    assertEquals("bytes:///", gateway.findAttribute("armed").get().getSchemeUri().toString());
    assertEquals("fail:whenever", gateway.findAttribute("fail").get().getSchemeUri().toString());
    assertEquals("bytes:///", gateway.findAttribute("bytes").get().getSchemeUri().toString());

    assertEquals(1, repo.getTransformations().size());
    TransformationDescriptor transform =
        Iterables.getOnlyElement(repo.getTransformations().values());
    assertEquals(PassthroughFilter.class, transform.getFilter().getClass());
    assertEquals(event, transform.getEntity());
    assertEquals(Arrays.asList(event.findAttribute("data").get()), transform.getAttributes());
    assertEquals(EventDataToDummy.class, transform.getTransformation().getClass());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFamily() {
    ConfigRepository.Builder.of(
            ConfigFactory.load()
                .withFallback(ConfigFactory.load("test-reference.conf"))
                .withFallback(ConfigFactory.parseString("attributeFamilies.invalid.invalid = true"))
                .resolve())
        .build();
  }

  @Test
  public void testInvalidDisabledFamily() {
    ConfigRepository.Builder.of(
            ConfigFactory.load()
                .withFallback(ConfigFactory.load("test-reference.conf"))
                .withFallback(
                    ConfigFactory.parseString(
                        "attributeFamilies.invalid.invalid = true\n"
                            + "attributeFamilies.invalid.disabled = true"))
                .resolve())
        .build();
  }

  @Test(timeout = 10000)
  public void testProxyWrite() throws UnsupportedEncodingException, InterruptedException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<DirectAttributeFamilyDescriptor> families = direct.getFamiliesForAttribute(target);

    Set<DirectAttributeFamilyDescriptor> proxiedFamilies = direct.getFamiliesForAttribute(source);

    assertEquals(
        families
            .stream()
            .map(a -> "proxy::" + a.getDesc().getName() + "::" + a.getDesc().getName())
            .collect(Collectors.toList()),
        proxiedFamilies.stream().map(a -> a.getDesc().getName()).collect(Collectors.toList()));

    // verify that writing to attribute event.abc ends up as _e.abc
    CountDownLatch latch = new CountDownLatch(2);
    proxiedFamilies
        .iterator()
        .next()
        .getCommitLogReader()
        .get()
        .observe(
            "dummy",
            new LogObserver() {

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
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

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.abc",
                System.currentTimeMillis(),
                "test".getBytes("UTF-8")),
            (s, exc) -> {
              latch.countDown();
            });

    latch.await();

    KeyValue<?> kv =
        families
            .iterator()
            .next()
            .getRandomAccessReader()
            .get()
            .get("key", "_e.raw-abc", target)
            .orElseGet(
                () -> {
                  fail("Missing _e.raw-abc stored");
                  return null;
                });

    assertEquals("test", new String((byte[]) kv.getValue()));
  }

  @Test
  public void testProxyRandomGet() throws UnsupportedEncodingException, InterruptedException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<DirectAttributeFamilyDescriptor> proxiedFamilies = direct.getFamiliesForAttribute(source);

    // verify that writing to attribute event.abc ends up as _e.abc
    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.abc",
                System.currentTimeMillis(),
                "test".getBytes("UTF-8")),
            (s, exc) -> {
              assertTrue(s);
            });

    KeyValue<?> kv =
        proxiedFamilies
            .iterator()
            .next()
            .getRandomAccessReader()
            .get()
            .get("key", "event.abc", source)
            .orElseGet(
                () -> {
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

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<DirectAttributeFamilyDescriptor> proxiedFamilies = direct.getFamiliesForAttribute(source);

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.abc",
                System.currentTimeMillis(),
                "test".getBytes("UTF-8")),
            (s, exc) -> {
              assertTrue(s);
            });

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.def",
                System.currentTimeMillis(),
                "test2".getBytes("UTF-8")),
            (s, exc) -> {
              assertTrue(s);
            });

    List<KeyValue<?>> kvs = new ArrayList<>();
    proxiedFamilies
        .iterator()
        .next()
        .getRandomAccessReader()
        .get()
        .scanWildcard("key", source, kvs::add);

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
  public void testProxyScanWithOffset() throws UnsupportedEncodingException, InterruptedException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    Set<DirectAttributeFamilyDescriptor> proxiedFamilies = direct.getFamiliesForAttribute(source);

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.abc",
                System.currentTimeMillis(),
                "test".getBytes("UTF-8")),
            (s, exc) -> {
              assertTrue(s);
            });

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.def",
                System.currentTimeMillis(),
                "test2".getBytes("UTF-8")),
            (s, exc) -> {
              assertTrue(s);
            });

    List<KeyValue<?>> kvs = new ArrayList<>();
    RandomAccessReader reader = proxiedFamilies.iterator().next().getRandomAccessReader().get();
    reader.scanWildcard(
        "key",
        source,
        reader.fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, "event.abc"),
        1,
        kvs::add);

    assertEquals(1, kvs.size());
    assertEquals("test2", new String((byte[]) kvs.get(0).getValue()));
    assertEquals(source, kvs.get(0).getAttrDescriptor());
    assertEquals("event.def", kvs.get(0).getAttribute());
    assertEquals("key", kvs.get(0).getKey());
  }

  @Test
  public void testProxyCachedView() throws UnsupportedEncodingException {
    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> target = proxied.findAttribute("_e.*", true).get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    CachedView view =
        direct
            .getFamiliesForAttribute(source)
            .stream()
            .filter(af -> af.getDesc().getAccess().canCreateCachedView())
            .findAny()
            .flatMap(af -> af.getCachedView())
            .orElseThrow(() -> new IllegalStateException("Missing cached view for " + source));
    RandomAccessReader reader =
        direct
            .getFamiliesForAttribute(target)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(af -> af.getRandomAccessReader())
            .orElseThrow(() -> new IllegalStateException("Missing random reader for " + target));
    view.assign(Arrays.asList(() -> 0));
    long now = System.currentTimeMillis();
    StreamElement update =
        StreamElement.update(
            proxied,
            source,
            UUID.randomUUID().toString(),
            "key",
            "event.def",
            now,
            "test2".getBytes("UTF-8"));
    assertFalse(reader.get("key", target.toAttributePrefix() + "def", target, now).isPresent());
    view.write(update, (succ, exc) -> {});
    assertTrue(reader.get("key", target.toAttributePrefix() + "raw-def", target, now).isPresent());
    assertTrue(view.get("key", source.toAttributePrefix() + "def", source, now).isPresent());
  }

  @Test
  public void testProxyObserve() throws InterruptedException, UnsupportedEncodingException {

    testProxyObserveWithAttributeName("event.abc");
  }

  @Test
  // this tests lookup in attributefamilyproxydescriptor
  // that automatically drops 'prefix$' from attribute name
  public void testProxyObserveBackwardCompatible()
      throws InterruptedException, UnsupportedEncodingException {

    testProxyObserveWithAttributeName("_ignored_$event.abc");
  }

  private void testProxyObserveWithAttributeName(String name)
      throws InterruptedException, UnsupportedEncodingException {
    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(source)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(af -> af.getCommitLogReader())
            .get();
    List<StreamElement> read = new ArrayList<>();
    reader
        .observe(
            "dummy",
            new LogObserver() {
              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                read.add(ingest);
                context.confirm();
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                name,
                System.currentTimeMillis(),
                "test".getBytes("UTF-8")),
            (s, exc) -> {
              assertTrue(s);
            });

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.def",
                System.currentTimeMillis(),
                "test2".getBytes("UTF-8")),
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
  public void testProxyObserveBulk() throws InterruptedException, UnsupportedEncodingException {

    EntityDescriptor proxied = repo.findEntity("proxied").get();
    AttributeDescriptor<?> source = proxied.findAttribute("event.*").get();
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(source)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(af -> af.getCommitLogReader())
            .get();
    List<StreamElement> read = new ArrayList<>();
    reader
        .observeBulk(
            "dummy",
            new LogObserver() {
              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                read.add(ingest);
                context.confirm();
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.abc",
                System.currentTimeMillis(),
                "test".getBytes("UTF-8")),
            (s, exc) -> {
              assertTrue(s);
            });

    direct
        .getWriter(source)
        .get()
        .write(
            StreamElement.update(
                proxied,
                source,
                UUID.randomUUID().toString(),
                "key",
                "event.def",
                System.currentTimeMillis(),
                "test2".getBytes("UTF-8")),
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
    assertEquals(8, repo.findEntity("replica").get().getAllAttributes().size());
  }

  @Test
  public void testReplicationAttributesCreation() {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    EntityDescriptor gateway =
        repo.findEntity("gateway").orElseThrow(() -> new AssertionError("Missing entity gateway"));
    // assert that we have created all necessary protected attributes
    assertTrue(gateway.findAttribute("_gatewayReplication_inmemFirst$status", true).isPresent());
    assertTrue(gateway.findAttribute("_gatewayReplication_inmemSecond$armed", true).isPresent());
    assertTrue(gateway.findAttribute("_gatewayReplication_read$status", true).isPresent());
    assertTrue(gateway.findAttribute("_gatewayReplication_write$device.*", true).isPresent());
    assertTrue(gateway.findAttribute("_gatewayReplication_replicated$rule.*", true).isPresent());
    assertTrue(gateway.findAttribute("_gatewayReplication_read$rule.*", true).isPresent());
    assertTrue(gateway.findAttribute("_gatewayReplication_read$rule.*", true).get().isWildcard());
    assertTrue(gateway.findAttribute("status").isPresent());
    assertTrue(gateway.findAttribute("status").get().isPublic());
  }

  @Test
  public void testReplicationWriteObserve() throws InterruptedException {
    Config config =
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve();
    repo.reloadConfig(true, config);
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));

    // start replications
    TransformationRunner.runTransformations(repo, direct);
    direct
        .getWriter(armed)
        .get()
        .write(
            StreamElement.update(
                gateway,
                armed,
                "uuid",
                "gw",
                armed.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            (succ, exc) -> {
              assertTrue(succ);
            });
    // wait till write propagates
    TimeUnit.MILLISECONDS.sleep(300);
    Optional<KeyValue<Object>> kv =
        direct
            .getFamiliesForAttribute(armed)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(af -> af.getRandomAccessReader())
            .orElseThrow(() -> new IllegalStateException("Missing random access reader for armed"))
            .get("gw", armed);
    assertTrue(kv.isPresent());
    assertEquals(armed, kv.get().getAttrDescriptor());
  }

  @Test
  public void testReplicationGloballyDisabled() throws InterruptedException {
    Config config =
        ConfigFactory.parseString("replications.disabled = true")
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve();
    repo.reloadConfig(true, config);
    // we have only single explicitly defined transformation left, others were
    // switched off
    assertEquals(1, repo.getTransformations().size());
    assertNotNull(repo.getTransformations().get("event-data-to-dummy-wildcard"));
  }

  @Test
  public void testReplicationGloballyReadOnly() throws InterruptedException {
    Config config =
        ConfigFactory.parseString("replications.read-only = true")
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve();
    repo.reloadConfig(true, config);
    // we have only single explicitly defined transformation left, others were
    // switched off
    assertEquals(1, repo.getTransformations().size());
    assertNotNull(repo.getTransformations().get("event-data-to-dummy-wildcard"));
  }

  @Test
  public void testReplicationGloballyReadLocal() throws InterruptedException {
    Config config =
        ConfigFactory.parseString("replications.read = local")
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve();
    repo.reloadConfig(true, config);
    // we have only single explicitly defined transformation left, others were
    // switched off
    assertEquals(1, repo.getTransformations().size());
    assertNotNull(repo.getTransformations().get("event-data-to-dummy-wildcard"));

    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<?> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));
    assertTrue(armed.isProxy());
    assertEquals("_gatewayReplication_write$armed", armed.asProxy().getReadTarget().getName());
  }

  @Test
  public void testReplicationWriteObserveReadLocal() throws InterruptedException {

    testReplicationWriteObserveInternal(
        ConfigFactory.load()
            .withFallback(
                ConfigFactory.parseString("replications.gateway-replication.read = local"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve(),
        true,
        true);
  }

  @Test
  public void testReplicationWriteObserveReadLocalWriteRemote() throws InterruptedException {

    testReplicationWriteObserveInternal(
        ConfigFactory.load()
            .withFallback(
                ConfigFactory.parseString("replications.gateway-replication.read = local"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve(),
        false,
        false);
  }

  @Test
  public void testReplicationTransformsHaveFilter() {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    TransformationDescriptor desc =
        repo.getTransformations().get("_dummyReplicationMasterSlave_slave");
    assertNotNull(desc);
    assertEquals(DummyFilter.class, desc.getFilter().getClass());
  }

  private void testReplicationWriteObserveInternal(
      Config config, boolean localWrite, boolean expectNonEmpty) throws InterruptedException {

    repo.reloadConfig(true, config);
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));

    AttributeDescriptor<Object> armedWrite =
        gateway
            .findAttribute(
                localWrite
                    ? "_gatewayReplication_write$armed"
                    : "_gatewayReplication_replicated$armed",
                true)
            .orElseThrow(() -> new IllegalStateException("Missing write attribute for armed"));

    // observe stream
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(armed)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(af -> af.getCommitLogReader())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader for armed"));

    List<StreamElement> observed = new ArrayList<>();
    reader.observe(
        "dummy",
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
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
    TransformationRunner.runTransformations(repo, direct);
    OnlineAttributeWriter writer = direct.getWriter(armedWrite).get();
    writer.write(
        StreamElement.update(
            gateway,
            armedWrite,
            "uuid",
            "gw",
            armedWrite.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2}),
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
  @Test(timeout = 10000)
  public void testReplicationWriteReadonlyObserve() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            // make the replication read-only
            .withFallback(
                ConfigFactory.parseString("replications.gateway-replication.read-only = true"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));

    TimeUnit.MILLISECONDS.sleep(300);
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(armed)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(af -> af.getCommitLogReader())
            .orElseThrow(() -> new IllegalStateException("Missing random access reader for armed"));
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        "dummy",
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals(ingest.getAttributeDescriptor(), armed);
            latch.countDown();
            context.confirm();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    OnlineAttributeWriter writer = direct.getWriter(armed).get();
    writer.write(
        StreamElement.update(
            gateway,
            armed,
            "uuid",
            "gw",
            armed.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
        });
    latch.await();
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 10000)
  public void testReplicationWriteReadonlyObserveLocal() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            // make the replication read-only
            .withFallback(
                ConfigFactory.parseString("replications.gateway-replication.read-only = true"))
            .withFallback(
                ConfigFactory.parseString("replications.gateway-replication.read = local"))
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf")));
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));

    TimeUnit.MILLISECONDS.sleep(300);
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(armed)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(af -> af.getCommitLogReader())
            .orElseThrow(() -> new IllegalStateException("Missing random access reader for armed"));
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        "dummy",
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals(ingest.getAttributeDescriptor(), armed);
            latch.countDown();
            context.confirm();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    OnlineAttributeWriter writer = direct.getWriter(armed).get();
    writer.write(
        StreamElement.update(
            gateway,
            armed,
            "uuid",
            "gw",
            armed.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
        });
    latch.await();
  }

  @Test(timeout = 10000)
  public void testWriteIntoReplicatedProxyAttribute() throws InterruptedException {

    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    EntityDescriptor dummy =
        repo.findEntity("dummy")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));
    AttributeDescriptor<Object> data =
        dummy
            .findAttribute("data", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute data"));

    AttributeDescriptor<Object> dataReplicated =
        dummy
            .findAttribute("_dummyReplicationProxiedSlave_replicated$_d", true)
            .orElseThrow(
                () -> new IllegalStateException("Missing write target for replicated data"));

    CountDownLatch latch = new CountDownLatch(2);
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(data)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(af -> af.getCommitLogReader())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader for data"));
    reader.observe(
        "dummy",
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals(ingest.getAttributeDescriptor(), data);
            latch.countDown();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    OnlineAttributeWriter writer = direct.getWriter(dataReplicated).get();
    writer.write(
        StreamElement.update(
            dummy,
            dataReplicated,
            "uuid",
            "gw",
            dataReplicated.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
  }

  @Test(timeout = 10000)
  public void testRandomReadFromReplicatedProxyAttribute() throws InterruptedException {

    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-proxy.conf"))
            .resolve());
    EntityDescriptor dummy =
        repo.findEntity("dummy")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));
    AttributeDescriptor<Object> data =
        dummy
            .findAttribute("data", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute data"));
    TransformationRunner.runTransformations(repo, direct);
    CountDownLatch latch = new CountDownLatch(2);
    runAttributeReplicas(tmp -> latch.countDown());
    OnlineAttributeWriter writer = direct.getWriter(data).get();
    writer.write(
        StreamElement.update(
            dummy,
            data,
            "uuid",
            "gw",
            data.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();

    Optional<RandomAccessReader> reader =
        direct
            .getFamiliesForAttribute(data)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(af -> af.getRandomAccessReader());
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", data).isPresent());
  }

  @Test(timeout = 10000)
  public void testRandomReadFromReplicatedProxyAttributeDirect() throws InterruptedException {

    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-proxy.conf"))
            .resolve());
    final EntityDescriptor dummy =
        repo.findEntity("dummy2")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy2"));
    final AttributeDescriptor<Object> event =
        dummy
            .findAttribute("event.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute event.*"));
    final AttributeDescriptor<Object> raw =
        dummy
            .findAttribute("_e.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute _e.*"));
    CountDownLatch latch = new CountDownLatch(2);
    runAttributeReplicas(tmp -> latch.countDown());
    TransformationRunner.runTransformations(repo, direct);
    OnlineAttributeWriter writer = direct.getWriter(event).get();
    writer.write(
        StreamElement.update(
            dummy,
            event,
            "uuid",
            "gw",
            event.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();

    Optional<RandomAccessReader> reader =
        direct
            .getFamiliesForAttribute(event)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(af -> af.getRandomAccessReader());
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", event.toAttributePrefix() + "1", event).isPresent());

    reader =
        direct
            .getFamiliesForAttribute(raw)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader);
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", raw.toAttributePrefix() + "2", event).isPresent());
    assertFalse(reader.get().get("gw", raw.toAttributePrefix() + "1", event).isPresent());
  }

  @Test(timeout = 10000)
  public void testApplicationOfProxyTransformOnIncomingData() throws InterruptedException {

    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-proxy.conf"))
            .resolve());
    final EntityDescriptor dummy =
        repo.findEntity("dummy2")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy2"));
    final AttributeDescriptor<Object> event =
        dummy
            .findAttribute("event.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute event.*"));
    final AttributeDescriptor<Object> eventSource =
        dummy
            .findAttribute("_dummy2Replication_read$event.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing source attribute for event.*"));
    final AttributeDescriptor<Object> raw =
        dummy
            .findAttribute("_e.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute _e.*"));
    TransformationRunner.runTransformations(repo, direct);
    CountDownLatch latch = new CountDownLatch(2);
    runAttributeReplicas(tmp -> latch.countDown());
    OnlineAttributeWriter writer = direct.getWriter(eventSource).get();
    writer.write(
        StreamElement.update(
            dummy,
            eventSource,
            "uuid",
            "gw",
            event.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();

    Optional<RandomAccessReader> reader =
        direct
            .getFamiliesForAttribute(event)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(af -> af.getRandomAccessReader());
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", event.toAttributePrefix() + "1", event).isPresent());

    reader =
        direct
            .getFamiliesForAttribute(raw)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader);
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", raw.toAttributePrefix() + "2", raw).isPresent());
  }

  @Test(timeout = 10000)
  public void testApplicationOfProxyTransformOnReplicatedData() throws InterruptedException {

    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-proxy.conf"))
            .resolve());
    final EntityDescriptor dummy =
        repo.findEntity("dummy2")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy2"));
    final AttributeDescriptor<Object> event =
        dummy
            .findAttribute("event.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute event.*"));
    final AttributeDescriptor<Object> raw =
        dummy
            .findAttribute("_e.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute _e.*"));
    TransformationRunner.runTransformations(repo, direct);
    CountDownLatch latch = new CountDownLatch(2);
    runAttributeReplicas(tmp -> latch.countDown());
    OnlineAttributeWriter writer = direct.getWriter(event).get();
    writer.write(
        StreamElement.update(
            dummy,
            event,
            "uuid",
            "gw",
            event.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();

    Optional<RandomAccessReader> reader =
        direct
            .getFamiliesForAttribute(event)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(af -> af.getRandomAccessReader());
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", event.toAttributePrefix() + "1", event).isPresent());

    reader =
        direct
            .getFamiliesForAttribute(raw)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader);
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", raw.toAttributePrefix() + "2", raw).isPresent());
  }

  @Test(timeout = 10000)
  public void testApplicationOfProxyTransformOnReplicatedDataWithTransform()
      throws InterruptedException {

    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-proxy.conf"))
            .resolve());
    final EntityDescriptor dummy =
        repo.findEntity("dummy2")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy2"));
    final EntityDescriptor event =
        repo.findEntity("event")
            .orElseThrow(() -> new IllegalStateException("Missing entity event"));

    final AttributeDescriptor<Object> data =
        event
            .findAttribute("data")
            .orElseThrow(() -> new IllegalStateException("Missing attribute data"));

    final AttributeDescriptor<Object> raw =
        dummy
            .findAttribute("_e.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute _e.*"));

    TransformationRunner.runTransformations(repo, direct);
    CountDownLatch latch = new CountDownLatch(2);
    runAttributeReplicas(tmp -> latch.countDown());
    OnlineAttributeWriter writer = direct.getWriter(data).get();
    long now = System.currentTimeMillis();
    writer.write(
        StreamElement.update(dummy, data, "uuid", "gw", data.getName(), now, new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();

    Optional<RandomAccessReader> reader =
        direct
            .getFamiliesForAttribute(raw)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader);
    assertTrue(reader.isPresent());
    assertTrue(reader.get().get("gw", raw.toAttributePrefix() + (now + 1), raw).isPresent());
    assertFalse(reader.get().get("gw", raw.toAttributePrefix() + now, raw).isPresent());
  }

  @Test(timeout = 10000)
  public void testReplicationFull() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-full.conf"))
            .resolve());
    EntityDescriptor first =
        repo.findEntity("first")
            .orElseThrow(() -> new IllegalStateException("Missing entity first"));
    EntityDescriptor second =
        repo.findEntity("second")
            .orElseThrow(() -> new IllegalStateException("Missing entity second"));

    testFullReplicationWithEntities(first, second);
    testFullReplicationWithEntities(second, first);
  }

  void testFullReplicationWithEntities(EntityDescriptor first, EntityDescriptor second)
      throws InterruptedException {

    final AttributeDescriptor<Object> wildcardFirst =
        first
            .findAttribute("wildcard.*")
            .orElseThrow(
                () -> new IllegalStateException("Missing attribute wildcard.* in entity " + first));
    final AttributeDescriptor<Object> wildcardSecond =
        second
            .findAttribute("wildcard.*")
            .orElseThrow(
                () ->
                    new IllegalStateException("Missing attribute wildcard.* in entity " + second));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
    runAttributeReplicas(tmp -> latch.get().countDown());
    TransformationRunner.runTransformations(repo, direct, tmp -> latch.get().countDown());
    long now = System.currentTimeMillis();
    direct
        .getWriter(wildcardFirst)
        .get()
        .write(
            StreamElement.update(
                first,
                wildcardFirst,
                "uuid",
                "key",
                wildcardFirst.toAttributePrefix() + "1",
                now,
                new byte[] {1, 2}),
            (succ, exc) -> assertTrue(succ));

    Optional<RandomAccessReader> reader =
        direct
            .getFamiliesForAttribute(wildcardSecond)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader);

    latch.getAndUpdate(old -> new CountDownLatch(1)).await();
    assertTrue(reader.isPresent());
    assertTrue(
        reader
            .get()
            .get("key", wildcardSecond.toAttributePrefix() + 1, wildcardSecond)
            .isPresent());

    direct
        .getWriter(wildcardSecond)
        .get()
        .write(
            StreamElement.deleteWildcard(
                first, wildcardSecond, "uuid", "key", wildcardSecond.toAttributePrefix(), now + 1),
            (succ, exc) -> {});

    reader =
        direct
            .getFamiliesForAttribute(wildcardFirst)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader);

    latch.get().await();
    assertTrue(reader.isPresent());
    assertFalse(
        reader
            .get()
            .get("key", wildcardFirst.toAttributePrefix() + 1, wildcardFirst, now + 1)
            .isPresent());
  }

  @Test(timeout = 10000)
  public void testObserveReplicatedWithProxy() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    final EntityDescriptor dummy =
        repo.findEntity("dummy")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));
    final AttributeDescriptor<Object> data =
        dummy
            .findAttribute("data")
            .orElseThrow(() -> new IllegalStateException("Missing attribute data"));
    final AttributeDescriptor<Object> dataRead =
        dummy
            .findAttribute("_dummyReplicationProxiedSlave_read$data", true)
            .orElseThrow(
                () -> new IllegalStateException("Missing read source for replicated data"));
    final AttributeDescriptor<Object> dataWrite =
        dummy
            .findAttribute("_dummyReplicationProxiedSlave_write$_d", true)
            .orElseThrow(
                () -> new IllegalStateException("Missing read source for replicated data"));

    TransformationRunner.runTransformations(repo, direct);
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(data)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
            .orElseThrow(
                () -> new IllegalStateException("Missing commit log reader for " + data.getName()));
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        "dummy",
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals(ingest.getAttributeDescriptor(), data);
            latch.countDown();
            context.confirm();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    OnlineAttributeWriter writer = direct.getWriter(dataRead).get();
    writer.write(
        StreamElement.update(
            dummy,
            data,
            "uuid",
            "gw",
            data.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
        });
    latch.await();
    assertFalse(
        direct
            .getFamiliesForAttribute(dataWrite)
            .stream()
            .filter(af -> af.getDesc().getType() == StorageType.PRIMARY)
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
            .orElseThrow(() -> new IllegalStateException("Missing random access for " + dataWrite))
            .get("gw", dataWrite)
            .isPresent());
  }

  @Test
  public void testReplicationTransformations() {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());

    final EntityDescriptor dummy =
        repo.findEntity("dummy")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));
    Map<String, TransformationDescriptor> transformations = repo.getTransformations();
    assertNotNull(transformations.get("_dummyReplicationMasterSlave_slave"));
    assertNotNull(transformations.get("_dummyReplicationMasterSlave_replicated"));
    assertNotNull(transformations.get("_dummyReplicationProxiedSlave_read"));

    // transformation from local writes to slave
    checkTransformation(
        dummy,
        transformations.get("_dummyReplicationMasterSlave_slave"),
        "wildcard.*",
        "_dummyReplicationMasterSlave_write$wildcard.*",
        "wildcard.*",
        "_dummyReplicationMasterSlave_slave$wildcard.*");

    // transformation from local writes to replicated result
    checkTransformation(
        dummy,
        transformations.get("_dummyReplicationMasterSlave_replicated"),
        "wildcard.*",
        "_dummyReplicationMasterSlave_write$wildcard.*",
        "wildcard.*",
        "_dummyReplicationMasterSlave_replicated$wildcard.*");

    // transformation from remote writes to local replicated result
    // with proxy
    checkTransformation(
        dummy,
        transformations.get("_dummyReplicationProxiedSlave_read"),
        "data",
        "data",
        "_d",
        "_dummyReplicationProxiedSlave_replicated$_d");
  }

  @Test
  public void testReplicationTransformationsNonProxied() {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());

    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    Map<String, TransformationDescriptor> transformations = repo.getTransformations();
    assertNotNull(transformations.get("_gatewayReplication_read"));
    assertNotNull(transformations.get("_gatewayReplication_inmemSecond"));

    // transformation from remote writes to local replicated result
    // without proxy
    checkTransformation(gateway, transformations.get("_gatewayReplication_read"), "armed", "armed");

    // transformation from local writes to slave
    checkTransformation(
        gateway,
        transformations.get("_gatewayReplication_inmemSecond"),
        "armed",
        "_gatewayReplication_write$armed",
        "armed",
        "_gatewayReplication_inmemSecond$armed");
  }

  @Test
  public void testReplicationProxies() {
    repo.reloadConfig(true, ConfigFactory.load("test-replication-proxy.conf").resolve());

    EntityDescriptor dummy =
        repo.findEntity("dummy")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));

    // attribute _d should be proxy to
    // _dummyReplicationMasterSlave_write$_d
    // and _dummyReplicationMasterSlave_replicated$_d
    AttributeDescriptor<Object> _d =
        dummy
            .findAttribute("_d", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute _d"));
    assertTrue(((AttributeDescriptorBase<?>) _d).isProxy());
    Set<AttributeFamilyDescriptor> families = repo.getFamiliesForAttribute(_d);
    assertEquals(1, families.size());
    AttributeFamilyDescriptor primary = Iterables.getOnlyElement(families);
    assertTrue("Family " + primary + " must be proxy", primary.isProxy());
    AttributeFamilyProxyDescriptor proxy = (AttributeFamilyProxyDescriptor) primary;
    assertEquals(
        "proxy::replication_dummy-replication-proxied-slave_replicated::"
            + "replication_dummy-replication-proxied-slave_write",
        primary.getName());
    assertEquals(
        "replication_dummy-replication-proxied-slave_replicated",
        proxy.getTargetFamilyRead().getName());
    assertEquals(
        "replication_dummy-replication-proxied-slave_write",
        proxy.getTargetFamilyWrite().getName());
    assertFalse(proxy.getTargetFamilyRead().isProxy());
    assertFalse(proxy.getTargetFamilyWrite().isProxy());
    assertEquals(1, proxy.getAttributes().size());
    AttributeProxyDescriptor<?> attr;
    attr = (AttributeProxyDescriptor<?>) _d;
    assertEquals("_d", attr.getWriteTransform().fromProxy("_d"));
    assertEquals("_d", attr.getWriteTransform().toProxy("_d"));
    assertEquals("_d", attr.getReadTransform().fromProxy("_d"));
    assertEquals("_d", attr.getReadTransform().toProxy("_d"));

    // attribute dummy.data should be proxy to _d
    attr = (AttributeProxyDescriptor<?>) dummy.findAttribute("data").get();
    assertEquals("data", attr.getWriteTransform().toProxy("_d"));
    assertEquals("data", attr.getReadTransform().toProxy("_d"));
    assertEquals("_d", attr.getWriteTransform().fromProxy("data"));
    assertEquals("_d", attr.getReadTransform().fromProxy("data"));
    families = repo.getFamiliesForAttribute(attr);
    assertEquals(2, families.size());
    primary =
        families.stream().filter(af -> af.getType() == StorageType.PRIMARY).findAny().orElse(null);
    assertNotNull(primary);
    assertTrue(primary.isProxy());
    proxy = (AttributeFamilyProxyDescriptor) primary;
    assertEquals(
        "proxy::proxy::replication_dummy-replication-proxied-slave_replicated"
            + "::replication_dummy-replication-proxied-slave_write::proxy"
            + "::replication_dummy-replication-proxied-slave_replicated"
            + "::replication_dummy-replication-proxied-slave_write",
        primary.getName());
    assertEquals(
        "proxy::replication_dummy-replication-proxied-slave_replicated::"
            + "replication_dummy-replication-proxied-slave_write",
        proxy.getTargetFamilyRead().getName());
    assertEquals(
        "proxy::replication_dummy-replication-proxied-slave_replicated::"
            + "replication_dummy-replication-proxied-slave_write",
        proxy.getTargetFamilyWrite().getName());
    assertTrue(proxy.getTargetFamilyRead().isProxy());
    assertTrue(proxy.getTargetFamilyWrite().isProxy());
    assertEquals(1, proxy.getAttributes().size());
  }

  @Test(timeout = 10000)
  public void testIncomingReplicationDoesntCycle() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    final EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    final AttributeDescriptor<Object> status =
        gateway
            .findAttribute("status")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));
    final AttributeDescriptor<Object> statusRead =
        gateway
            .findAttribute("_gatewayReplication_read$status", true)
            .orElseThrow(
                () -> new IllegalStateException("Missing read source for replicated status"));
    final AttributeDescriptor<Object> statusWrite =
        gateway
            .findAttribute("_gatewayReplication_write$status", true)
            .orElseThrow(
                () -> new IllegalStateException("Missing write target for replicated status"));

    TransformationRunner.runTransformations(repo, direct);
    CommitLogReader reader =
        direct
            .getFamiliesForAttribute(status)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing random access reader for " + status.getName()));
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        "dummy",
        new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            assertEquals(ingest.getAttributeDescriptor(), status);
            latch.countDown();
            context.confirm();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    OnlineAttributeWriter writer = direct.getWriter(statusRead).get();
    writer.write(
        StreamElement.update(
            gateway,
            status,
            "uuid",
            "gw",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
        });
    latch.await();
    RandomAccessReader localReader =
        direct
            .getFamiliesForAttribute(statusWrite)
            .stream()
            .filter(af -> af.getDesc().getType() == StorageType.PRIMARY)
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing primary random access family for status write"));
    assertFalse(localReader.get("gw", statusWrite).isPresent());
  }

  @Test(timeout = 10000)
  public void testWriteToSlaveOfProxyReplicatedAttribute() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-proxy.conf"))
            .resolve());
    EntityDescriptor dummy =
        repo.findEntity("dummy2")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy2"));
    AttributeDescriptor<Object> event =
        dummy
            .findAttribute("event.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute event.*"));

    CountDownLatch latch = new CountDownLatch(1);
    TransformationRunner.runTransformations(repo, direct);
    OnlineAttributeWriter writer = direct.getWriter(event).get();
    writer.write(
        StreamElement.update(
            dummy,
            event,
            "uuid",
            "gw",
            event.toAttributePrefix() + "123",
            System.currentTimeMillis(),
            new byte[] {1, 2}),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
  }

  @Test(timeout = 10000)
  public void testWildcardDeleteReplication() throws InterruptedException {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication-proxy.conf"))
            .resolve());
    EntityDescriptor dummy =
        repo.findEntity("dummy2")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy2"));
    AttributeDescriptor<Object> event =
        dummy
            .findAttribute("event.*", true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute event.*"));

    TransformationRunner.runTransformations(repo, direct);
    CountDownLatch latch = new CountDownLatch(1);
    runAttributeReplicas(tmp -> latch.countDown());
    OnlineAttributeWriter writer = direct.getWriter(event).get();

    DirectAttributeFamilyDescriptor outputFamily =
        direct
            .getAllFamilies()
            .filter(af -> af.getDesc().getName().equals("replication_dummy2-replication_source"))
            .findAny()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing family replication_dummy2-replication_source"));
    outputFamily
        .getWriter()
        .get()
        .online()
        .write(
            StreamElement.update(
                dummy,
                event,
                "uuid",
                "gw",
                "event.1",
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    latch.await();
    RandomAccessReader reader =
        direct
            .getFamiliesForAttribute(event)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
            .orElseThrow(() -> new IllegalStateException("Missing random access for event.*"));
    assertTrue(reader.get("gw", "event.1", event).isPresent());

    long now = System.currentTimeMillis();
    writer.write(
        StreamElement.deleteWildcard(dummy, event, "uuid", "gw", now),
        (succ, exc) -> {
          assertTrue(succ);
        });
    assertFalse(reader.get("gw", "event.1", event, now).isPresent());
  }

  @Test
  public void testGetCommitLog() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<Object> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));
    AttributeDescriptor<Object> status =
        gateway
            .findAttribute("status")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));

    assertTrue(direct.getCommitLogReader(armed, status).isPresent());
  }

  @Test
  public void testGetCachedView() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway  "));
    AttributeDescriptor<Object> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));

    assertTrue(direct.getCachedView(armed).isPresent());
  }

  // validate that given transformation transforms in the desired way
  private void checkTransformation(
      EntityDescriptor entity, TransformationDescriptor transform, String from, String to) {

    checkTransformation(entity, transform, from, from, to, to);
  }

  private void checkTransformation(
      EntityDescriptor entity,
      TransformationDescriptor transform,
      String fromAttr,
      String fromAttrDesc,
      String toAttr,
      String toAttrDesc) {

    Optional<AttributeDescriptor<Object>> f = entity.findAttribute(fromAttrDesc, true);
    assertTrue("Entity " + entity + " doesn't contain attribute " + fromAttrDesc, f.isPresent());
    assertTrue(
        "Entity " + entity + " doesn't contain attribute " + toAttrDesc,
        entity.findAttribute(toAttrDesc, true).isPresent());
    assertEquals(transform.getEntity(), entity);
    assertEquals(
        toAttr,
        collectSingleAttributeUpdate(
            transform.getTransformation(),
            entity,
            fromAttr,
            entity.findAttribute(fromAttr, true).get()));
  }

  private static String collectSingleAttributeUpdate(
      Transformation transform,
      EntityDescriptor entity,
      String inputAttribute,
      AttributeDescriptor<?> inputDesc) {

    AtomicReference<StreamElement> element = new AtomicReference<>();
    assertEquals(
        1,
        transform.apply(
            StreamElement.update(
                entity,
                inputDesc,
                UUID.randomUUID().toString(),
                "key",
                inputAttribute,
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            element::set));
    return element.get().getAttribute();
  }

  private void runAttributeReplicas(Consumer<StreamElement> onReplicated) {
    direct
        .getAllFamilies()
        .filter(af -> af.getDesc().getType() == StorageType.REPLICA)
        .filter(af -> !af.getDesc().getAccess().isReadonly() && !af.getDesc().isProxy())
        .forEach(
            af -> {
              List<AttributeDescriptor<?>> attributes = af.getAttributes();
              OnlineAttributeWriter writer =
                  af.getWriter()
                      .orElseThrow(
                          () -> new IllegalStateException("Missing writer of family " + af))
                      .online();
              attributes
                  .stream()
                  .map(
                      a ->
                          direct
                              .getFamiliesForAttribute(a)
                              .stream()
                              .filter(f -> f.getDesc().getType() == StorageType.PRIMARY)
                              .findAny()
                              .get())
                  .collect(Collectors.toSet())
                  .stream()
                  .findFirst()
                  .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
                  .get()
                  .observe(
                      af.getDesc().getName(),
                      new LogObserver() {
                        @Override
                        public boolean onNext(StreamElement ingest, OnNextContext context) {
                          log.debug("Replicating input {} to {}", ingest, writer);
                          writer.write(
                              ingest,
                              (succ, exc) -> {
                                context.commit(succ, exc);
                                onReplicated.accept(ingest);
                              });
                          return true;
                        }

                        @Override
                        public boolean onError(Throwable error) {
                          throw new RuntimeException(error);
                        }
                      });
              log.info("Started attribute replica {}", af.getDesc().getName());
            });
  }
}
