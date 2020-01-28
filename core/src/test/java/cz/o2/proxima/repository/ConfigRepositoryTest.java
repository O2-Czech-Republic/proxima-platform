/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;

import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.EventDataToDummy;
import cz.o2.proxima.transform.Transformation;
import cz.o2.proxima.util.DummyFilter;
import cz.o2.proxima.util.TestUtils;
import java.io.NotSerializableException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/** Test repository config parsing. */
@Slf4j
public class ConfigRepositoryTest {

  private final ConfigRepository repo;

  public ConfigRepositoryTest() {
    this.repo =
        ConfigRepository.Builder.of(
                ConfigFactory.load()
                    .withFallback(ConfigFactory.load("test-reference.conf"))
                    .resolve())
            .build();
  }

  @Test
  public void testConfigParsing() {
    assertTrue("Entity event should have been parsed", repo.findEntity("event").isPresent());
    assertTrue("Entity gateway should have been parsed", repo.findEntity("gateway").isPresent());

    EntityDescriptor event = repo.getEntity("event");
    assertEquals("event", event.getName());
    assertEquals("data", event.getAttribute("data").getName());
    assertEquals("bytes", event.getAttribute("data").getSchemeUri().getScheme());
    assertNotNull(event.getAttribute("data").getValueSerializer());

    EntityDescriptor gateway = repo.getEntity("gateway");
    assertEquals("gateway", gateway.getName());
    assertEquals("bytes:///", gateway.getAttribute("armed").getSchemeUri().toString());
    assertEquals("fail:whenever", gateway.getAttribute("fail").getSchemeUri().toString());
    assertEquals("bytes:///", gateway.getAttribute("bytes").getSchemeUri().toString());

    assertEquals(1, repo.getTransformations().size());
    TransformationDescriptor transform =
        Iterables.getOnlyElement(repo.getTransformations().values());
    assertEquals(PassthroughFilter.class, transform.getFilter().getClass());
    assertEquals(event, transform.getEntity());
    assertEquals(Arrays.asList(event.getAttribute("data")), transform.getAttributes());
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
    // make sonar happy :-)
    assertTrue(true);
  }

  @Test(expected = NotSerializableException.class)
  public void testRepositoryNotSerializable() throws Exception {
    ConfigRepository clone = (ConfigRepository) TestUtils.assertSerializable(repo);
    assertNotNull(clone.getConfig());
    assertTrue(clone.getConfig().isResolved());
  }

  @Test
  public void testEntityFromOtherEntity() {
    assertEquals(8, repo.getEntity("replica").getAllAttributes().size());
  }

  @Test
  public void testReplicationAttributesCreation() {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    EntityDescriptor gateway = repo.getEntity("gateway");
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

    // check that all produced families that have scheme `proxy` are
    // really proxies
    repo.getAllFamilies()
        .forEach(af -> assertTrue(!af.getStorageUri().getScheme().equals("proxy") || af.isProxy()));
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

    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<?> armed = gateway.getAttribute("armed");
    assertTrue(armed instanceof AttributeProxyDescriptor);
    assertEquals(
        "_gatewayReplication_write$armed",
        ((AttributeProxyDescriptor<?>) armed).getReadTarget().getName());
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

  @Test
  public void testReplicationTransformations() {
    repo.reloadConfig(
        true,
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-replication.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());

    final EntityDescriptor dummy = repo.getEntity("dummy");
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

    EntityDescriptor gateway = repo.getEntity("gateway");
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

    EntityDescriptor dummy = repo.getEntity("dummy");

    // attribute _d should be proxy to
    // _dummyReplicationMasterSlave_write$_d
    // and _dummyReplicationMasterSlave_replicated$_d
    AttributeDescriptor<Object> _d = dummy.getAttribute("_d", true);
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
    attr = (AttributeProxyDescriptor<?>) dummy.getAttribute("data");
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

    // check that all produced families that have scheme `proxy` are
    // really proxies
    repo.getAllFamilies()
        .forEach(af -> assertTrue(!af.getStorageUri().getScheme().equals("proxy") || af.isProxy()));
  }

  @Test
  public void testGetEntity() {
    assertNotNull(repo.getEntity("event"));
  }

  @Test
  public void testGetNonExistentEntity() {
    IllegalArgumentException exception = null;
    try {
      repo.getEntity("non-existent");
    } catch (IllegalArgumentException e) {
      exception = e;
    }
    assertNotNull(exception);
    assertEquals("Unable to find entity [non-existent].", exception.getMessage());
  }

  @Test
  public void testGetAttribute() {
    assertNotNull(repo.getEntity("event").getAttribute("data"));
  }

  @Test
  public void testGetNonExistentAttribute() {
    IllegalArgumentException exception = null;
    try {
      repo.getEntity("event").getAttribute("non-existent");
    } catch (IllegalArgumentException e) {
      exception = e;
    }
    assertNotNull(exception);
    assertEquals(
        "Unable to find attribute [non-existent] of entity [event].", exception.getMessage());
  }

  @Test
  public void testDisallowedEntityNames() {
    try {
      checkThrows(() -> ConfigRepository.validateEntityName("0test"));
      checkThrows(() -> ConfigRepository.validateEntityName("test_with_underscores"));
      checkThrows(() -> ConfigRepository.validateEntityName("test-with-dashes"));
      // must not throw
      ConfigRepository.validateEntityName("testOk");
      Repository.of(
          () -> ConfigFactory.load("test-reference-with-invalid-entities.conf").resolve());
      fail("Should have throws exception");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "Entity [entity-with-dashes] contains invalid characters. Valid are a-zA-Z0-9 and entity cannot start with number.",
          ex.getCause().getMessage());
    }
  }

  private void checkThrows(Factory<?> factory) {
    try {
      factory.apply();
      fail("Expression should have throws exception");
    } catch (Exception err) {
      // pass
    }
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
            transform.getTransformation(), entity, fromAttr, entity.getAttribute(fromAttr, true)));
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
}
