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
import cz.o2.proxima.repository.ConfigRepository.Builder;
import cz.o2.proxima.repository.Repository.Validate;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.ElementWiseProxyTransform.ProxySetupContext;
import cz.o2.proxima.transform.ElementWiseTransformation;
import cz.o2.proxima.transform.EventDataToDummy;
import cz.o2.proxima.transform.WriteProxy;
import cz.o2.proxima.util.DummyFilter;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Test;

/** Test repository config parsing. */
@Slf4j
public class ConfigRepositoryTest {

  private final ConfigRepository repo =
      ConfigRepository.Builder.of(
              ConfigFactory.load()
                  .withFallback(ConfigFactory.load("test-reference.conf"))
                  .resolve())
          .build();

  @After
  public void tearDown() {
    ConfigRepository.dropCached();
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

    // check that we can query all families
    repo.getAllFamilies()
        .forEach(family -> assertTrue(repo.findFamilyByName(family.getName()).isPresent()));
    assertFalse(
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("proxy-event-storage"))
            .findAny()
            .isPresent());
    assertTrue(repo.findFamilyByName("proxy-event-storage").isPresent());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFamily() {
    ConfigRepository.Builder.ofTest(
            ConfigFactory.load()
                .withFallback(ConfigFactory.load("test-reference.conf"))
                .withFallback(ConfigFactory.parseString("attributeFamilies.invalid.invalid = true"))
                .resolve())
        .build();
  }

  @Test
  public void testInvalidDisabledFamily() {
    ConfigRepository.Builder.ofTest(
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

  @Test
  public void testTestRepositorySerializable() throws Exception {
    Repository testRepo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    Repository clone = TestUtils.assertSerializable(testRepo);
    assertTrue(clone == testRepo);
  }

  @Test
  public void testRepositorySerializable() throws Exception {
    ConfigRepository clone = TestUtils.assertSerializable(repo);
    assertTrue(clone == repo);
  }

  @Test
  public void testConstructionSerializable() throws IOException, ClassNotFoundException {
    Repository repo = Repository.of(ConfigFactory.load("test-reference.conf").resolve());
    TestUtils.assertSerializable(repo);
  }

  @Test
  public void testBuilderSerializable() throws IOException, ClassNotFoundException {
    ConfigRepository repo = Builder.of(ConfigFactory.load("test-reference.conf").resolve()).build();
    TestUtils.assertSerializable(repo);
    repo = Builder.ofTest(ConfigFactory.load("test-reference.conf").resolve()).build();
    TestUtils.assertSerializable(repo);
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
    assertEquals("_d", attr.getWriteTransform().asElementWise().fromProxy("_d"));
    assertEquals("_d", attr.getWriteTransform().asElementWise().toProxy("_d"));
    assertEquals("_d", attr.getReadTransform().asElementWise().fromProxy("_d"));
    assertEquals("_d", attr.getReadTransform().asElementWise().toProxy("_d"));

    // attribute dummy.data should be proxy to _d
    attr = (AttributeProxyDescriptor<?>) dummy.getAttribute("data");
    assertEquals("data", attr.getWriteTransform().asElementWise().toProxy("_d"));
    assertEquals("data", attr.getReadTransform().asElementWise().toProxy("_d"));
    assertEquals("_d", attr.getWriteTransform().asElementWise().fromProxy("data"));
    assertEquals("_d", attr.getReadTransform().asElementWise().fromProxy("data"));
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
      checkThrows(() -> ConfigRepository.validateEntityName("test-with-dashes"));
      // must not throw
      ConfigRepository.validateEntityName("testOk");
      ConfigRepository.validateEntityName("_testOk");
      ConfigRepository.validateEntityName("test_with_underscores");
      Repository.of(ConfigFactory.load("test-reference-with-invalid-entities.conf").resolve());
      fail("Should have thrown exception");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "Entity [entity-with-dashes] contains invalid characters. Valid are patterns [a-zA-Z_][a-zA-Z0-9_]*.",
          ex.getCause().getMessage());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testMultipleInstancesThrowException() {
    Builder.of(ConfigFactory.load("test-reference.conf").resolve()).build();
    Builder.of(
            ConfigFactory.load("test-reference.conf")
                .withFallback(ConfigFactory.parseMap(Collections.singletonMap("key", "value")))
                .resolve())
        .build();
  }

  @Test
  public void testConfigUpdateAfterSerialization() throws IOException, ClassNotFoundException {
    Repository repo = Repository.of(ConfigFactory.load("test-reference.conf").resolve());
    byte[] serialized = TestUtils.serializeObject(repo);
    assertTrue(TestUtils.deserializeObject(serialized).equals(repo));
    RepositoryFactory.VersionedCaching.drop();
    Repository repo2 = Repository.of(ConfigFactory.empty());
    byte[] serialized2 = TestUtils.serializeObject(repo2);
    RepositoryFactory.VersionedCaching.drop();

    repo = TestUtils.deserializeObject(serialized);
    assertTrue(repo.findEntity("gateway").isPresent());
    repo2 = TestUtils.deserializeObject(serialized2);
    assertFalse(repo2.findEntity("gateway").isPresent());
    assertTrue(repo != repo2);
    assertTrue(TestUtils.deserializeObject(serialized) == repo2);
    RepositoryFactory.VersionedCaching.drop();
  }

  @Test
  public void testProxySetuped() {
    AttributeProxyDescriptor<Object> proxy =
        repo.getEntity("proxied").getAttribute("asymmetric..*").asProxy();
    WriteProxy transform = (WriteProxy) proxy.getWriteTransform();
    assertEquals("_e.", transform.getTarget());
  }

  @Test
  public void testProxySetupContext() {
    EntityDescriptor entity = repo.getEntity("proxied");
    AttributeDescriptor<Object> attribute = entity.getAttribute("raw.*");
    AttributeProxyDescriptor<Object> proxy = entity.getAttribute("event.*").asProxy();
    ProxySetupContext context = ConfigRepository.asProxySetupContext(proxy, attribute, true, false);
    assertTrue(context.isReadTransform());
    assertFalse(context.isWriteTransform());
    assertFalse(context.isSymmetric());
    assertEquals(attribute, context.getTargetAttribute());
    assertEquals(proxy, context.getProxyAttribute());
  }

  @Test
  public void testDisableValidations() {
    Repository r =
        Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve(), Validate.NONE);
    for (Validate v : Validate.values()) {
      if (v != Validate.NONE) {
        assertFalse(r.isShouldValidate(v));
      } else {
        assertTrue(r.isShouldValidate(v));
      }
    }
  }

  @Test
  public void testValidateAllHasAllFlags() {
    int flag = 0;
    for (Validate v : Validate.values()) {
      if (v != Validate.ALL) {
        flag |= v.getFlag();
      }
    }
    assertEquals(flag, Validate.ALL.getFlag());
  }

  @Test
  public void testAsFactoryCreatesSameRepository() {
    RepositoryFactory factory = repo.asFactory();
    Config newCfg = ConfigFactory.parseString("dummy = 1").withFallback(repo.getConfig());
    ConfigRepository.dropCached();
    ConfigRepository updated = ConfigRepository.Builder.of(newCfg).build();
    RepositoryFactory newFactory = updated.asFactory();
    assertTrue(factory instanceof RepositoryFactory.VersionedCaching);
    assertTrue(newFactory instanceof RepositoryFactory.VersionedCaching);
    RepositoryFactory.VersionedCaching.drop();

    Repository oldRepo = factory.apply();
    assertNotSame(repo, oldRepo);
    assertEquals(repo, oldRepo);
    assertSame(oldRepo, oldRepo.asFactory().apply());

    ConfigRepository newRepo = (ConfigRepository) newFactory.apply();
    assertNotSame(updated, newRepo);
    assertEquals(updated, newRepo);
    assertSame(newRepo.asFactory().apply(), newRepo);
    assertSame(factory.apply(), newRepo);
  }

  @Test
  public void testRepositoryDrop() {
    Repository cloned = repo.asFactory().apply();
    assertSame(cloned, repo);
    repo.drop();
    cloned = repo.asFactory().apply();
    assertNotSame(cloned, repo);
  }

  @Test
  public void testTestRepositoryDrop() {
    Repository first = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    Repository second = first.asFactory().apply();
    assertSame(second, first);
    first.drop();
    second = first.asFactory().apply();
    assertNotSame(second, first);
  }

  @Test
  public void testFindFamilyByName() {
    assertFalse(repo.findFamilyByName("not-found").isPresent());
    assertTrue(repo.findFamilyByName("event-storage-stream").isPresent());
    assertNotNull(repo.getFamilyByName("event-storage-stream"));
    checkThrows(() -> repo.getFamilyByName("not-found"), IllegalArgumentException.class);
  }

  private void checkThrows(Factory<?> factory) {
    checkThrows(factory, null);
  }

  private void checkThrows(Factory<?> factory, @Nullable Class<? extends Throwable> cls) {
    try {
      factory.apply();
      fail("Expression should have thrown exception");
    } catch (Exception err) {
      assertTrue(
          "Exception " + err.getClass() + " is not " + cls,
          cls == null || err.getClass().isAssignableFrom(cls));
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
            transform.getTransformation().asElementWiseTransform(),
            entity,
            fromAttr,
            entity.getAttribute(fromAttr, true)));
  }

  private static String collectSingleAttributeUpdate(
      ElementWiseTransformation transform,
      EntityDescriptor entity,
      String inputAttribute,
      AttributeDescriptor<?> inputDesc) {

    AtomicReference<StreamElement> element = new AtomicReference<>();
    assertEquals(
        1,
        transform.apply(
            StreamElement.upsert(
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
