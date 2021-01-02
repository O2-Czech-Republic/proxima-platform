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
package cz.o2.proxima.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.DefaultConsumerNameFactory.DefaultReplicationConsumerNameFactory;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class DefaultConsumerNameFactoryTest {

  private static final String ATTR_FAM_NAME = "family-name";
  private final AttributeFamilyDescriptor attrFamilyDesc;
  private final AttributeFamilyDescriptor attrFamilyDescWithPrefixAndSuffix;
  private ConsumerNameFactory<AttributeFamilyDescriptor> replicationFactory;

  public DefaultConsumerNameFactoryTest() {

    ConfigRepository repo =
        ConfigRepository.Builder.of(
                ConfigFactory.load()
                    .withFallback(ConfigFactory.load("test-reference.conf"))
                    .resolve())
            .build();
    AttributeDescriptor<byte[]> attributeDescriptor =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("attribute")
            .setSchemeUri(URI.create("bytes:///"))
            .build();
    EntityDescriptor entityDescriptor =
        EntityDescriptor.newBuilder().setName("entity").addAttribute(attributeDescriptor).build();

    this.attrFamilyDesc =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entityDescriptor)
            .setAccess(AccessType.from("commit-log"))
            .setType(StorageType.PRIMARY)
            .setName(ATTR_FAM_NAME)
            .setStorageUri(URI.create("inmem:///proxima_events"))
            .build();
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(DefaultConsumerNameFactory.CFG_REPLICATION_CONSUMER_NAME_PREFIX, "rep-prefix-");
    cfg.put(DefaultConsumerNameFactory.CFG_REPLICATION_CONSUMER_NAME_SUFFIX, "-rep-suffix");
    cfg.put(DefaultConsumerNameFactory.CFG_TRANSFORMER_CONSUMER_NAME_PREFIX, "trans-prefix-");
    cfg.put(DefaultConsumerNameFactory.CFG_TRANSFORMER_CONSUMER_NAME_SUFFIX, "-trans-suffix");
    this.attrFamilyDescWithPrefixAndSuffix =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entityDescriptor)
            .setAccess(AccessType.from("commit-log"))
            .setType(StorageType.PRIMARY)
            .setName(ATTR_FAM_NAME)
            .setStorageUri(URI.create("inmem:///proxima_events"))
            .setCfg(Collections.unmodifiableMap(cfg))
            .build();
  }

  @Before
  public void setup() {
    this.replicationFactory = new DefaultReplicationConsumerNameFactory();
  }

  @Test
  public void testSerializableAndEquals() throws IOException, ClassNotFoundException {
    replicationFactory.setup(attrFamilyDesc);
    TestUtils.assertSerializable(replicationFactory);
    ConsumerNameFactory<AttributeFamilyDescriptor> factory1 =
        new DefaultReplicationConsumerNameFactory();
    factory1.setup(attrFamilyDesc);
    TestUtils.assertHashCodeAndEquals(replicationFactory, factory1);

    factory1.setup(attrFamilyDescWithPrefixAndSuffix);
    assertNotEquals(replicationFactory, factory1);
  }

  @Test
  public void testWithoutPrefixName() {
    replicationFactory.setup(attrFamilyDesc);
    assertEquals(
        String.format("consumer-%s", attrFamilyDesc.getName()), replicationFactory.apply());
  }

  @Test
  public void testWithPrefixName() {
    replicationFactory.setup(attrFamilyDesc);
    assertEquals(
        String.format("consumer-%s", attrFamilyDesc.getName()), replicationFactory.apply());
  }

  @Test
  public void testConfiguredFamily() {
    replicationFactory.setup(attrFamilyDescWithPrefixAndSuffix);
    assertEquals(
        String.format("%sconsumer-%s%s", "rep-prefix-", attrFamilyDesc.getName(), "-rep-suffix"),
        replicationFactory.apply());
  }
}
