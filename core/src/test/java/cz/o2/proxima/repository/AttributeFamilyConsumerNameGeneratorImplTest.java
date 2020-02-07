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

import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

public class AttributeFamilyConsumerNameGeneratorImplTest {

  private static final String SUFFIX = "my-suffix";
  private static final String ATTR_FAM_NAME = "family-name";
  private final AttributeFamilyDescriptor attrFamilyDesc;
  private final AttributeFamilyDescriptor attrFamilyWithSuffix;

  public AttributeFamilyConsumerNameGeneratorImplTest() {

    ConfigRepository repo =
        ConfigRepository.Builder.of(
                () ->
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
    AttributeFamilyDescriptor.Builder builder =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entityDescriptor)
            .setAccess(AccessType.from("commit-log"))
            .setType(StorageType.PRIMARY)
            .setName(ATTR_FAM_NAME)
            .setStorageUri(URI.create("inmem:///proxima_events"));

    this.attrFamilyDesc = builder.build();
    this.attrFamilyWithSuffix =
        builder
            .setCfg(
                Collections.singletonMap(
                    AttributeFamilyDescriptor.CFG_CONSUMER_NAME_SUFFIX, SUFFIX))
            .build();
  }

  @Test
  public void testSerializableAndEquals() throws IOException, ClassNotFoundException {
    AttributeFamilyConsumerNameGenerator generator =
        AttributeFamilyConsumerNameGeneratorImpl.of(attrFamilyDesc);
    TestUtils.assertSerializable(generator);
    AttributeFamilyConsumerNameGenerator generator1 =
        new AttributeFamilyConsumerNameGeneratorImpl(ATTR_FAM_NAME);
    TestUtils.assertHashCodeAndEquals(generator, generator1);
  }

  @Test
  public void testGetNameWithoutSuffix() {
    AttributeFamilyConsumerNameGenerator generator =
        AttributeFamilyConsumerNameGeneratorImpl.of(attrFamilyDesc);
    assertEquals(ATTR_FAM_NAME, generator.getName());
  }

  @Test
  public void testGetNameWithSuffix() {
    AttributeFamilyConsumerNameGenerator generator =
        AttributeFamilyConsumerNameGeneratorImpl.of(attrFamilyWithSuffix);
    assertEquals(String.format("%s-%s", ATTR_FAM_NAME, SUFFIX), generator.getName());
  }
}
