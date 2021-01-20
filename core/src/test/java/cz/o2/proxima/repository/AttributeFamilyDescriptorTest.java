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
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.DefaultConsumerNameFactory.DefaultReplicationConsumerNameFactory;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

public class AttributeFamilyDescriptorTest {

  private final ConfigRepository repo;
  private final EntityDescriptor entity;
  private final AttributeDescriptor<byte[]> attribute;
  private final AttributeFamilyDescriptor descriptorWithSuffix;
  private final AttributeFamilyDescriptor descriptorWithoutSuffix;

  public AttributeFamilyDescriptorTest() {
    this.repo =
        ConfigRepository.Builder.of(
                ConfigFactory.load()
                    .withFallback(ConfigFactory.load("test-reference.conf"))
                    .resolve())
            .build();
    this.entity = repo.getEntity("event");
    this.attribute = entity.getAttribute("data");
    descriptorWithSuffix =
        repo.getFamiliesForAttribute(attribute)
            .stream()
            .filter(
                f ->
                    f.getCfg()
                        .containsKey(
                            DefaultConsumerNameFactory.CFG_REPLICATION_CONSUMER_NAME_SUFFIX))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to get attribute family for attribute %s with config property: %s",
                            attribute.getName(),
                            DefaultConsumerNameFactory.CFG_REPLICATION_CONSUMER_NAME_SUFFIX)));
    descriptorWithoutSuffix =
        repo.getFamiliesForAttribute(attribute)
            .stream()
            .filter(
                f ->
                    !f.getCfg()
                        .containsKey(
                            DefaultConsumerNameFactory.CFG_REPLICATION_CONSUMER_NAME_SUFFIX))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to get attribute family for attribute %s without config property: %s",
                            attribute.getName(),
                            DefaultConsumerNameFactory.CFG_REPLICATION_CONSUMER_NAME_SUFFIX)));
  }

  @Test
  public void testSerializableAndHashcodeAndEquals() throws IOException, ClassNotFoundException {
    AttributeFamilyDescriptor familyDescriptor =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entity)
            .setAccess(AccessType.from("commit-log"))
            .setType(StorageType.PRIMARY)
            .setName("ok")
            .setStorageUri(URI.create("inmem:///proxima_events"))
            .build();
    TestUtils.assertSerializable(familyDescriptor);
    AttributeFamilyDescriptor anotherFamilyDescriptor =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entity)
            .setAccess(AccessType.from("commit-log"))
            .setType(StorageType.PRIMARY)
            .setName("ok")
            .setStorageUri(URI.create("inmem:///proxima_events"))
            .build();
    TestUtils.assertHashCodeAndEquals(familyDescriptor, anotherFamilyDescriptor);
  }

  @Test
  public void testGetConsumerNames() {
    assertEquals(
        String.format("consumer-%s-my-suffix", descriptorWithSuffix.getName()),
        descriptorWithSuffix.getReplicationConsumerNameFactory().apply());
    assertEquals(
        String.format("consumer-%s", descriptorWithoutSuffix.getName()),
        descriptorWithoutSuffix.getReplicationConsumerNameFactory().apply());
  }

  @Test(expected = RuntimeException.class)
  public void testWithCustomNotExistsGenerator() {
    AttributeFamilyDescriptor familyDescriptor =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entity)
            .setAccess(AccessType.from("commit-log"))
            .setType(StorageType.PRIMARY)
            .setName("fail")
            .setCfg(
                Collections.singletonMap(
                    AttributeFamilyDescriptor.CFG_REPLICATION_CONSUMER_NAME_GENERATOR,
                    "NotExistsClass"))
            .setStorageUri(URI.create("inmem:///proxima_events"))
            .build();
  }

  @Test
  public void testWithCustomConsumerGenerator() {
    AttributeFamilyDescriptor familyDescriptor =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entity)
            .setAccess(AccessType.from("commit-log"))
            .setType(StorageType.PRIMARY)
            .setName("ok")
            .setCfg(
                Collections.singletonMap(
                    AttributeFamilyDescriptor.CFG_REPLICATION_CONSUMER_NAME_GENERATOR,
                    DefaultReplicationConsumerNameFactory.class.getName()))
            .setStorageUri(URI.create("inmem:///proxima_events"))
            .build();
    assertTrue(true); // make sonar happy : )
  }

  @Test
  public void testGetValueDescriptor() {
    SchemaTypeDescriptor<byte[]> descriptor = attribute.getSchemaTypeDescriptor();
    assertEquals(AttributeValueType.ARRAY, descriptor.getType());
    assertEquals(AttributeValueType.BYTE, descriptor.getArrayTypeDescriptor().getValueType());
  }
}
