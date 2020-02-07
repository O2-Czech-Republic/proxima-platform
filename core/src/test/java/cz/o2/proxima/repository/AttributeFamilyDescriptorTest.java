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
            .filter(f -> f.getCfg().containsKey(AttributeFamilyDescriptor.CFG_CONSUMER_NAME_SUFFIX))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to get attribute family for attribute %s with config property: %s",
                            attribute.getName(),
                            AttributeFamilyDescriptor.CFG_CONSUMER_NAME_SUFFIX)));
    descriptorWithoutSuffix =
        repo.getFamiliesForAttribute(attribute)
            .stream()
            .filter(
                f -> !f.getCfg().containsKey(AttributeFamilyDescriptor.CFG_CONSUMER_NAME_SUFFIX))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to get attribute family for attribute %s without config property: %s",
                            attribute.getName(),
                            AttributeFamilyDescriptor.CFG_CONSUMER_NAME_SUFFIX)));
  }

  @Test
  public void testGetConsumerNames() {
    assertEquals(
        String.format("%s-my-suffix", descriptorWithSuffix.getName()),
        descriptorWithSuffix.getConsumerNameGenerator().getName());
    assertEquals(
        descriptorWithoutSuffix.getName(),
        descriptorWithoutSuffix.getConsumerNameGenerator().getName());
  }
}
