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
package cz.o2.proxima.core.repository;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import cz.o2.proxima.core.storage.PassthroughFilter;
import cz.o2.proxima.core.transform.Transformation;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

public class TransformationDescriptorTest {

  @Test
  public void testDefaultNamingOfTransformationConsumers() {
    Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    Map<String, TransformationDescriptor> transformations = repo.getTransformations();
    assertFalse(transformations.isEmpty());
    transformations.forEach(
        (name, t) -> assertEquals("transformer-" + name, t.getConsumerNameFactory().apply()));
  }

  @Test
  public void testConfigurableNamingConvention() {
    Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    TransformationDescriptor desc =
        TransformationDescriptor.newBuilder()
            .setCfg(
                Map.of(DefaultConsumerNameFactory.CFG_TRANSFORMER_CONSUMER_NAME_PREFIX, "prefix-"))
            .setName("name")
            .addAttributes(repo.getEntity("gateway").getAttribute("status"))
            .setTransformation(mock(Transformation.class))
            .setFilter(new PassthroughFilter())
            .build();
    desc.getConsumerNameFactory().setup(desc);
    assertEquals("prefix-transformer-name", desc.getConsumerNameFactory().apply());
  }
}
