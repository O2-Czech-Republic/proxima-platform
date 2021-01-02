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
package cz.o2.proxima.beam.core;

import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test reading from proxy attribute. */
public class AttributeFamilyProxyDataDescriptorTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private DirectDataOperator direct;
  private BeamDataOperator beam;

  @Before
  public void setUp() {
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    beam = repo.getOrCreateOperator(BeamDataOperator.class);
  }

  @After
  public void tearDown() {
    direct.close();
    beam.close();
  }

  @Test
  public void testReadingFromProxy() {
    EntityDescriptor proxied = repo.getEntity("proxied");
    AttributeDescriptor<byte[]> event = proxied.getAttribute("event.*");
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalArgumentException("Missing writer for " + event))
        .write(newEvent(proxied, event), (succ, exc) -> {});
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> input = beam.getBatchSnapshot(p, event);
    PCollection<Long> result = input.apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    assertNotNull(p.run());
  }

  private StreamElement newEvent(EntityDescriptor proxied, AttributeDescriptor<byte[]> event) {
    return StreamElement.upsert(
        proxied,
        event,
        "uuid",
        "key",
        event.toAttributePrefix() + "1",
        System.currentTimeMillis(),
        new byte[] {});
  }
}
