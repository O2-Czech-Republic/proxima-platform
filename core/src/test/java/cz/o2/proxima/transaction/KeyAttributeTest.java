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
package cz.o2.proxima.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import org.junit.Test;

public class KeyAttributeTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");

  @Test
  public void testKeyAttributeConstruction() {
    KeyAttribute ka = KeyAttribute.ofAttributeDescriptor(gateway, "gw", status, 1L);
    KeyAttribute ka2 = KeyAttribute.ofAttributeDescriptor(gateway, "gw", status, 1L);
    assertEquals(ka, ka2);
    StreamElement el =
        StreamElement.upsert(
            gateway,
            status,
            1L,
            "key",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {});
    ka = KeyAttribute.ofStreamElement(el);
    ka2 = KeyAttribute.ofStreamElement(el);
    assertEquals(ka, ka2);
    ka = KeyAttribute.ofAttributeDescriptor(gateway, "gw", device, 1L, "1");
    ka2 = KeyAttribute.ofAttributeDescriptor(gateway, "gw", device, 1L, "1");
    assertEquals(ka, ka2);
    try {
      KeyAttribute.ofAttributeDescriptor(gateway, "gw", device, 1L);
      fail("Should have thrown exception");
    } catch (IllegalArgumentException ex) {
      // pass
    }
  }
}
