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

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class KeyAttributeTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");

  @Test
  public void testKeyAttributeConstruction() {
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "gw", status, 1L);
    KeyAttribute ka2 = KeyAttributes.ofAttributeDescriptor(gateway, "gw", status, 1L);
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
    ka = KeyAttributes.ofStreamElement(el);
    ka2 = KeyAttributes.ofStreamElement(el);
    assertEquals(ka, ka2);
    ka = KeyAttributes.ofAttributeDescriptor(gateway, "gw", device, 1L, "1");
    ka2 = KeyAttributes.ofAttributeDescriptor(gateway, "gw", device, 1L, "1");
    assertEquals(ka, ka2);
    try {
      KeyAttributes.ofAttributeDescriptor(gateway, "gw", device, 1L);
      fail("Should have thrown exception");
    } catch (IllegalArgumentException ex) {
      // pass
    }
  }

  @Test
  public void testKeyAttributeDeleteConstruction() {
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "gw", status, 1L);
    KeyAttribute ka2 = KeyAttributes.ofAttributeDescriptor(gateway, "gw", status, 1L);
    assertEquals(ka, ka2);
    StreamElement el =
        StreamElement.delete(
            gateway, status, 1L, "key", status.getName(), System.currentTimeMillis());
    ka = KeyAttributes.ofStreamElement(el);
    ka2 = KeyAttributes.ofStreamElement(el);
    assertEquals(ka, ka2);
    ka = KeyAttributes.ofAttributeDescriptor(gateway, "gw", device, 1L, "1");
    ka2 = KeyAttributes.ofAttributeDescriptor(gateway, "gw", device, 1L, "1");
    assertEquals(ka, ka2);
    try {
      KeyAttributes.ofAttributeDescriptor(gateway, "gw", device, 1L);
      fail("Should have thrown exception");
    } catch (IllegalArgumentException ex) {
      // pass
    }
  }

  @Test
  public void testWildcardQueryElements() {
    Wildcard<byte[]> device = Wildcard.wildcard(gateway, this.device);
    StreamElement first =
        device.upsert(100L, "key", "1", System.currentTimeMillis(), new byte[] {});
    StreamElement second =
        device.upsert(101L, "key", "2", System.currentTimeMillis(), new byte[] {});
    List<KeyAttribute> wildcardQuery =
        KeyAttributes.ofWildcardQueryElements(gateway, "key", device, Arrays.asList(first, second));
    assertEquals(3, wildcardQuery.size());
    assertEquals(new KeyAttribute(gateway, "key", device, 100L, false, null), wildcardQuery.get(2));

    List<KeyAttribute> empty =
        KeyAttributes.ofWildcardQueryElements(gateway, "key", device, Collections.emptyList());
    assertEquals(1, empty.size());
    assertEquals(1L, empty.get(0).getSequenceId());
    assertTrue(empty.get(0).isWildcardQuery());
  }
}
