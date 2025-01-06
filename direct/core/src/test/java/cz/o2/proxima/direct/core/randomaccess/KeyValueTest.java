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
package cz.o2.proxima.direct.core.randomaccess;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import org.junit.Test;

/** Test {@link KeyValue}. */
public class KeyValueTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = entity.getAttribute("status");

  @Test
  public void testCreate() {
    KeyValue<byte[]> kv =
        KeyValue.of(
            entity,
            status,
            "key",
            status.getName(),
            new RawOffset(""),
            new byte[] {1, 2},
            null,
            System.currentTimeMillis());

    assertEquals("key", kv.getKey());
    assertEquals(entity, kv.getEntityDescriptor());
    assertEquals(status, kv.getAttributeDescriptor());
    assertEquals(status.getName(), kv.getAttribute());
    assertNotNull(kv.toString());
    assertNotNull(kv.getParsedRequired());
  }

  @Test
  public void testCreateWithSeqId() {
    KeyValue<byte[]> kv =
        KeyValue.of(
            entity,
            status,
            1L,
            "key",
            status.getName(),
            new RawOffset(""),
            new byte[] {1, 2},
            null,
            System.currentTimeMillis());

    assertEquals("key", kv.getKey());
    assertEquals(entity, kv.getEntityDescriptor());
    assertEquals(status, kv.getAttributeDescriptor());
    assertEquals(status.getName(), kv.getAttribute());
    assertNotNull(kv.toString());
    assertNotNull(kv.getParsedRequired());
    assertTrue(kv.hasSequentialId());
    assertEquals(1L, kv.getSequentialId());
  }

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    KeyValue<byte[]> kv =
        KeyValue.of(
            entity,
            status,
            "key",
            status.getName(),
            new RawOffset(""),
            new byte[] {1, 2},
            null,
            System.currentTimeMillis());
    KeyValue<byte[]> kv2 = TestUtils.assertSerializable(kv);
    TestUtils.assertHashCodeAndEquals(kv, kv2);
  }
}
