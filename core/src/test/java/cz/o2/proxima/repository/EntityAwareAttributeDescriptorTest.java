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

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.time.Instant;
import org.junit.Test;

public class EntityAwareAttributeDescriptorTest {

  private final Repository repo =
      Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  private final Instant now = Instant.now();

  @Test
  public void testRegularAttributeDescriptor() {
    Regular<byte[]> regular = EntityAwareAttributeDescriptor.regular(getEvent(), getData());
    StreamElement element = regular.upsert("uuid", "key", now, new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.upsert(1L, "key", now.toEpochMilli(), new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.upsert("uuid", "key", now.toEpochMilli(), new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.upsert("key", now, new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.upsert("key", now.toEpochMilli(), new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.delete("uuid", "key", now);
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.delete(1L, "key", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.delete("uuid", "key", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.delete("key", now);
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = regular.delete("key", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("event", element.getEntityDescriptor().getName());
    assertEquals("data", element.getAttribute());
    assertEquals("data", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
  }

  @Test
  public void testWildcard() {
    Wildcard<byte[]> wildcard = EntityAwareAttributeDescriptor.wildcard(getGateway(), getDevice());
    StreamElement element = wildcard.upsert("uuid", "key", "1", now, new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.upsert(1L, "key", "1", now.toEpochMilli(), new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.upsert("uuid", "key", "1", now.toEpochMilli(), new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.upsert("key", "1", now, new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.upsert("key", "1", now.toEpochMilli(), new byte[] {});
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.delete("uuid", "key", "1", now);
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.delete("uuid", "key", "1", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.delete(1L, "key", "1", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.delete("key", "1", now);
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.delete("key", "1", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.delete("uuid", "key", "1", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.1", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.deleteWildcard("uuid", "key", now);
    assertTrue(element.isDelete());
    assertTrue(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.*", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.deleteWildcard(1L, "key", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertTrue(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.*", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.deleteWildcard("uuid", "key", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertTrue(element.isDeleteWildcard());
    assertEquals("uuid", element.getUuid());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.*", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.deleteWildcard("key", now);
    assertTrue(element.isDelete());
    assertTrue(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.*", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());
    element = wildcard.deleteWildcard("key", now.toEpochMilli());
    assertTrue(element.isDelete());
    assertTrue(element.isDeleteWildcard());
    assertEquals("gateway", element.getEntityDescriptor().getName());
    assertEquals("device.*", element.getAttribute());
    assertEquals("device.*", element.getAttributeDescriptor().getName());
    assertEquals("key", element.getKey());
    assertEquals(now.toEpochMilli(), element.getStamp());

    assertEquals("1", wildcard.extractSuffix("device.1"));
    assertEquals("", wildcard.extractSuffix("device."));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardNotRegular() {
    EntityAwareAttributeDescriptor.regular(getGateway(), getDevice());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegularNotWildcard() {
    EntityAwareAttributeDescriptor.wildcard(getEvent(), getData());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMimatchingEntities() {
    EntityAwareAttributeDescriptor.regular(getGateway(), getData());
  }

  @Test
  public void testSerializabilityAndEquality() throws IOException, ClassNotFoundException {
    TestUtils.assertHashCodeAndEquals(
        TestUtils.assertSerializable(EntityAwareAttributeDescriptor.regular(getEvent(), getData())),
        getData());
    TestUtils.assertHashCodeAndEquals(
        TestUtils.assertSerializable(
            EntityAwareAttributeDescriptor.wildcard(getGateway(), getDevice())),
        getDevice());
  }

  private AttributeDescriptor<byte[]> getData() {
    return getEvent().getAttribute("data");
  }

  private EntityDescriptor getEvent() {
    return repo.getEntity("event");
  }

  private EntityDescriptor getGateway() {
    return repo.getEntity("gateway");
  }

  private AttributeDescriptor<byte[]> getDevice() {
    return getGateway().getAttribute("device.*");
  }
}
