/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.util.UUID;
import org.junit.Test;

/** Test suite for {@link StreamElement}. */
public class StreamElementTest {

  private final Repository repo =
      Repository.of(() -> ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalArgumentException("Missing entity gateway"));
  private final AttributeDescriptor<?> armed =
      gateway
          .findAttribute("armed")
          .orElseThrow(
              () -> new IllegalArgumentException("Entity gateway is missing attribute armed"));
  private final AttributeDescriptor<?> device =
      gateway
          .findAttribute("device.*")
          .orElseThrow(
              () -> new IllegalArgumentException("Entity gateway is missing attribute device.*"));

  @Test
  public void testUpdateCreation() {
    long now = System.currentTimeMillis();
    StreamElement element =
        StreamElement.update(
            gateway,
            armed,
            UUID.randomUUID().toString(),
            "key",
            armed.getName(),
            now,
            new byte[] {1, 2});
    assertEquals(gateway, element.getEntityDescriptor());
    assertEquals(armed, element.getAttributeDescriptor());
    assertEquals(armed.getName(), element.getAttribute());
    assertEquals("key", element.getKey());
    assertEquals(now, element.getStamp());
    assertTrue(element.getParsed().isPresent());
    assertNotNull(element.dump());
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
  }

  @Test
  public void testWildcardUpdateCreation() {
    long now = System.currentTimeMillis();
    StreamElement element =
        StreamElement.update(
            gateway,
            device,
            UUID.randomUUID().toString(),
            "key",
            device.toAttributePrefix() + "1",
            now,
            new byte[] {1, 2});
    assertEquals(gateway, element.getEntityDescriptor());
    assertEquals(device, element.getAttributeDescriptor());
    assertEquals(device.toAttributePrefix() + "1", element.getAttribute());
    assertEquals("key", element.getKey());
    assertEquals(now, element.getStamp());
    assertTrue(element.getParsed().isPresent());
    assertNotNull(element.dump());
    assertFalse(element.isDelete());
    assertFalse(element.isDeleteWildcard());
  }

  @Test
  public void testDeleteCreation() {
    long now = System.currentTimeMillis();
    StreamElement element =
        StreamElement.delete(
            gateway, armed, UUID.randomUUID().toString(), "key", armed.getName(), now);
    assertEquals(gateway, element.getEntityDescriptor());
    assertEquals(armed, element.getAttributeDescriptor());
    assertEquals(armed.getName(), element.getAttribute());
    assertEquals("key", element.getKey());
    assertEquals(now, element.getStamp());
    assertFalse(element.getParsed().isPresent());
    assertNotNull(element.dump());
    assertTrue(element.isDelete());
    assertFalse(element.isDeleteWildcard());
  }

  @Test
  public void testWildcardDeleteCreation() {
    long now = System.currentTimeMillis();
    StreamElement element =
        StreamElement.deleteWildcard(gateway, device, UUID.randomUUID().toString(), "key", now);
    assertEquals(gateway, element.getEntityDescriptor());
    assertEquals(device, element.getAttributeDescriptor());
    assertEquals(device.getName(), element.getAttribute());
    assertEquals("key", element.getKey());
    assertEquals(now, element.getStamp());
    assertFalse(element.getParsed().isPresent());
    assertNotNull(element.dump());
    assertTrue(element.isDelete());
    assertTrue(element.isDeleteWildcard());
  }

  @Test
  public void testWildcardDeletePrefixCreation() {
    long now = System.currentTimeMillis();
    StreamElement element =
        StreamElement.deleteWildcard(
            gateway,
            device,
            UUID.randomUUID().toString(),
            "key",
            device.toAttributePrefix() + "1",
            now);
    assertEquals(gateway, element.getEntityDescriptor());
    assertEquals(device, element.getAttributeDescriptor());
    assertEquals(device.toAttributePrefix() + "1*", element.getAttribute());
    assertEquals("key", element.getKey());
    assertEquals(now, element.getStamp());
    assertFalse(element.getParsed().isPresent());
    assertNotNull(element.dump());
    assertTrue(element.isDelete());
    assertTrue(element.isDeleteWildcard());
  }
}
