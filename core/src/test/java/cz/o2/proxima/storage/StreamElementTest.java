/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.UUID;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test suite for {@link StreamElement}.
 */
public class StreamElementTest {

  final Repository repo = ConfigRepository.Builder
      .ofTest(ConfigFactory.empty())
      .build();
  final AttributeDescriptorBase<byte[]> attr;
  final AttributeDescriptorBase<byte[]> attrWildcard;
  final EntityDescriptor entity;

  {
    try {
      attr = AttributeDescriptor
            .newBuilder(repo)
            .setEntity("entity")
            .setName("attr")
            .setSchemeUri(new URI("bytes:///"))
            .build();

      attrWildcard = AttributeDescriptor
            .newBuilder(repo)
            .setEntity("entity")
            .setName("wildcard.*")
            .setSchemeUri(new URI("bytes:///"))
            .build();

      entity = EntityDescriptor.newBuilder()
            .setName("entity")
            .addAttribute(attr)
            .addAttribute(attrWildcard)
            .build();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }



  @Test
  public void testUpdate() {
    long now = System.currentTimeMillis();
    StreamElement update = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), now, new byte[] { 1, 2 });
    assertFalse(update.isDelete());
    assertFalse(update.isDeleteWildcard());
    assertEquals("key", update.getKey());
    assertEquals(attr.getName(), update.getAttribute());
    assertEquals(attr, update.getAttributeDescriptor());
    assertEquals(entity, update.getEntityDescriptor());
    assertArrayEquals(new byte[] { 1, 2 }, update.getValue());
    assertEquals(now, update.getStamp());
  }

  @Test
  public void testDelete() {
    long now = System.currentTimeMillis();
    StreamElement delete = StreamElement.delete(
        entity, attr, UUID.randomUUID().toString(),
        "key", attr.getName(), now);
    assertTrue(delete.isDelete());
    assertFalse(delete.isDeleteWildcard());
    assertEquals("key", delete.getKey());
    assertEquals(attr.getName(), delete.getAttribute());
    assertEquals(attr, delete.getAttributeDescriptor());
    assertEquals(entity, delete.getEntityDescriptor());
    assertNull(delete.getValue());
    assertEquals(now, delete.getStamp());
  }

  @Test
  public void testDeleteWildcard() {
    long now = System.currentTimeMillis();
    StreamElement delete = StreamElement.deleteWildcard(
        entity, attrWildcard, UUID.randomUUID().toString(),
        "key", now);
    assertTrue(delete.isDelete());
    assertTrue(delete.isDeleteWildcard());
    assertEquals("key", delete.getKey());
    assertEquals(attrWildcard.getName(), delete.getAttribute());
    assertEquals(attrWildcard, delete.getAttributeDescriptor());
    assertEquals(entity, delete.getEntityDescriptor());
    assertNull(delete.getValue());
    assertEquals(now, delete.getStamp());
  }

}
