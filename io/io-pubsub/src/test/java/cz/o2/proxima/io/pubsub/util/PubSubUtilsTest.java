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
package cz.o2.proxima.io.pubsub.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.io.pubsub.proto.PubSub.KeyValue;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PubSubUtilsTest {

  private final Repository repository = Repository.of(ConfigFactory.empty());
  private final AttributeDescriptor<byte[]> attribute =
      AttributeDescriptor.newBuilder(repository)
          .setName("attribute")
          .setEntity("entity")
          .setSchemeUri(URI.create("bytes:///"))
          .build();
  private final AttributeDescriptor<byte[]> wildcard =
      AttributeDescriptor.newBuilder(repository)
          .setName("wildcard.*")
          .setEntity("entity")
          .setSchemeUri(URI.create("bytes:///"))
          .build();
  private final EntityDescriptor entity =
      EntityDescriptor.newBuilder()
          .setName("entity")
          .addAttribute(attribute)
          .addAttribute(wildcard)
          .build();

  @Test
  void toStreamElementTest_correct_upsert() {
    final ByteString payload = ByteString.copyFrom("DATA".getBytes());
    KeyValue kv =
        KeyValue.newBuilder()
            .setAttribute(attribute.getName())
            .setKey("key")
            .setStamp(1000L)
            .setValue(payload)
            .build();
    final String uuid = UUID.randomUUID().toString();
    Optional<StreamElement> element = PubSubUtils.toStreamElement(entity, uuid, kv.toByteArray());

    assertTrue(element.isPresent());
    assertEquals("key", element.get().getKey());
    assertEquals(entity, element.get().getEntityDescriptor());
    assertEquals(attribute, element.get().getAttributeDescriptor());
    assertFalse(element.get().isDelete());
    assertFalse(element.get().isDeleteWildcard());
    assertEquals(1000L, element.get().getStamp());
    assertNotNull(element.get().getValue());
    assertEquals("DATA", new String(element.get().getValue()));
  }

  @Test
  void toStreamElementTest_correct_delete() {
    KeyValue kv =
        KeyValue.newBuilder()
            .setDelete(true)
            .setAttribute(attribute.getName())
            .setKey("key")
            .setStamp(1000L)
            .build();
    final String uuid = UUID.randomUUID().toString();
    Optional<StreamElement> element = PubSubUtils.toStreamElement(entity, uuid, kv.toByteArray());

    assertTrue(element.isPresent());
    assertEquals("key", element.get().getKey());
    assertEquals(entity, element.get().getEntityDescriptor());
    assertEquals(attribute, element.get().getAttributeDescriptor());
    assertTrue(element.get().isDelete());
    assertFalse(element.get().isDeleteWildcard());
    assertEquals(1000L, element.get().getStamp());
    assertFalse(element.get().getParsed().isPresent());
  }

  @Test
  void toStreamElementTest_correct_deleteWildcard() {
    KeyValue kv =
        KeyValue.newBuilder()
            .setDeleteWildcard(true)
            .setAttribute(wildcard.toAttributePrefix())
            .setKey("key")
            .setStamp(1000L)
            .build();
    final String uuid = UUID.randomUUID().toString();
    Optional<StreamElement> element = PubSubUtils.toStreamElement(entity, uuid, kv.toByteArray());

    assertTrue(element.isPresent());
    assertEquals("key", element.get().getKey());
    assertEquals(entity, element.get().getEntityDescriptor());
    assertEquals(wildcard, element.get().getAttributeDescriptor());
    assertTrue(element.get().isDelete());
    assertTrue(element.get().isDeleteWildcard());
    assertEquals(1000L, element.get().getStamp());
    assertFalse(element.get().getParsed().isPresent());
  }

  @Test
  void toStreamElementTest_illegal_attribute() {
    byte[] payload = new byte[0];
    final String uuid = UUID.randomUUID().toString();
    Optional<StreamElement> element = PubSubUtils.toStreamElement(entity, uuid, payload);

    assertFalse(element.isPresent());
  }

  @Test
  void toStreamElementTest_illegal_payload() {
    byte[] payload = new byte[] {1, 2, 3};
    final String uuid = UUID.randomUUID().toString();
    Optional<StreamElement> element = PubSubUtils.toStreamElement(entity, uuid, payload);

    assertFalse(element.isPresent());
  }
}
