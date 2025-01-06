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
package cz.o2.proxima.io.pubsub.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.io.pubsub.proto.PubSub;
import cz.o2.proxima.io.pubsub.proto.PubSub.KeyValue;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubUtils {

  private PubSubUtils() {
    // no-op
  }

  /**
   * Convert pubsub payload into {@link StreamElement}.
   *
   * @param entity Entity descriptor
   * @param uuid message uuid
   * @param payload paylod to parse
   * @return StreamElement
   */
  public static Optional<StreamElement> toStreamElement(
      EntityDescriptor entity, String uuid, byte[] payload) {

    try {
      return toStreamElement(entity, uuid, PubSub.KeyValue.parseFrom(payload));
    } catch (InvalidProtocolBufferException e) {
      log.warn("Failed to parse message with uuid {}", uuid, e);
      return Optional.empty();
    }
  }

  /**
   * Convert pubsub payload into {@link StreamElement}.
   *
   * @param entity Entity descriptor
   * @param uuid message uuid
   * @param kv the {@code KeyValue}
   * @return StreamElement
   */
  public static Optional<StreamElement> toStreamElement(
      EntityDescriptor entity, String uuid, PubSub.KeyValue kv) {

    final long stamp = kv.getStamp();
    Optional<AttributeDescriptor<Object>> attribute =
        entity.findAttribute(kv.getAttribute(), true /* allow protected */);

    if (attribute.isPresent()) {
      if (kv.getDelete()) {
        return Optional.of(
            StreamElement.delete(
                entity, attribute.get(), uuid, kv.getKey(), kv.getAttribute(), stamp));
      } else if (kv.getDeleteWildcard()) {
        return Optional.of(
            StreamElement.deleteWildcard(
                entity, attribute.get(), uuid, kv.getKey(), kv.getAttribute(), stamp));
      }
      return Optional.of(
          StreamElement.upsert(
              entity,
              attribute.get(),
              uuid,
              kv.getKey(),
              kv.getAttribute(),
              stamp,
              kv.getValue().toByteArray()));
    }
    log.warn("Failed to find attribute {} in entity {}", kv.getAttribute(), entity);
    return Optional.empty();
  }

  /**
   * Create {@link KeyValue} from {@link StreamElement}.
   *
   * @param element the StreamElement
   * @return {@link KeyValue}
   */
  public static PubSub.KeyValue toKeyValue(StreamElement element) {
    return KeyValue.newBuilder()
        .setKey(element.getKey())
        .setAttribute(element.getAttribute())
        .setDelete(element.isDelete())
        .setDeleteWildcard(element.isDeleteWildcard())
        .setValue(element.isDelete() ? ByteString.EMPTY : ByteString.copyFrom(element.getValue()))
        .setStamp(element.getStamp())
        .build();
  }
}
