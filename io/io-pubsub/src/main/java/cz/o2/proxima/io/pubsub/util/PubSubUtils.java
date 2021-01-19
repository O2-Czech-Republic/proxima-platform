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

import com.google.protobuf.InvalidProtocolBufferException;
import cz.o2.proxima.io.pubsub.proto.PubSub.KeyValue;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
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
      final KeyValue parsed = KeyValue.parseFrom(payload);
      final long stamp = parsed.getStamp();
      Optional<AttributeDescriptor<Object>> attribute =
          entity.findAttribute(parsed.getAttribute(), true /* allow protected */);

      if (attribute.isPresent()) {
        if (parsed.getDelete()) {
          return Optional.of(
              StreamElement.delete(
                  entity, attribute.get(), uuid, parsed.getKey(), parsed.getAttribute(), stamp));
        } else if (parsed.getDeleteWildcard()) {
          return Optional.of(
              StreamElement.deleteWildcard(
                  entity, attribute.get(), uuid, parsed.getKey(), parsed.getAttribute(), stamp));
        }
        return Optional.of(
            StreamElement.upsert(
                entity,
                attribute.get(),
                uuid,
                parsed.getKey(),
                parsed.getAttribute(),
                stamp,
                parsed.getValue().toByteArray()));
      }
      log.warn("Failed to find attribute {} in entity {}", parsed.getAttribute(), entity);

    } catch (InvalidProtocolBufferException e) {
      log.warn("Failed to parse message with uuid {}", uuid, e);
    }

    return Optional.empty();
  }
}
