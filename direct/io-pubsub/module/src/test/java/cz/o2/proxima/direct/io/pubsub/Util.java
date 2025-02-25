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
package cz.o2.proxima.direct.io.pubsub;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.io.pubsub.proto.PubSub;
import java.util.UUID;

/** Various utilities. */
class Util {

  private Util() {
    // no-op
  }

  static StreamElement update(
      String key, EntityDescriptor entity, AttributeDescriptor<?> attr, byte[] value, long stamp) {

    return StreamElement.upsert(
        entity, attr, UUID.randomUUID().toString(), key, attr.getName(), stamp, value);
  }

  static PubsubMessage update(String key, String attribute, byte[] value, long stamp) {
    return PubsubMessage.newBuilder()
        .setMessageId(UUID.randomUUID().toString())
        .setPublishTime(
            Timestamp.newBuilder()
                .setSeconds((int) (stamp / 1000))
                .setNanos((int) (stamp % 1_000L) * 1_000_000))
        .setData(
            PubSub.KeyValue.newBuilder()
                .setKey(key)
                .setAttribute(attribute)
                .setValue(ByteString.copyFrom(value))
                .setStamp(stamp)
                .build()
                .toByteString())
        .build();
  }

  static StreamElement delete(
      String key, EntityDescriptor entity, AttributeDescriptor<?> attr, long stamp) {

    return StreamElement.delete(
        entity, attr, UUID.randomUUID().toString(), key, attr.getName(), stamp);
  }

  static PubsubMessage delete(String key, String attribute, long stamp) {
    return PubsubMessage.newBuilder()
        .setMessageId(UUID.randomUUID().toString())
        .setPublishTime(
            Timestamp.newBuilder()
                .setSeconds((int) (stamp / 1000))
                .setNanos((int) (stamp % 1_000L) * 1_000_000))
        .setData(
            PubSub.KeyValue.newBuilder()
                .setKey(key)
                .setAttribute(attribute)
                .setDelete(true)
                .setStamp(stamp)
                .build()
                .toByteString())
        .build();
  }

  static StreamElement deleteWildcard(
      String key, EntityDescriptor entity, AttributeDescriptor<?> wildcard, long stamp) {

    return StreamElement.deleteWildcard(entity, wildcard, UUID.randomUUID().toString(), key, stamp);
  }

  static PubsubMessage deleteWildcard(String key, AttributeDescriptor<?> attribute, long stamp) {
    return PubsubMessage.newBuilder()
        .setMessageId(UUID.randomUUID().toString())
        .setPublishTime(
            Timestamp.newBuilder()
                .setSeconds((int) (stamp / 1000))
                .setNanos((int) (stamp % 1_000L) * 1_000_000))
        .setData(
            PubSub.KeyValue.newBuilder()
                .setKey(key)
                .setAttribute(attribute.toAttributePrefix())
                .setDeleteWildcard(true)
                .setStamp(stamp)
                .build()
                .toByteString())
        .build();
  }
}
