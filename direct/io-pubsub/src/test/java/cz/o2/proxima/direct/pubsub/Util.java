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
package cz.o2.proxima.direct.pubsub;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.direct.pubsub.proto.PubSub;
import cz.o2.proxima.repository.AttributeDescriptor;

/** Various utilities. */
class Util {

  static PubsubMessage update(String key, String attribute, byte[] value, long stamp) {
    return PubsubMessage.newBuilder()
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

  static PubsubMessage delete(String key, String attribute, long stamp) {
    return PubsubMessage.newBuilder()
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

  static PubsubMessage deleteWildcard(String key, AttributeDescriptor<?> attribute, long stamp) {

    return PubsubMessage.newBuilder()
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
