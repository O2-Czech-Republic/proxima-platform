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
package cz.o2.proxima.direct.io.kafka;

import cz.o2.proxima.core.scheme.SerializationException;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.commitlog.OffsetExternalizer;
import cz.o2.proxima.internal.com.google.gson.JsonObject;
import cz.o2.proxima.internal.com.google.gson.JsonParser;

/** Externalizes Kafka offset to external formats. */
class TopicOffsetExternalizer implements OffsetExternalizer {

  @Override
  public String toJson(Offset offset) {
    final TopicOffset topicOffset = (TopicOffset) offset;
    final JsonObject json = new JsonObject();
    json.addProperty("topic", topicOffset.getPartition().getTopic());
    json.addProperty("partition", topicOffset.getPartition().getPartition());
    json.addProperty("offset", topicOffset.getOffset());
    json.addProperty("watermark", topicOffset.getWatermark());

    return json.toString();
  }

  @Override
  public TopicOffset fromJson(String json) {
    try {
      final JsonObject object = JsonParser.parseString(json).getAsJsonObject();
      final PartitionWithTopic partitionWithTopic =
          new PartitionWithTopic(
              object.get("topic").getAsString(), object.get("partition").getAsInt());
      return new TopicOffset(
          partitionWithTopic,
          object.get("offset").getAsLong(),
          object.get("watermark").getAsLong());
    } catch (Exception ex) {
      throw new SerializationException("Failed to deserialize " + json, ex);
    }
  }
}
