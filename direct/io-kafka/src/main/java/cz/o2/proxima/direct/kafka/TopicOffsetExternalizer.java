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
package cz.o2.proxima.direct.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.commitlog.OffsetExternalizer;
import cz.o2.proxima.scheme.SerializationException;
import java.util.HashMap;

/** Externalizes Kafka offset to external formats. */
class TopicOffsetExternalizer implements OffsetExternalizer {

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Override
  public String toJson(Offset offset) {
    try {
      final TopicOffset topicOffset = (TopicOffset) offset;
      final HashMap<String, Object> jsonMap = new HashMap<>();
      jsonMap.put("topic", topicOffset.getPartition().getTopic());
      jsonMap.put("partition", topicOffset.getPartition().getPartition());
      jsonMap.put("offset", topicOffset.getOffset());
      jsonMap.put("watermark", topicOffset.getWatermark());

      return JSON_MAPPER.writeValueAsString(jsonMap);
    } catch (JsonProcessingException e) {
      throw new SerializationException("Offset can't be externalized to Json", e);
    }
  }

  @Override
  public TopicOffset fromJson(String json) {
    try {
      final HashMap<String, Object> jsonMap =
          JSON_MAPPER.readValue(json, new TypeReference<HashMap<String, Object>>() {});

      final PartitionWithTopic partitionWithTopic =
          new PartitionWithTopic((String) jsonMap.get("topic"), (int) jsonMap.get("partition"));
      return new TopicOffset(
          partitionWithTopic,
          ((Number) jsonMap.get("offset")).longValue(),
          ((Number) jsonMap.get("watermark")).longValue());

    } catch (JsonProcessingException e) {
      throw new SerializationException("Offset can't be create from externalized Json", e);
    }
  }
}
