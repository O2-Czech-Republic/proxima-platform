/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.example;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.example.event.Event.BaseEvent;
import cz.o2.proxima.storage.kafka.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partitioner for the events.
 */
public class EventPartitioner implements Partitioner {

  private static final Logger LOG = LoggerFactory.getLogger(EventPartitioner.class);

  @Override
  public int getPartitionId(String key, String attribute, byte[] value) {
    if (value == null) {
      LOG.warn("Received event with null value!");
    } else {
      try {
        BaseEvent event = BaseEvent.parseFrom(value);
        int partition = event.getUserName().hashCode();
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Partitioned event {} to partition ID {}",
              TextFormat.shortDebugString(event), partition);
        }
        return partition;
      } catch (InvalidProtocolBufferException ex) {
        LOG.warn("Failed to parse event", ex);
      }
    }
    return 0;
  }

}
