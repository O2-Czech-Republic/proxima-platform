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
package cz.o2.proxima.storage.kafka;

import java.io.Serializable;
import java.util.Map;

/**
 * An interface that each class configured in {@code KafkaCommitLog.PARTITIONER_CLASS}
 * must implement. The class also has to have a default (empty) constructor.
 */
public interface Partitioner extends Serializable {

  /**
   * Retrieve partition ID for the specified ingest.
   * All ingests that have the same partition ID are guaranteed to
   * be written to the same Kafka partition.
   * @param key key of entity
   * @param attribute name of attribute
   * @param value serialized bytes of value
   * @return ID of partition (can be negative)
   */
  int getPartitionId(String key, String attribute, byte[] value);

  /**
   * Setup the partitioner (if needed).
   * @param map a configuration map
   */
  default void setup(Map<String, ?> map) {

  }

  /**
   * Close the partitioner after usage.
   */
  default void close() {

  }

}
