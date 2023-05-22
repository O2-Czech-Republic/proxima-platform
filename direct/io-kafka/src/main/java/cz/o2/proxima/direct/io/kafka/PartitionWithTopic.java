/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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

import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import java.util.Objects;
import lombok.Getter;

/** A {@link Partition} having information about associated kafka partition and topic. */
class PartitionWithTopic implements Partition {

  @Getter private final String topic;
  @Getter private final int partition;

  PartitionWithTopic(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  @Override
  public int getId() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionWithTopic that = (PartitionWithTopic) o;
    return partition == that.partition && Objects.equals(topic, that.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("topic", topic)
        .add("partition", partition)
        .toString();
  }
}
