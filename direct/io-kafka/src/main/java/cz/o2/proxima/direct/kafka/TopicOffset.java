/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import com.google.common.base.MoreObjects;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.Partition;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;

/** Offset used in bulk consumption. */
class TopicOffset implements Offset {

  private static final long serialVersionUID = 1L;

  // map of partitionId -> committed offset
  private final int partition;
  // offset to kafka partition, < 0 means undefined (or default)
  @Getter private final long offset;
  @Getter private final long watermark;

  TopicOffset(int partition, long offset, long watermark) {
    this.partition = partition;
    this.offset = offset;
    this.watermark = watermark;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partition", partition)
        .add("offset", offset)
        .add("watermark", watermark)
        .toString();
  }

  @Override
  public Partition getPartition() {
    return Partition.of(partition);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TopicOffset) {
      TopicOffset other = (TopicOffset) obj;
      return other.partition == partition && other.offset == offset && other.watermark == watermark;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, offset, watermark);
  }

  static List<TopicOffset> fromMap(Map<Integer, Long> offsetMap, long watermark) {
    return offsetMap
        .entrySet()
        .stream()
        .map(e -> new TopicOffset(e.getKey(), e.getValue(), watermark))
        .collect(Collectors.toList());
  }
}
