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
package cz.o2.proxima.direct.batch;

import cz.o2.proxima.storage.Partition;
import lombok.Value;

public interface Offset {

  static SimpleOffset of(Partition partition, long elementIndex, boolean last) {
    return new SimpleOffset(partition, elementIndex, last);
  }

  @Value
  class SimpleOffset implements Offset {
    Partition partition;
    long elementIndex;
    boolean last;
  }

  /**
   * Partition the offset belongs to.
   *
   * @return Partition.
   */
  Partition getPartition();

  /**
   * Index of the element within the partition. Elements are indexed from zero.
   *
   * @return Index of the element.
   */
  long getElementIndex();

  /**
   * Is there any other element in the partition?
   *
   * @return True if there are no more elements in the partition.
   */
  boolean isLast();
}
