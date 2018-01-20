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
package cz.o2.proxima.storage.batch;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.Partition;
import java.util.List;

/**
 * Observer of batch data stored in batch storage.
 */
public interface BatchLogObservable {

  /**
   * Retrieve list of partitions of this batch observer.
   * @return list of partitions of this observable
   */
  default List<Partition> getPartitions() {
    return getPartitions(Long.MIN_VALUE);
  }

  /**
   * Retrieve list of partitions covering data at least from given starting stamp.
   * @param startStamp timestamp to start reading from
   * @return list of partitions covering the time range
   */
  default List<Partition> getPartitions(long startStamp) {
    return getPartitions(startStamp, Long.MAX_VALUE);
  }

  /**
   * Retrieve list of partitions covering data from the given range.
   * @param startStamp starting timestamp (inclusive)
   * @param endStamp ending timestamp (exclusive)
   * @return list of partitions covering the time range
   */
  List<Partition> getPartitions(long startStamp, long endStamp);

  /**
   * Observe data stored in given partitions.
   * @param partitions partitions to observe
   * @param attributes attribute descriptors to filter out
   * @param observer the observer by which to consume the data
   */
  void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer);

}
