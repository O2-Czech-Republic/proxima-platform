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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.annotations.Stable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

/**
 * Interface representing a partition of the commit log. A partition is an element of parallelism,
 * an atomic part that is read all at once and cannot be divided.
 */
@Stable
@FunctionalInterface
public interface Partition extends Serializable {

  /**
   * Retrieve id of the partition.
   *
   * @return if od the partition
   */
  int getId();

  /** Retrieve minimal timestamp associated with data in this partition. */
  default long getMinTimestamp() {
    return Long.MIN_VALUE;
  }

  /** Retrieve maximal timestamp associated with data in this partition. */
  default long getMaxTimestamp() {
    return Long.MAX_VALUE;
  }

  /**
   * Check if this is bounded or unbounded partition.
   *
   * @return {@code true} if this is bounded partition
   */
  default boolean isBounded() {
    return false;
  }

  /**
   * Estimate size of this partition.
   *
   * @return estimated size of this partition or -1 if unknown
   */
  default long size() {
    return -1L;
  }

  /**
   * Verify if this partition can be split into two when reading.
   *
   * @return {@code true} when the partition can be split and read independently in two consumers.
   */
  default boolean isSplittable() {
    return false;
  }

  /**
   * Split this partition to given number of sub-partitions.
   *
   * @param desiredCount desired number of split partitions
   * @return collection of split partitions
   */
  default Collection<Partition> split(int desiredCount) {
    return Arrays.asList(this);
  }
}
