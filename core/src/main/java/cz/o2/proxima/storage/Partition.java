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
package cz.o2.proxima.storage;

import com.google.common.base.MoreObjects;
import cz.o2.proxima.annotations.Stable;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * Interface representing a partition of the commit log. A partition is an element of parallelism,
 * an atomic part that is read all at once and cannot be divided.
 */
@Stable
@FunctionalInterface
public interface Partition extends Serializable, Comparable<Partition> {

  /**
   * Retrieve id of the partition.
   *
   * @return if od the partition
   */
  int getId();

  /** @return Retrieve minimal timestamp associated with data in this partition. */
  default long getMinTimestamp() {
    return Long.MIN_VALUE;
  }

  /** @return Retrieve maximal timestamp associated with data in this partition. */
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
    return Collections.singletonList(this);
  }

  default int compareTo(Partition other) {
    int cmp = Long.compare(getMinTimestamp(), other.getMinTimestamp());
    if (cmp != 0) {
      return cmp;
    }
    return Integer.compare(getId(), other.getId());
  }

  /**
   * Wrap numerical id to {@link Partition} object.
   *
   * @param id the ID of partition
   * @return partition
   */
  static Partition of(int id) {
    return new Partition() {
      @Override
      public int getId() {
        return id;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(Partition.class).add("id", id).toString();
      }
    };
  }

  /**
   * Wrap numerical id to {@link Partition} with given minimal timestamp.
   *
   * @param id if the ID of partition
   * @param minTimestamp minimal timestamp of the partition
   * @return partition
   */
  static Partition withMinimalTimestamp(int id, long minTimestamp) {
    return new Partition() {

      @Override
      public int getId() {
        return id;
      }

      @Override
      public long getMinTimestamp() {
        return minTimestamp;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(Partition.class)
            .add("id", id)
            .add("minTimestamp", minTimestamp)
            .toString();
      }
    };
  }
}
