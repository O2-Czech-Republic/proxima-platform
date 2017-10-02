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

package cz.o2.proxima.storage;

import java.io.Serializable;

/**
 * Interface representing a partition of the commit log.
 * A partition is an element of parallelism, an atomic part
 * that is read all at once and cannot be divided.
 */
@FunctionalInterface
public interface Partition extends Serializable {

  /** Retrieve id of the partition. */
  int getId();

  /**
   * Is this bounded or unbounded partition.
   */
  default boolean isBounded() {
    return false;
  }

  /**
   * Estimate size of this partition.
   */
  default long size() {
    return -1L;
  }

}

