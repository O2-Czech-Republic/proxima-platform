/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.time;

import cz.o2.proxima.core.storage.StreamElement;

/** Watermark estimator wrapper for partitioned sources. */
public interface PartitionedWatermarkEstimator extends WatermarkSupplier {

  /**
   * Returns estimated watermark across all partitions.
   *
   * @return the watermark estimate.
   */
  long getWatermark();

  /**
   * Updates the partition watermark estimate according to the given stream element.
   *
   * @param partition partition
   * @param element a stream element.
   */
  default void update(int partition, StreamElement element) {}

  /**
   * Signals that a given partition is idle.
   *
   * @param partition partition
   */
  default void idle(int partition) {}
}
