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
package cz.o2.proxima.direct.time;

import com.google.common.base.Preconditions;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.PartitionedWatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Watermark estimator wrapper for partitioned sources. Estimates watermark as a minimum across all
 * partition's watermarks. Update and idle calls are delegated to estimator of a particular
 * partition.
 */
public class MinimalPartitionWatermarkEstimator implements PartitionedWatermarkEstimator {
  private static final long serialVersionUID = 1L;
  private final ConcurrentHashMap<Integer, WatermarkEstimator> estimators;

  public MinimalPartitionWatermarkEstimator(Map<Integer, WatermarkEstimator> partitionEstimators) {
    Preconditions.checkArgument(!partitionEstimators.isEmpty());
    estimators = new ConcurrentHashMap<>(partitionEstimators);
  }

  @Override
  public long getWatermark() {
    return estimators
        .values()
        .stream()
        .map(WatermarkEstimator::getWatermark)
        .min(Long::compare)
        .orElseThrow(IllegalStateException::new);
  }

  public long getWatermark(int partition) {
    return estimators.get(partition).getWatermark();
  }

  @Override
  public void update(int partition, StreamElement element) {
    Optional.ofNullable(estimators.get(partition))
        .ifPresent(estimator -> estimator.update(element));
  }

  @Override
  public void idle(int partition) {
    estimators.get(partition).idle();
  }
}
