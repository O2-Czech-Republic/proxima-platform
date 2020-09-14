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
package cz.o2.proxima.direct.batch;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.batch.BatchLogObserver.OnNextContext;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.time.WatermarkSupplier;

/** Utility class related to {@link BatchLogObserver BatchLogObservers}. */
@Internal
public class BatchLogObservers {

  /**
   * Create {@link OnNextContext} which holds watermark back on {@link Long#MIN_VALUE} until the end
   * of data. This is the default behavior of batch readers when there is no way to time-order data.
   *
   * @param partition the partition to create context for
   * @return a wrapped {@link OnNextContext} for given partition
   */
  public static OnNextContext defaultContext(Partition partition) {
    return withWatermark(partition, Long.MIN_VALUE);
  }

  /**
   * Create {@link OnNextContext} which moves watermark according to given {@link
   * WatermarkSupplier}.
   *
   * @param partition the partition to create context for
   * @param watermark {@link WatermarkSupplier} for watermark at any given time. The supplier can
   *     assume that each element gets consumed immediately after being passed to {@link
   *     BatchLogObserver#onNext}
   * @return a wrapped {@link OnNextContext} for given partition and given watermark supplier
   */
  public static OnNextContext withWatermarkSupplier(
      Partition partition, WatermarkSupplier watermark) {

    return new OnNextContext() {

      private static final long serialVersionUID = 1L;

      @Override
      public Partition getPartition() {
        return partition;
      }

      @Override
      public long getWatermark() {
        return watermark.getWatermark();
      }
    };
  }

  /**
   * Create {@link OnNextContext} which sets watermark ti given epoch millis.
   *
   * @param partition the partition to create context for
   * @param watermark epoch millis to set the watermark to
   * @return a wrapped {@link OnNextContext} for given partition with given watermark
   */
  public static OnNextContext withWatermark(Partition partition, long watermark) {
    return new OnNextContext() {
      @Override
      public Partition getPartition() {
        return partition;
      }

      @Override
      public long getWatermark() {
        return watermark;
      }
    };
  }

  private BatchLogObservers() {}
}
