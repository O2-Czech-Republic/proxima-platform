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

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkSupplier;
import lombok.Value;

/**
 * Batch observer of data. No commits needed.
 *
 * <p>Implementations should override either of `onNext` methods.
 */
@Stable
public interface BatchLogObserver {

  @Value
  class SimpleOffset implements BatchLogObserver.Offset {
    Partition partition;
    long elementIndex;
    boolean last;
  }

  interface Offset {

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

  /** Context passed to {@link #onNext}. */
  @Stable
  interface OnNextContext extends WatermarkSupplier {

    /**
     * Retrieve partition for currently processed record.
     *
     * @return partition of currently processed record
     */
    Partition getPartition();

    /**
     * Retrieve offset of the current element.
     *
     * @return Offset.
     */
    Offset getOffset();

    /**
     * Retrieve current watermark of the observe process
     *
     * @return watermark in milliseconds
     */
    @Override
    long getWatermark();
  }

  /**
   * Read next data from the batch storage.
   *
   * @param element the retrieved data element
   * @return {@code true} to continue processing, {@code false} otherwise
   */
  default boolean onNext(StreamElement element) {
    throw new UnsupportedOperationException("Please override either of `onNext` methods");
  }

  /**
   * Read next data from the batch storage.
   *
   * @param element the retrieved data element
   * @param context context of the data element
   * @return {@code true} to continue processing, {@code false} otherwise
   */
  default boolean onNext(StreamElement element, OnNextContext context) {
    return onNext(element);
  }

  /** Signalled when the reading is finished. */
  default void onCompleted() {}

  /** Signalled when the reading is cancelled. */
  default void onCancelled() {}

  /**
   * Signaled when reading error occurs.
   *
   * @param error error caught during processing
   * @return {@code true} to restart processing from beginning {@code false} to stop processing
   */
  default boolean onError(Throwable error) {
    return false;
  }
}
