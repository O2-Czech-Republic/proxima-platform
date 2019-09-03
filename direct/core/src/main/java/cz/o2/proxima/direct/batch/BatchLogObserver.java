/**
 * Copyright 2017-${Year} O2 Czech Republic, a.s.
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
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.storage.StreamElement;

/**
 * Batch observer of data. No commits needed.
 *
 * <p>Implementations should override either of `onNext` methods.
 */
@Stable
public interface BatchLogObserver {

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
   * @param partition the partition of the element
   * @return {@code true} to continue processing, {@code false} otherwise
   */
  default boolean onNext(StreamElement element, Partition partition) {
    return onNext(element);
  }

  /** Signalled when the reading is finished. */
  default void onCompleted() {}

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
