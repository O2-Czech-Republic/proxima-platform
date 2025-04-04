/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.batch;

import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.LogObserver;

/**
 * Batch observer of data. No commits needed.
 *
 * <p>Implementations should override either of `onNext` methods.
 */
@Stable
public interface BatchLogObserver extends LogObserver<Offset, BatchLogObserver.OnNextContext> {

  /** Context passed to {@link #onNext}. */
  @Stable
  interface OnNextContext extends LogObserver.OnNextContext<Offset> {

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

  @Override
  default boolean onNext(StreamElement element, OnNextContext context) {
    return onNext(element);
  }

  /**
   * Called when the consumption is cancelled in case {@link #onNext(StreamElement, OnNextContext)}
   * method is blocked and should be interrupted.
   */
  default void onInterrupted() {}
}
