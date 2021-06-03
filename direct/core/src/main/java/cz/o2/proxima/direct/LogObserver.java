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
package cz.o2.proxima.direct;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkSupplier;
import java.io.Serializable;

/** Base interface for bulk and online observers. */
@Stable
public interface LogObserver<
        OffsetT extends Serializable, ContextT extends LogObserver.OnNextContext<OffsetT>>
    extends Serializable {

  /** Context passed to {@link #onNext}. */
  @Stable
  interface OnNextContext<OffsetT extends Serializable> extends WatermarkSupplier {

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
    OffsetT getOffset();
  }

  /** Notify that the processing has gracefully ended. */
  default void onCompleted() {}

  /** Notify that the processing has been canceled. */
  default void onCancelled() {}

  /**
   * Called to notify there was an {@link Throwable error} in the commit reader.
   *
   * @param error error caught during processing
   * @return {@code true} to restart processing from last committed position, {@code false} to stop
   *     processing
   */
  default boolean onError(Throwable error) {
    if (error instanceof Error) {
      return onFatalError((Error) error);
    }
    if (error instanceof Exception) {
      return onException((Exception) error);
    }
    throw new UnsupportedOperationException(
        String.format("Unknown throwable implementation [%s].", error.getClass()));
  }

  /**
   * Called to notify there was an {@link Exception exception} in the commit reader. There is no
   * guarantee this method gets called, if {@link #onError(Throwable)} is overridden.
   *
   * @param exception exception caught during processing
   * @return {@code true} to restart processing from last committed position, {@code false} to stop
   *     processing
   */
  default boolean onException(Exception exception) {
    throw new IllegalStateException("Unhandled exception.", exception);
  }

  /**
   * Called to notify there was an {@link Error error} in the commit reader. There is no guarantee
   * this method gets called, if {@link #onError(Throwable)} is overridden.
   *
   * @param error error caught during processing
   * @return {@code true} to restart processing from last committed position, {@code false} to stop
   *     processing
   */
  default boolean onFatalError(Error error) {
    throw error;
  }

  /**
   * Process next record in the commit log.
   *
   * @param ingest the ingested data written to the commit log
   * @param context a context that the application must use to confirm processing of the ingest. If
   *     the application fails to do so, the result is undefined.
   * @return {@code true} if the processing should continue, {@code false} otherwise
   */
  boolean onNext(StreamElement ingest, ContextT context);
}
