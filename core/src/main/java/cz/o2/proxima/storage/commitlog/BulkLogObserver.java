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

package cz.o2.proxima.storage.commitlog;

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import javax.annotation.Nullable;

/**
 * Observer of a {@code CommitLog} that commits offsets in bulk fashion
 * and not one-by-one.
 * This observer provides access only to the data that was not yet consumed
 * by the named consumer. The consumer maintains position in the commit log
 * so if it crashes, then it will continue consuming from the committed position.
 * However, it is not guaranteed that a message will not be delivered multiple
 * times, nor it is guaranteed that messages will be delivered in-order.
 *
 * Implementation has to override either of `onNext` methods.
 */
public interface BulkLogObserver extends LogObserverBase {

  @FunctionalInterface
  interface BulkCommitter {

    /**
     * Commit bulk processing of all currently uncommitted messages.
     * @param success the successs flag
     * @param err error thrown in bulk processing (if any)
     */
    void commit(boolean success, @Nullable Throwable err);

    /**
     * Confirm successfull processing.
     */
    default void commit() {
      commit(true, null);
    }

    /**
     * Fail the bulk processing.
     * @param err the error thrown during processing
     */
    default void fail(Throwable err)  {
      commit(false, err);
    }
  }

  /**
   * Process next record in the commit log.
   * @param ingest the ingested data written to the commit log
   * @param confirm a callback that the application *might* use to commit the ingest
   * until the confirm is committed, all uncommitted elements will be reprocessed
   * in case of failure. Call to `BulkCommitter#commit` all elements
   * processed so far will be committed.
   * @returns {@code true} if the processing should continue, {@code false} otherwise
   **/
  default boolean onNext(StreamElement ingest, BulkCommitter confirm) {
    throw new UnsupportedOperationException(
        "Please override either of `onNext`methods");
  }


  /**
   * Process next record in the commit log.
   * @param ingest the ingested data written to the commit log
   * @param confirm a callback that the application *might* use to commit the ingest
   * until the confirm is committed, all uncommitted elements will be reprocessed
   * in case of failure. Call to `BulkCommitter#commit` all elements
   * processed so far will be committed.
   * @returns {@code true} if the processing should continue, {@code false} otherwise
   **/
  default boolean onNext(
      StreamElement ingest,
      Partition partition,
      BulkCommitter confirm) {

    return onNext(ingest, confirm);
  }

  /**
   * Called when the bulk processing is restarted from last committed position.
   */
  default void onRestart() {

  }

}
