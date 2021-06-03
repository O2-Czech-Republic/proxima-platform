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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.LogObserver;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.time.WatermarkSupplier;
import java.io.Serializable;
import java.util.Collection;
import javax.annotation.Nullable;

/** Base interface for bulk and online observers. */
@Stable
public interface CommitLogObserver extends LogObserver<Offset, CommitLogObserver.OnNextContext> {

  /** Committer for manipulation with offset during consumption. */
  interface OffsetCommitter extends Serializable {

    /**
     * Confirm processing of element.
     *
     * @param success success/fail flag
     * @param error error that was thrown during processing
     */
    void commit(boolean success, @Nullable Throwable error);

    /** Successfully confirm the processing. */
    default void confirm() {
      commit(true, null);
    }

    /**
     * Confirm failure of processing.
     *
     * @param error error caught during processing
     */
    default void fail(Throwable error) {
      commit(false, error);
    }

    /** Nack processing (no error thrown, message can be immediately retried). */
    default void nack() {
      commit(false, null);
    }
  }

  /** Context passed to {@link #onNext}. */
  @Stable
  interface OnNextContext extends LogObserver.OnNextContext<Offset>, OffsetCommitter {

    /**
     * Retrieve committer for currently processed record.
     *
     * @return offset committer to use for committing
     */
    OffsetCommitter committer();

    @Override
    default void commit(boolean success, Throwable error) {
      committer().commit(success, error);
    }
  }

  /** Context passed to {@link #onIdle}. */
  @Stable
  interface OnIdleContext extends Serializable, WatermarkSupplier {

    @Override
    long getWatermark();
  }

  /** Context passed to {@link #onRepartition}. */
  @Stable
  interface OnRepartitionContext {

    /**
     * Retrieve list of currently assigned partitions.
     *
     * @return partitions currently assigned (after the repartition)
     */
    Collection<Partition> partitions();
  }

  /**
   * Callback to notify of automatic repartitioning. This method is always called first before any
   * {@link #onNext} call happens.
   *
   * @param context context of the repartition
   */
  default void onRepartition(OnRepartitionContext context) {}

  /**
   * Called when the observer is idle. Note that the definition of idle is commit-log dependent and
   * it even might NOT be called at all, if the commit log guarantees that as long as there are
   * *any* data flowing in, then {@link #onNext} will be called eventually.
   *
   * <p>Typical example of commit log with no need to call {@link #onIdle} is google PubSub, having
   * virtually single shared partition which loads balances incoming data.
   *
   * @param context the context for on idle processing
   */
  default void onIdle(OnIdleContext context) {}
}
