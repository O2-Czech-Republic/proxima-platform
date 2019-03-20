/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.view;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import javax.annotation.Nullable;
import cz.o2.proxima.direct.commitlog.LogObserver;

/**
 * Observer of data in {@code PartitionedView}.
 */
@Stable
public interface PartitionedLogObserver<T> extends LogObserver {

  @FunctionalInterface
  public interface ConfirmCallback extends Serializable {

    /**
     * Confirm processing of element.
     * @param success success/fail flag
     * @param error error that was thrown during processing
     */
    void confirm(boolean success, @Nullable Throwable error);

    /**
     * Successfully confirm the processing.
     */
    default void confirm() {
      confirm(true, null);
    }

    /**
     * Confirm failure of processing.
     * @param error error caught during processing
     */
    default void fail(Throwable error) {
      confirm(false, error);
    }

  }

  /**
   * Process next record in the commit log.
   * @param ingest the ingested data written to the commit log
   * @param confirm a callback that the application must use to confirm processing
   * of the ingest. If the application fails to do so, the result is undefined
   * @param partition the source partition of this ingest
   * @param collector collector of the output data that will be available as output
   *                 {@code Dataset}
   * @return {@code true} if the processing should continue, {@code false} otherwise
   **/
  boolean onNext(
      StreamElement ingest, ConfirmCallback confirm,
      Partition partition,
      Consumer<T> collector);


  /**
   * Notify that the processing has gracefully ended.
   */
  @Override
  default void onCompleted() {

  }

}
