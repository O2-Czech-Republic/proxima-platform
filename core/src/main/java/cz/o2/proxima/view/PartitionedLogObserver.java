/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.view;

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.Collection;
import javax.annotation.Nullable;

/**
 * Observer of data in {@code PartitionedView}.
 */
public interface PartitionedLogObserver<T> extends Serializable {

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

  @FunctionalInterface
  public interface Consumer<T> extends Serializable {

    void consume(T what);

  }


  /**
   * A repartitioning operation has just happened.
   * This method is called before first element is processed and
   * then after each repartition.
   * @param assigned collection of assigned partitions
   */
  default void onRepartition(Collection<Partition> assigned) {

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
  default void onCompleted() {

  }

  /**
   * Called to notify there was an error in the reader.
   * @param error the error caught during processing
   */
  void onError(Throwable error);


}
