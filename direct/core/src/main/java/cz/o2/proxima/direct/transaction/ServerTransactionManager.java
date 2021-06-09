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
package cz.o2.proxima.direct.transaction;

import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.Pair;
import javax.annotation.Nullable;

public interface ServerTransactionManager extends AutoCloseable, TransactionManager {

  /**
   * Observe all transactional families with given observer.
   *
   * @param name name of the observer (will be appended with name of the family)
   * @param requestObserver the observer (need not be synchronized)
   */
  default void runObservations(String name, CommitLogObserver requestObserver) {
    runObservations(name, (elem, p) -> {}, requestObserver);
  }

  /**
   * Observe all transactional families with given observer.
   *
   * @param name name of the observer (will be appended with name of the family)
   * @param updateConsumer consumer of updates to the view of transaction states
   * @param requestObserver the observer (need not be synchronized)
   */
  void runObservations(
      String name,
      BiConsumer<StreamElement, Pair<Long, Object>> updateConsumer,
      CommitLogObserver requestObserver);

  /**
   * Retrieve current state of the transaction.
   *
   * @param transactionId ID of the transaction
   * @return the {@link State} associated with the transaction on server
   */
  State getCurrentState(String transactionId);

  /**
   * Write new {@link State} for the transaction.
   *
   * @param transactionId ID of the transaction
   * @param state the new state, when {@code null} the state is erased and the transaction is
   *     cleared
   * @param callback callback for committing the write
   */
  void setCurrentState(String transactionId, @Nullable State state, CommitCallback callback);

  /**
   * Ensure that the given transaction ID is initialized.
   *
   * @param transactionId ID of the transaction
   * @param state the state that the transaction is supposed to have
   */
  void ensureTransactionOpen(String transactionId, State state);

  /**
   * Write response for a request to the caller.
   *
   * @param transactionId ID of transaction
   * @param responseId ID of response
   * @param response the response
   * @param callback callback for commit after write
   */
  void writeResponse(
      String transactionId, String responseId, Response response, CommitCallback callback);

  @Override
  void close();

  /**
   * Called by the server to signal that the manager should reclaim any resources that are not
   * needed anymore.
   */
  void houseKeeping();
}
