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
package cz.o2.proxima.direct.core.transaction;

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import java.io.Serializable;

public interface ServerTransactionManager extends AutoCloseable, TransactionManager {

  @FunctionalInterface
  interface InitialSequenceIdPolicy extends Serializable {
    /**
     * Retrieve an initial seed for sequential ID.
     *
     * @return the new lower bound for sequential ID.
     */
    long apply();

    class Default implements InitialSequenceIdPolicy {
      @Override
      public long apply() {
        return System.currentTimeMillis() * 1000L;
      }
    }
  }

  interface ServerTransactionConfig extends TransactionConfig {
    /** Get initial sequential ID policy. */
    InitialSequenceIdPolicy getInitialSeqIdPolicy();

    TransactionMonitoringPolicy getTransactionMonitoringPolicy();

    int getServerTerminationTimeoutSeconds();
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
   * Ensure that the given transaction ID is initialized.
   *
   * @param transactionId ID of the transaction
   * @param state the state that the transaction is supposed to have
   */
  void ensureTransactionOpen(String transactionId, State state);

  /**
   * Atomically write response and update state of a transaction
   *
   * @param transactionId ID of transaction
   * @param state the state to update the transaction to
   * @param responseId ID of response
   * @param response the response
   * @param callback callback for commit after write
   */
  void writeResponseAndUpdateState(
      String transactionId,
      State state,
      String responseId,
      Response response,
      CommitCallback callback);

  @Override
  void close();

  /**
   * Called by the server to signal that the manager should reclaim any resources that are not
   * needed anymore.
   */
  void houseKeeping();

  /** Retrieve a configuration read from the {@link Repository}. */
  @Override
  ServerTransactionConfig getCfg();
}
