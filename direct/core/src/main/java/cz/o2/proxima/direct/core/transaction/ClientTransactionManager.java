/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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

import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.Response;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ClientTransactionManager extends AutoCloseable, TransactionManager {

  /**
   * Initialize (possibly) new transaction. If the transaction already existed prior to this call,
   * its current state is returned, otherwise the transaction is opened.
   *
   * @param transactionId ID of the transaction
   * @param attributes attributes affected by this transaction (both input and output)
   * @return asynchronous response
   */
  CompletableFuture<Response> begin(String transactionId, List<KeyAttribute> attributes);

  /**
   * Update the transaction with additional attributes related to the transaction.
   *
   * @param transactionId ID of the transaction
   * @param newAttributes attributes to be added to the transaction
   * @return asynchronous response
   */
  CompletableFuture<Response> updateTransaction(
      String transactionId, List<KeyAttribute> newAttributes);

  /**
   * Commit the transaction with given output KeyAttributes being written.
   *
   * @param transactionId ID of the transaction
   * @param outputs elements to be written to the output
   * @return asynchronous response
   */
  CompletableFuture<Response> commit(String transactionId, Collection<StreamElement> outputs);

  /**
   * Rollback transaction with given ID.
   *
   * @param transactionId ID of the transaction to rollback.
   * @return asynchronous response
   */
  CompletableFuture<Response> rollback(String transactionId);

  @Override
  void close();

  /**
   * Release resources associated with given transaction.
   *
   * @param transactionId ID of the transaction
   */
  void release(String transactionId);
}
