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

import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.Response;
import java.util.List;

public interface ClientTransactionManager extends AutoCloseable, TransactionManager {

  /**
   * Initialize (possibly) new transaction. If the transaction already existed prior to this call,
   * its current state is returned, otherwise the transaction is opened.
   *
   * @param transactionId ID of the transaction
   * @param responseConsumer consumer of responses related to the transaction
   * @param attributes attributes affected by this transaction (both input and output)
   * @return current state of the transaction
   */
  void begin(
      String transactionId,
      BiConsumer<String, Response> responseConsumer,
      List<KeyAttribute> attributes);

  /**
   * Update the transaction with additional attributes related to the transaction.
   *
   * @param transactionId ID of the transaction
   * @param newAttributes attributes to be added to the transaction
   * @return
   */
  void updateTransaction(String transactionId, List<KeyAttribute> newAttributes);

  /**
   * Commit the transaction with given output KeyAttributes being written.
   *
   * @param transactionId ID of the transaction
   * @param outputAttributes attributes to be written to the output
   */
  void commit(String transactionId, List<KeyAttribute> outputAttributes);

  /**
   * Rollback transaction with given ID.
   *
   * @param transactionId ID of the transaction to rollback.
   */
  void rollback(String transactionId);

  @Override
  void close();
}
