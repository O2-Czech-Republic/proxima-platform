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
package cz.o2.proxima.server.transaction;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.Transaction;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.server.IngestService;
import cz.o2.proxima.server.RetrieveService;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/** A per-transaction context shared between {@link RetrieveService} and {@link IngestService}. */
@ThreadSafe
@Internal
public class TransactionContext {

  public interface Transaction {
    String getTransactionId();

    void update(List<KeyAttribute> keyAttributes) throws TransactionRejectedException;

    void addOutputs(List<StreamElement> outputs);

    void rollback();

    void commit(CommitCallback callback) throws TransactionRejectedException;
  }

  private final Map<String, Transaction> openTransactions = new ConcurrentHashMap<>();
  // FIXME: currently we support only global transactions
  // need to support https://github.com/O2-Czech-Republic/proxima-platform/issues/216
  // for full support of other transactions
  @Nullable private final TransactionalOnlineAttributeWriter globalWriter;

  public TransactionContext(DirectDataOperator direct) {
    this.globalWriter =
        direct.getRepository().getAllEntities().anyMatch(EntityDescriptor::isTransactional)
            ? direct.getGlobalTransactionWriter()
            : null;
  }

  public Transaction create() {
    Preconditions.checkArgument(globalWriter != null, "No transactions are allowed in model!");

    Transaction res = wrap(globalWriter.begin());
    openTransactions.put(res.getTransactionId(), res);
    return res;
  }

  public Transaction get(String transactionId) {
    return Objects.requireNonNull(
        openTransactions.get(transactionId), () -> "Transaction " + transactionId + " is not open");
  }

  public void close() {
    // need to clone the values to prevent ConcurrentModificationException
    new ArrayList<>(openTransactions.values()).forEach(Transaction::rollback);
  }

  private Transaction wrap(TransactionalOnlineAttributeWriter.Transaction delegate) {
    return new Transaction() {

      private final List<StreamElement> allOutputs = new ArrayList<>();

      @Override
      public String getTransactionId() {
        return delegate.getTransactionId();
      }

      @Override
      public void update(List<KeyAttribute> keyAttributes) throws TransactionRejectedException {
        delegate.update(keyAttributes);
      }

      @Override
      public void addOutputs(List<StreamElement> outputs) {
        allOutputs.addAll(outputs);
      }

      @Override
      public void rollback() {
        delegate.rollback();
        openTransactions.remove(delegate.getTransactionId());
      }

      @Override
      public void commit(CommitCallback callback) throws TransactionRejectedException {
        delegate.commitWrite(allOutputs, callback);
        openTransactions.remove(delegate.getTransactionId());
      }
    };
  }
}
