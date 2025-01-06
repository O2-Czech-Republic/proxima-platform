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
package cz.o2.proxima.direct.server.transaction;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State.Flags;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.server.IngestService;
import cz.o2.proxima.direct.server.RetrieveService;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/** A per-transaction context shared between {@link RetrieveService} and {@link IngestService}. */
@ThreadSafe
@Internal
public class TransactionContext implements AutoCloseable {

  public interface Transaction extends AutoCloseable {
    boolean isActive();

    String getTransactionId();

    void update(List<KeyAttribute> keyAttributes) throws TransactionRejectedException;

    void addOutputs(List<StreamElement> outputs);

    void rollback();

    void commit(CommitCallback callback) throws TransactionRejectedException;

    @Override
    void close();
  }

  private final ExecutorService executor;
  private final Map<String, Transaction> openTransactions = new ConcurrentHashMap<>();
  private final long cleanupInterval;
  private Future<?> cleanupFuture = null;
  // FIXME: currently we support only global transactions
  // need to support https://github.com/O2-Czech-Republic/proxima-platform/issues/216
  // for full support of other transactions
  @Nullable private final TransactionalOnlineAttributeWriter globalWriter;
  private final Supplier<Long> timeSupplier;

  public TransactionContext(DirectDataOperator direct) {
    this(direct, System::currentTimeMillis);
  }

  @VisibleForTesting
  TransactionContext(DirectDataOperator direct, Supplier<Long> timeSupplier) {
    this.executor = direct.getContext().getExecutorService();
    this.cleanupInterval = direct.getClientTransactionManager().getCfg().getCleanupInterval();
    this.globalWriter =
        direct.getRepository().getAllEntities().anyMatch(EntityDescriptor::isTransactional)
            ? direct.getGlobalTransactionWriter()
            : null;
    this.timeSupplier = timeSupplier;
  }

  public void run() {
    executor.submit(
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            ExceptionUtils.ignoringInterrupted(
                () -> {
                  TimeUnit.MILLISECONDS.sleep(cleanupInterval);
                  clearAnyStaleTransactions();
                });
          }
        });
  }

  @VisibleForTesting
  void clearAnyStaleTransactions() {
    openTransactions.values().stream()
        .filter(t -> !t.isActive())
        .collect(Collectors.toList())
        .forEach(Transaction::close);
  }

  public String create() {
    return create("");
  }

  public String create(String transactionId) {
    Preconditions.checkArgument(globalWriter != null, "No transactions are allowed in the model!");

    Transaction res =
        wrap(
            Objects.requireNonNull(transactionId).isEmpty()
                ? globalWriter.begin()
                : globalWriter.begin(transactionId));
    openTransactions.put(res.getTransactionId(), res);
    return res.getTransactionId();
  }

  public Transaction get(String transactionId) {
    return Objects.requireNonNull(
        openTransactions.get(transactionId), () -> "Transaction " + transactionId + " is not open");
  }

  @VisibleForTesting
  Map<String, Transaction> getTransactionMap() {
    return openTransactions;
  }

  @Override
  public void close() {
    // need to clone the values to prevent ConcurrentModificationException
    new ArrayList<>(openTransactions.values()).forEach(Transaction::close);
    Optional.ofNullable(cleanupFuture).ifPresent(f -> f.cancel(true));
  }

  @VisibleForTesting
  long currentTimeMillis() {
    return timeSupplier.get();
  }

  private Transaction wrap(TransactionalOnlineAttributeWriter.Transaction delegate) {
    return new Transaction() {

      private final List<StreamElement> allOutputs = new ArrayList<>();
      private long lastUpdated = currentTimeMillis();

      @Override
      public boolean isActive() {
        return currentTimeMillis() - lastUpdated < cleanupInterval;
      }

      @Override
      public String getTransactionId() {
        return delegate.getTransactionId();
      }

      @Override
      public void update(List<KeyAttribute> keyAttributes) throws TransactionRejectedException {
        lastUpdated = currentTimeMillis();
        delegate.update(keyAttributes);
        delegate.sync();
        if (delegate.getState() == Flags.ABORTED) {
          throw new TransactionRejectedException(getTransactionId(), Response.Flags.ABORTED) {};
        }
        if (delegate.getState() == Flags.COMMITTED) {
          // committed after update is only possible for duplicates
          throw new TransactionRejectedException(getTransactionId(), Response.Flags.DUPLICATE) {};
        }
      }

      @Override
      public void addOutputs(List<StreamElement> outputs) {
        lastUpdated = currentTimeMillis();
        allOutputs.addAll(outputs);
      }

      @Override
      public void rollback() {
        ExceptionUtils.unchecked(() -> delegate.rollback().get());
        close();
      }

      @Override
      public void commit(CommitCallback callback) throws TransactionRejectedException {
        BlockingQueue<Optional<Throwable>> err = new ArrayBlockingQueue<>(1);
        delegate.commitWrite(allOutputs, (succ, exc) -> err.add(Optional.ofNullable(exc)));
        close();
        try {
          Optional<Throwable> res = err.take();
          if (res.isEmpty()) {
            callback.commit(true, null);
          } else if (!(res.get() instanceof TransactionRejectedException)) {
            callback.commit(false, res.get());
          } else {
            throw (TransactionRejectedException) res.get();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          TransactionRejectedException exc =
              new TransactionRejectedException(getTransactionId(), Response.Flags.ABORTED) {};
          exc.addSuppressed(e);
          callback.commit(false, exc);
        }
      }

      @Override
      public void close() {
        openTransactions.remove(getTransactionId());
        delegate.close();
      }
    };
  }
}
