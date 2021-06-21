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

import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.TransactionMode;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Commit;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.Pair;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;

/** A {@link OnlineAttributeWriter} that enforces transactions for each write. */
public class TransactionalOnlineAttributeWriter implements OnlineAttributeWriter {

  public static TransactionalOnlineAttributeWriter of(
      DirectDataOperator direct, OnlineAttributeWriter delegate) {

    return new TransactionalOnlineAttributeWriter(direct, delegate);
  }

  public static TransactionalOnlineAttributeWriter global(DirectDataOperator direct) {
    ClientTransactionManager manager = direct.getClientTransactionManager();
    // write each output to the _transaction.commit, even if there is single output
    return new TransactionalOnlineAttributeWriter(
        direct, Optionals.get(direct.getWriter(manager.getCommitDesc()))) {

      @Override
      public Transaction begin() {
        Transaction ret = super.begin();
        // this should never throw TransactionRejectedException
        ExceptionUtils.unchecked(ret::beginGlobal);
        return ret;
      }
    };
  }

  public class TransactionRejectedException extends Exception {
    @Getter private final String transactionId;

    private TransactionRejectedException(String transactionId) {
      super("Transaction " + transactionId + " rejected. Please restart the transaction.");
      this.transactionId = transactionId;
    }
  }

  public class Transaction implements AutoCloseable {

    @Getter private final String transactionId;
    private final BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(100);
    private boolean isGlobalTransaction;
    private State.Flags state;
    private long sequenceId = -1L;
    private long stamp = Long.MIN_VALUE;

    private Transaction(String transactionId) {
      this.transactionId = transactionId;
      this.state = State.Flags.UNKNOWN;
    }

    void beginGlobal() throws TransactionRejectedException {
      Preconditions.checkArgument(
          !globalKeyAttributes.isEmpty(), "Cannot resolve global transactional attributes.");
      this.isGlobalTransaction = true;
      update(globalKeyAttributes);
    }

    public void update(List<KeyAttribute> addedInputs) throws TransactionRejectedException {
      final Response.Flags expectedResponse;
      switch (state) {
        case UNKNOWN:
          manager.begin(
              transactionId,
              ExceptionUtils.uncheckedBiConsumer(this::enqueueResponse),
              addedInputs);
          expectedResponse = Response.Flags.OPEN;
          break;
        case OPEN:
          manager.updateTransaction(transactionId, addedInputs);
          expectedResponse = Response.Flags.UPDATED;
          break;
        default:
          throw new TransactionRejectedException(transactionId);
      }
      Response response =
          Optional.ofNullable(
                  ExceptionUtils.uncheckedFactory(() -> responseQueue.poll(5, TimeUnit.SECONDS)))
              .orElse(Response.empty());

      if (response.getFlags() != expectedResponse) {
        throw new TransactionRejectedException(transactionId);
      }
      if (response.hasSequenceId()) {
        Preconditions.checkState(
            sequenceId == -1 || sequenceId == response.getSeqId(),
            "Updated sequence ID from %s to %s. That is a bug in proxima's transactions.",
            sequenceId,
            response.getSeqId());
        sequenceId = response.getSeqId();
      }
      if (response.hasStamp()) {
        Preconditions.checkState(
            stamp == Long.MIN_VALUE || stamp == response.getStamp(),
            "Updated stamp from %s to %s. That is a bug in proxima's transactions.",
            stamp,
            response.getStamp());
        stamp = response.getStamp();
      }
      state = State.Flags.OPEN;
    }

    public void commitWrite(List<StreamElement> outputs, CommitCallback callback)
        throws TransactionRejectedException {

      List<StreamElement> injected =
          outputs.stream().map(this::injectSequenceIdAndStamp).collect(Collectors.toList());
      StreamElement toWrite = getSingleOrCommit(injected);
      OnlineAttributeWriter writer =
          outputs.size() == 1 && !isGlobalTransaction ? delegate : commitDelegate;
      List<KeyAttribute> keyAttributes =
          injected.stream().map(KeyAttributes::ofStreamElement).collect(Collectors.toList());
      manager.commit(transactionId, keyAttributes);
      Response response = ExceptionUtils.uncheckedFactory(responseQueue::take);
      if (response.getFlags() != Response.Flags.COMMITTED) {
        if (response.getFlags() == Response.Flags.ABORTED) {
          state = State.Flags.ABORTED;
        }
        throw new TransactionRejectedException(transactionId);
      }
      CommitCallback compositeCallback =
          (succ, exc) -> {
            if (!succ) {
              rollback();
            }
            callback.commit(succ, exc);
          };
      state = State.Flags.COMMITTED;
      writer.write(toWrite, compositeCallback);
    }

    private StreamElement getSingleOrCommit(List<StreamElement> outputs) {
      if (outputs.size() == 1 && !isGlobalTransaction) {
        return outputs.get(0);
      }
      return manager
          .getCommitDesc()
          .upsert(transactionId, stamp, Commit.of(sequenceId, stamp, outputs));
    }

    private void enqueueResponse(String responseId, Response response) {
      ExceptionUtils.unchecked(() -> responseQueue.put(response));
    }

    private StreamElement injectSequenceIdAndStamp(StreamElement in) {

      Preconditions.checkArgument(!in.isDeleteWildcard(), "Wildcard deletes not yet supported");

      return StreamElement.upsert(
          in.getEntityDescriptor(),
          in.getAttributeDescriptor(),
          sequenceId,
          in.getKey(),
          in.getAttribute(),
          stamp,
          in.getValue());
    }

    @Override
    public void close() {
      if (state == State.Flags.OPEN) {
        rollback();
      }
      manager.release(transactionId);
    }

    public void rollback() {
      manager.rollback(transactionId);
    }
  }

  @Getter private final OnlineAttributeWriter delegate;
  private final OnlineAttributeWriter commitDelegate;
  private final ClientTransactionManager manager;
  private final ExecutorService executor;
  private final List<KeyAttribute> globalKeyAttributes;

  private TransactionalOnlineAttributeWriter(
      DirectDataOperator direct, OnlineAttributeWriter delegate) {

    this.delegate = delegate;
    this.manager = direct.getClientTransactionManager();
    this.executor = direct.getContext().getExecutorService();
    this.commitDelegate = Optionals.get(direct.getWriter(manager.getCommitDesc()));
    // FIXME: this should be removed and resolved automatically as empty input
    // to be fixed in https://github.com/O2-Czech-Republic/proxima-platform/issues/216
    this.globalKeyAttributes = getAttributesWithGlobalTransactionMode(direct);
  }

  private List<KeyAttribute> getAttributesWithGlobalTransactionMode(DirectDataOperator direct) {
    return direct
        .getRepository()
        .getAllEntities()
        .filter(EntityDescriptor::isTransactional)
        .flatMap(
            e ->
                e.getAllAttributes()
                    .stream()
                    .filter(a -> a.getTransactionMode() == TransactionMode.ALL)
                    .map(a -> Pair.of(e, a)))
        .map(
            p ->
                p.getSecond().isWildcard()
                    ? KeyAttributes.ofAttributeDescriptor(
                        p.getFirst(),
                        "dummy-" + p.getSecond().hashCode(),
                        p.getSecond(),
                        Long.MAX_VALUE,
                        String.valueOf(p.getFirst().hashCode()))
                    : KeyAttributes.ofAttributeDescriptor(
                        p.getFirst(),
                        "dummy-" + p.getSecond().hashCode(),
                        p.getSecond(),
                        Long.MAX_VALUE))
        .collect(Collectors.toList());
  }

  @Override
  public URI getUri() {
    return delegate.getUri();
  }

  @Override
  public synchronized void close() {
    manager.close();
    delegate.close();
    commitDelegate.close();
  }

  @Override
  public synchronized void write(StreamElement data, CommitCallback statusCallback) {
    // this means start transaction with the single KeyAttribute as input and then
    // commit that transaction right away
    executor.execute(
        () -> {
          try (Transaction t = begin()) {
            @Nullable
            String suffix =
                data.getAttributeDescriptor().isWildcard()
                    ? data.getAttribute()
                        .substring(data.getAttributeDescriptor().toAttributePrefix().length())
                    : null;
            KeyAttribute outputKeyAttribute =
                KeyAttributes.ofAttributeDescriptor(
                    data.getEntityDescriptor(),
                    data.getKey(),
                    data.getAttributeDescriptor(),
                    Long.MAX_VALUE,
                    suffix);
            t.update(Collections.singletonList(outputKeyAttribute));
            t.commitWrite(Collections.singletonList(data), statusCallback);
          } catch (TransactionRejectedException e) {
            statusCallback.commit(false, e);
          }
        });
  }

  @Override
  public Factory<? extends OnlineAttributeWriter> asFactory() {
    Factory<? extends OnlineAttributeWriter> delegateFactory = delegate.asFactory();
    return repo ->
        new TransactionalOnlineAttributeWriter(
            repo.getOrCreateOperator(DirectDataOperator.class), delegateFactory.apply(repo));
  }

  @Override
  public boolean isTransactional() {
    return true;
  }

  @Override
  public TransactionalOnlineAttributeWriter transactional() {
    return this;
  }

  public Transaction begin() {
    String transactionId = UUID.randomUUID().toString();
    return new Transaction(transactionId);
  }
}
