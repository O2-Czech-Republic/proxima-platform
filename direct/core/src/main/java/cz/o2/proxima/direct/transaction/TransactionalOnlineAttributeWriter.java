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
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;

/** A {@link OnlineAttributeWriter} that enforces transactions for each write. */
public class TransactionalOnlineAttributeWriter implements OnlineAttributeWriter {

  public static TransactionalOnlineAttributeWriter of(
      DirectDataOperator direct, OnlineAttributeWriter delegate) {

    return new TransactionalOnlineAttributeWriter(direct, delegate);
  }

  public class TransactionRejectedException extends Exception {
    private TransactionRejectedException(String transactionId) {
      super("Transaction " + transactionId + " rejected. Please restart the transaction.");
    }
  }

  public class Transaction implements AutoCloseable {

    @Getter private final String transactionId;
    private final BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(1);
    private State.Flags state;
    private long sequenceId = -1L;
    private boolean needsRollback = false;

    private Transaction(String transactionId) {
      this.transactionId = transactionId;
      this.state = State.Flags.UNKNOWN;
    }

    public void update(List<KeyAttribute> addedInputs) throws TransactionRejectedException {
      final Response.Flags expectedResponse;
      switch (state) {
        case UNKNOWN:
          manager.begin(
              transactionId,
              ExceptionUtils.uncheckedBiConsumer(this::enqueueResponse),
              addedInputs);
          needsRollback = true;
          expectedResponse = Response.Flags.OPEN;
          break;
        case OPEN:
          manager.updateTransaction(transactionId, addedInputs);
          expectedResponse = Response.Flags.UPDATED;
          break;
        default:
          throw new TransactionRejectedException(transactionId);
      }
      Response response = ExceptionUtils.uncheckedFactory(responseQueue::take);
      if (response.getFlags() != expectedResponse) {
        throw new TransactionRejectedException(transactionId);
      }
      if (response.hasSequenceId()) {
        Preconditions.checkState(
            sequenceId == -1 || sequenceId == response.getSeqId(),
            "Updated sequence ID. That is a bug in proxima's transactions.");
        sequenceId = response.getSeqId();
      }
      state = State.Flags.OPEN;
    }

    public void commitWrite(List<StreamElement> outputs, CommitCallback callback)
        throws TransactionRejectedException {

      List<StreamElement> toWrite =
          outputs.stream().map(this::injectSequenceId).collect(Collectors.toList());
      List<KeyAttribute> keyAttributes =
          toWrite.stream().map(KeyAttributes::ofStreamElement).collect(Collectors.toList());
      manager.commit(transactionId, keyAttributes);
      Response response = ExceptionUtils.uncheckedFactory(responseQueue::take);
      if (response.getFlags() != Response.Flags.COMMITTED) {
        if (response.getFlags() == Response.Flags.ABORTED) {
          // already aborted
          needsRollback = false;
        }
        throw new TransactionRejectedException(transactionId);
      }
      CommitCallback compositeCallback = CommitCallback.afterNumCommits(toWrite.size(), callback);
      state = State.Flags.COMMITTED;
      toWrite.forEach(w -> delegate.write(w, compositeCallback));
    }

    private void enqueueResponse(String responseId, Response response) {
      ExceptionUtils.unchecked(() -> responseQueue.put(response));
    }

    private StreamElement injectSequenceId(StreamElement in) {

      Preconditions.checkArgument(!in.isDeleteWildcard(), "Wildcard deletes not yet supported");

      return StreamElement.upsert(
          in.getEntityDescriptor(),
          in.getAttributeDescriptor(),
          sequenceId,
          in.getKey(),
          in.getAttribute(),
          in.getStamp(),
          in.getValue());
    }

    @Override
    public void close() {
      if (needsRollback && state != State.Flags.COMMITTED) {
        rollback();
      }
    }

    public void rollback() {
      manager.rollback(transactionId);
    }
  }

  private final OnlineAttributeWriter delegate;
  private final ClientTransactionManager manager;

  private TransactionalOnlineAttributeWriter(
      DirectDataOperator direct, OnlineAttributeWriter delegate) {

    this.delegate = delegate;
    this.manager = TransactionResourceManager.of(direct);
  }

  @Override
  public URI getUri() {
    return delegate.getUri();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    // this means start transaction with the single KeyAttribute as input and then
    // commit that transaction right away
    try (Transaction t = begin()) {
      @Nullable
      String suffix =
          data.getAttributeDescriptor().isWildcard()
              ? data.getAttribute()
                  .substring(data.getAttributeDescriptor().toAttributePrefix().length())
              : null;
      KeyAttribute zeroInput =
          KeyAttributes.ofAttributeDescriptor(
              data.getEntityDescriptor(),
              data.getKey(),
              data.getAttributeDescriptor(),
              Long.MAX_VALUE,
              suffix);
      t.update(Collections.singletonList(zeroInput));
      t.commitWrite(Collections.singletonList(data), statusCallback);
    } catch (TransactionRejectedException e) {
      statusCallback.commit(false, e);
    }
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
  public TransactionalOnlineAttributeWriter toTransactional() {
    return this;
  }

  public Transaction begin() {
    String transactionId = UUID.randomUUID().toString();
    return new Transaction(transactionId);
  }
}
