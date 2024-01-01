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

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.TransactionMode;
import cz.o2.proxima.core.repository.TransformationDescriptor;
import cz.o2.proxima.core.repository.TransformationDescriptor.InputTransactionMode;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Commit;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.KeyAttributes;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.Response.Flags;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.transform.ElementWiseTransformation;
import cz.o2.proxima.core.transform.ElementWiseTransformation.Collector;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.transform.DirectElementWiseTransform;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** A {@link OnlineAttributeWriter} that enforces transactions for each write. */
@Slf4j
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

  /** Interface for a transformation to get access to {@link Transaction}. */
  public interface TransactionAware {
    Transaction currentTransaction();

    void setTransaction(Transaction transaction);
  }

  /**
   * Base class for enforcing constraints on outputs of transaction (e.g. unique constraints).
   * Extend this class to do any application-specific validation of to-be-committed outputs of a
   * transaction.
   */
  public abstract static class TransactionValidator
      implements TransactionAware, DirectElementWiseTransform {

    transient Transaction transaction;

    @Override
    public final Transaction currentTransaction() {
      return Objects.requireNonNull(transaction);
    }

    @Override
    public void setTransaction(Transaction transaction) {
      this.transaction = transaction;
    }

    @Override
    public final void transform(StreamElement input, CommitCallback commit)
        throws TransactionRejectedRuntimeException {

      TransactionRejectedException caught = null;
      for (int i = 0; ; i++) {
        try {
          validate(input, currentTransaction());
          break;
        } catch (TransactionRejectedException ex) {
          if (caught == null) {
            caught = ex;
          } else {
            caught.addSuppressed(ex);
          }
          if (i == 4) {
            // this needs to be delegated to the caller
            throw new TransactionRejectedRuntimeException(caught);
          }
        }
      }
    }

    /**
     * Validate the input element. Use provided {@link Transaction} to add new inputs (if any). MUST
     * NOT call {@link Transaction#commitWrite}.
     *
     * @param element the input stream element to transform
     * @throws TransactionPreconditionFailedException if any precondition for a transaction fails.
     */
    public abstract void validate(StreamElement element, Transaction transaction)
        throws TransactionPreconditionFailedException, TransactionRejectedException;
  }

  public static class TransactionPreconditionFailedException extends RuntimeException {
    public TransactionPreconditionFailedException(String message) {
      super(message);
    }
  }

  public static class TransactionRejectedException extends Exception {
    @Getter private final String transactionId;
    @Getter private final Response.Flags responseFlags;

    protected TransactionRejectedException(String transactionId, Response.Flags flags) {
      super("Transaction " + transactionId + " rejected. Please restart the transaction.");
      this.transactionId = transactionId;
      this.responseFlags = flags;
    }
  }

  /**
   * This exception might be thrown in place of {@link TransactionRejectedException} where from
   * {@link TransactionAware} transformations where the {@link
   * ElementWiseTransformation#apply(StreamElement, Collector)} does not allow for throwing checked
   * exceptions directly.
   */
  public static class TransactionRejectedRuntimeException extends RuntimeException {
    public TransactionRejectedRuntimeException(TransactionRejectedException wrap) {
      super(wrap.getMessage(), wrap);
    }
  }

  public class Transaction implements AutoCloseable {

    @Getter private final String transactionId;
    private final BlockingQueue<Pair<String, Response>> responseQueue =
        new ArrayBlockingQueue<>(100);
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
          throw new TransactionRejectedException(transactionId, Flags.ABORTED);
      }
      Response response = takeResponse();
      if (response.getFlags() != expectedResponse) {
        throw new TransactionRejectedException(transactionId, response.getFlags());
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
      Collection<StreamElement> transformed;
      try {
        transformed = applyTransforms(injected);
      } catch (TransactionRejectedRuntimeException ex) {
        throw (TransactionRejectedException) ex.getCause();
      }
      StreamElement toWrite = getCommit(transformed);
      OnlineAttributeWriter writer = commitDelegate;
      List<KeyAttribute> keyAttributes =
          transformed.stream().map(KeyAttributes::ofStreamElement).collect(Collectors.toList());
      boolean anyDisallowed = keyAttributes.stream().anyMatch(KeyAttribute::isWildcardQuery);
      if (anyDisallowed) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid output writes %s. Wildcards need to have suffixes.",
                keyAttributes.stream()
                    .filter(KeyAttribute::isWildcardQuery)
                    .collect(Collectors.toList())));
      }
      manager.commit(transactionId, keyAttributes);
      Response response = takeResponse();
      if (response.getFlags() != Response.Flags.COMMITTED) {
        if (response.getFlags() == Response.Flags.ABORTED) {
          state = State.Flags.ABORTED;
        }
        throw new TransactionRejectedException(transactionId, response.getFlags());
      }
      CommitCallback compositeCallback =
          (succ, exc) -> {
            if (!succ) {
              rollback();
            }
            log.debug(
                "Committed outputs {} (via {}) of transaction {}: ({}, {})",
                transformed,
                toWrite,
                transactionId,
                succ,
                (Object) exc);
            callback.commit(succ, exc);
          };
      state = State.Flags.COMMITTED;
      writer.write(toWrite, compositeCallback);
    }

    private Response takeResponse() {
      return Optional.ofNullable(
              ExceptionUtils.uncheckedFactory(() -> responseQueue.poll(5, TimeUnit.SECONDS)))
          .map(Pair::getSecond)
          .orElse(Response.empty());
    }

    private Collection<StreamElement> applyTransforms(List<StreamElement> outputs) {
      Set<StreamElement> elements = new HashSet<>();
      List<StreamElement> currentElements = outputs;
      do {
        List<StreamElement> newElements = new ArrayList<>();
        for (StreamElement el : currentElements) {
          if (elements.add(el)) {
            List<TransformationDescriptor> applicableTransforms =
                attributeTransforms.get(el.getAttributeDescriptor());
            if (applicableTransforms != null) {
              applicableTransforms.stream()
                  .filter(t -> !(t.getTransformation() instanceof TransactionValidator))
                  .filter(t -> t.getFilter().apply(el))
                  .forEach(td -> applyTransform(newElements, el, td));
            }
          }
        }
        currentElements = newElements;
      } while (!currentElements.isEmpty());
      applyValidations(elements);
      return elements;
    }

    private void applyValidations(Set<StreamElement> elements) {
      for (StreamElement el : elements) {
        List<TransformationDescriptor> applicableTransforms =
            attributeTransforms.get(el.getAttributeDescriptor());
        if (applicableTransforms != null) {
          applicableTransforms.stream()
              .filter(t -> t.getTransformation() instanceof TransactionValidator)
              .filter(t -> t.getFilter().apply(el))
              .forEach(td -> applyTransform(Collections.emptyList(), el, td));
        }
      }
    }

    private void applyTransform(
        List<StreamElement> newElements, StreamElement el, TransformationDescriptor td) {

      if (td.getTransformation() instanceof TransactionValidator) {
        TransactionValidator transform = (TransactionValidator) td.getTransformation();
        transform.setTransaction(this);
        transform.transform(el, CommitCallback.noop());
      } else {
        ElementWiseTransformation transform = td.getTransformation().asElementWiseTransform();
        if (transform instanceof TransactionAware) {
          ((TransactionAware) transform).setTransaction(this);
        }
        int currentSize = newElements.size();
        int add =
            transform.apply(
                el, transformed -> newElements.add(injectSequenceIdAndStamp(transformed)));
        Preconditions.checkState(
            newElements.size() == currentSize + add,
            "Transformation %s is asynchronous which not currently supported in transaction mode.",
            transform.getClass());
      }
    }

    private StreamElement getCommit(Collection<StreamElement> outputs) {
      return manager
          .getCommitDesc()
          .upsert(transactionId, stamp, Commit.of(sequenceId, stamp, outputs));
    }

    private void enqueueResponse(String responseId, Response response) {
      ExceptionUtils.unchecked(() -> responseQueue.put(Pair.of(responseId, response)));
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
  private final Map<AttributeDescriptor<?>, List<TransformationDescriptor>> attributeTransforms;

  private TransactionalOnlineAttributeWriter(
      DirectDataOperator direct, OnlineAttributeWriter delegate) {

    this.delegate = delegate;
    this.manager = direct.getClientTransactionManager();
    this.executor = direct.getContext().getExecutorService();
    this.commitDelegate = Optionals.get(direct.getWriter(manager.getCommitDesc()));
    // FIXME: this should be removed and resolved automatically as empty input
    // to be fixed in https://github.com/O2-Czech-Republic/proxima-platform/issues/216
    this.globalKeyAttributes = getAttributesWithGlobalTransactionMode(direct);
    attributeTransforms =
        direct.getRepository().getTransformations().values().stream()
            .filter(d -> d.getInputTransactionMode() == InputTransactionMode.TRANSACTIONAL)
            .flatMap(t -> t.getAttributes().stream().map(a -> Pair.of(a, t)))
            .collect(
                Collectors.groupingBy(
                    Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList())));
  }

  private List<KeyAttribute> getAttributesWithGlobalTransactionMode(DirectDataOperator direct) {
    return direct
        .getRepository()
        .getAllEntities()
        .filter(EntityDescriptor::isTransactional)
        .flatMap(
            e ->
                e.getAllAttributes().stream()
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
          } catch (Throwable e) {
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
    return begin(UUID.randomUUID().toString());
  }

  public Transaction begin(String transactionId) {
    return new Transaction(transactionId);
  }
}
