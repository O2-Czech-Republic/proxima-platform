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
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.TransactionMode;
import cz.o2.proxima.core.repository.TransformationDescriptor;
import cz.o2.proxima.core.repository.TransformationDescriptor.InputTransactionMode;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.time.Watermarks;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
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

    private transient ExecutorService executor;
    private transient Transaction transaction;

    @Override
    public void setup(
        Repository repo, DirectDataOperator directDataOperator, Map<String, Object> cfg) {

      this.executor = Objects.requireNonNull(directDataOperator.getContext().getExecutorService());
    }

    @Override
    public final Transaction currentTransaction() {
      return Objects.requireNonNull(transaction);
    }

    @Override
    public final void setTransaction(Transaction transaction) {
      this.transaction = transaction;
    }

    @Override
    public final void transform(StreamElement input, CommitCallback commit) {
      executor.submit(
          () -> {
            try {
              validate(input, currentTransaction());
              commit.commit(true, null);
            } catch (Throwable err) {
              commit.commit(false, err);
            }
          });
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

  @Getter
  public static class TransactionRejectedException extends Exception {
    private final String transactionId;
    private final Response.Flags responseFlags;

    protected TransactionRejectedException(String transactionId, Response.Flags flags) {
      super(
          String.format(
              "Transaction %s rejected with flags %s. Please restart the transaction.",
              transactionId, flags));
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
    private boolean commitAttempted = false;
    @Getter private State.Flags state;
    private long sequenceId = -1L;
    private long stamp = Long.MIN_VALUE;
    private final List<CompletableFuture<?>> runningUpdates =
        Collections.synchronizedList(new ArrayList<>());
    private final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());

    private Transaction(String transactionId) {
      this.transactionId = transactionId;
      this.state = State.Flags.UNKNOWN;
    }

    void beginGlobal() {
      Preconditions.checkArgument(
          !globalKeyAttributes.isEmpty(), "Cannot resolve global transactional attributes.");
      update(globalKeyAttributes);
    }

    public void update(List<KeyAttribute> addedInputs) {
      final CompletableFuture<Response> future;
      switch (state) {
        case UNKNOWN:
          future = manager.begin(transactionId, addedInputs);
          break;
        case OPEN:
          future = manager.updateTransaction(transactionId, addedInputs);
          break;
        default:
          future =
              CompletableFuture.failedFuture(
                  new TransactionRejectedException(transactionId, Flags.ABORTED));
      }
      runningUpdates.add(future);
      future.whenComplete((response, exc) -> processArrivedResponse(future, response, exc));
    }

    public void commitWrite(List<StreamElement> outputs, CommitCallback callback) {
      // FIXME: this should be removed once we write through coordinator
      final CommitContext commitContext = new CommitContext(outputs);
      this.commitAttempted = true;
      waitForInflight()
          .thenCompose(ign -> runTransforms(commitContext))
          // need to wait for any requests added during transforms
          .thenCompose(ign -> waitForInflight())
          .thenCompose(ign -> sendCommitRequest(commitContext))
          .thenCompose(response -> handleCommitResponse(response, commitContext))
          .whenComplete(
              (ign, err) -> {
                manager.release(transactionId);
                if (err instanceof CompletionException) {
                  callback.commit(false, err.getCause());
                } else {
                  callback.commit(err == null, err);
                }
              });
    }

    private CompletableFuture<?> waitForInflight() {
      final CompletableFuture<?> unfinished;
      synchronized (runningUpdates) {
        unfinished = CompletableFuture.allOf(runningUpdates.toArray(new CompletableFuture<?>[] {}));
      }
      return unfinished;
    }

    private CompletableFuture<?> runTransforms(CommitContext context) {
      if (state != State.Flags.OPEN) {
        return CompletableFuture.failedFuture(
            new TransactionRejectedException(
                transactionId, state == State.Flags.COMMITTED ? Flags.DUPLICATE : Flags.ABORTED));
      }
      List<StreamElement> injected =
          context.getOutputs().stream()
              .map(this::injectSequenceIdAndStamp)
              .collect(Collectors.toList());
      CompletableFuture<?> transformedFuture = applyTransforms(injected, context);
      runningUpdates.add(transformedFuture);
      CompletableFuture<StreamElement> commitFuture =
          transformedFuture.thenCompose(
              ign ->
                  CompletableFuture.completedFuture(getCommit(context.getTransformedElements())));
      context.setToWriteFuture(commitFuture);
      return commitFuture;
    }

    private CompletableFuture<Response> sendCommitRequest(CommitContext context) {
      if (state != State.Flags.OPEN) {
        return CompletableFuture.completedFuture(Response.empty().aborted());
      }
      Collection<StreamElement> transformed = context.getTransformedElements();
      List<KeyAttribute> keyAttributes =
          transformed.stream().map(KeyAttributes::ofStreamElement).collect(Collectors.toList());
      return manager
          .commit(transactionId, keyAttributes)
          .completeOnTimeout(
              Response.empty().aborted(),
              manager.getCfg().getTransactionTimeoutMs(),
              TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> handleCommitResponse(Response response, CommitContext context) {
      if (response.getFlags() != Flags.COMMITTED) {
        if (response.getFlags() == Flags.ABORTED) {
          state = State.Flags.ABORTED;
        }
        return CompletableFuture.failedFuture(
            new TransactionRejectedException(transactionId, response.getFlags()));
      }
      Collection<StreamElement> transformed = context.getTransformedElements();
      StreamElement toWrite = context.getToWrite();
      CompletableFuture<Void> result = new CompletableFuture<>();
      CommitCallback compositeCallback =
          (succ, exc) -> {
            if (!succ) {
              // FIXME: will be unnecessary on coordinator write
              rollback();
            }
            log.debug(
                "Committed outputs {} (via {}) of transaction {}: ({}, {})",
                transformed,
                toWrite,
                transactionId,
                succ,
                (Object) exc);
            if (exc != null) {
              result.completeExceptionally(exc);
            } else {
              result.complete(null);
            }
          };
      state = State.Flags.COMMITTED;
      commitDelegate.write(toWrite, compositeCallback);
      return result;
    }

    private void processArrivedResponse(
        CompletableFuture<?> finished, @Nullable Response response, @Nullable Throwable err) {

      Preconditions.checkState(runningUpdates.remove(finished));
      if (err != null) {
        TransactionRejectedException thrown =
            new TransactionRejectedException(transactionId, Flags.ABORTED);
        thrown.addSuppressed(err);
        exceptions.add(thrown);
        state = State.Flags.ABORTED;
      }
      if (state == State.Flags.ABORTED || state == State.Flags.COMMITTED) {
        return;
      }
      Objects.requireNonNull(response);
      if (response.getFlags() == Flags.ABORTED || response.getFlags() == Flags.DUPLICATE) {
        if (response.getFlags() == Flags.ABORTED) {
          state = State.Flags.ABORTED;
          exceptions.add(new TransactionRejectedException(transactionId, response.getFlags()));
        } else {
          state = State.Flags.COMMITTED;
        }
        return;
      }
      if (state == State.Flags.UNKNOWN
          && (response.getFlags() == Flags.UPDATED || response.getFlags() == Flags.OPEN)) {
        state = State.Flags.OPEN;
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
    }

    private CompletableFuture<Void> applyTransforms(
        List<StreamElement> outputs, CommitContext context) {

      Set<StreamElement> elements = new HashSet<>();
      List<StreamElement> currentElements = outputs;
      List<CompletableFuture<?>> futures = new ArrayList<>();
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
                  .forEach(td -> futures.add(applyTransform(newElements, el, td)));
            }
          }
        }
        currentElements = newElements;
      } while (!currentElements.isEmpty());
      futures.addAll(applyValidations(elements));
      context.setTransformedElements(elements);
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[] {}));
    }

    private List<CompletableFuture<?>> applyValidations(Set<StreamElement> elements) {
      for (StreamElement el : elements) {
        List<TransformationDescriptor> applicableTransforms =
            attributeTransforms.get(el.getAttributeDescriptor());
        if (applicableTransforms != null) {
          return applicableTransforms.stream()
              .filter(t -> t.getTransformation() instanceof TransactionValidator)
              .filter(t -> t.getFilter().apply(el))
              .map(td -> applyTransform(Collections.emptyList(), el, td))
              .collect(Collectors.toList());
        }
      }
      return Collections.emptyList();
    }

    private CompletableFuture<Void> applyTransform(
        List<StreamElement> newElements, StreamElement el, TransformationDescriptor td) {

      if (td.getTransformation() instanceof TransactionValidator) {
        TransactionValidator transform = (TransactionValidator) td.getTransformation();
        transform.setTransaction(this);
        CompletableFuture<Void> res = new CompletableFuture<>();
        transform.transform(
            el,
            (succ, exc) -> {
              if (exc != null) {
                res.completeExceptionally(exc);
              } else {
                res.complete(null);
              }
            });
        return res;
      }
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
      return CompletableFuture.completedFuture(null);
    }

    private StreamElement getCommit(Collection<StreamElement> outputs) {
      return manager
          .getCommitDesc()
          .upsert(transactionId, stamp, Commit.of(sequenceId, stamp, outputs));
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
      if (state == State.Flags.OPEN && !commitAttempted) {
        rollback();
      }
    }

    public CompletableFuture<Void> rollback() {
      return manager.rollback(transactionId).thenAccept(ign -> manager.release(transactionId));
    }

    public void sync() throws TransactionRejectedException {
      try {
        waitForInflight().get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TransactionRejectedException(getTransactionId(), Flags.ABORTED);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TransactionRejectedException) {
          throw (TransactionRejectedException) e.getCause();
        }
        TransactionRejectedException exc =
            new TransactionRejectedException(getTransactionId(), Flags.ABORTED);
        exc.addSuppressed(e);
        throw exc;
      }
    }

    private class CommitContext {

      @Getter private final List<StreamElement> outputs;
      @Setter CompletableFuture<StreamElement> toWriteFuture;
      @Setter @Getter Collection<StreamElement> transformedElements;

      public CommitContext(List<StreamElement> outputs) {
        this.outputs = outputs;
      }

      public StreamElement getToWrite() {
        return ExceptionUtils.uncheckedFactory(toWriteFuture::get);
      }
    }
  }

  @Getter private final OnlineAttributeWriter delegate;
  private final OnlineAttributeWriter commitDelegate;
  private final ClientTransactionManager manager;
  private final List<KeyAttribute> globalKeyAttributes;
  private final Map<AttributeDescriptor<?>, List<TransformationDescriptor>> attributeTransforms;

  private TransactionalOnlineAttributeWriter(
      DirectDataOperator direct, OnlineAttributeWriter delegate) {

    this.delegate = delegate;
    this.manager = direct.getClientTransactionManager();
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
  }

  @Override
  public synchronized void write(StreamElement data, CommitCallback statusCallback) {
    // this means start transaction with the single KeyAttribute as input and then
    // commit that transaction right away
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
              Watermarks.MAX_WATERMARK,
              suffix);
      t.update(Collections.singletonList(outputKeyAttribute));
      t.commitWrite(Collections.singletonList(data), statusCallback);
    } catch (Throwable e) {
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
