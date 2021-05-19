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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObservers;
import cz.o2.proxima.direct.commitlog.LogObservers.ForwardingObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.TransactionMode;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Commit;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Request.Flags;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.Pair;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manager of open transactional resources - e.g. writers, commit-log readers, etc.
 *
 * <p>The implementation is thread-safe in the sense it is okay to access this class from multiple
 * threads, <b>with different transactions</b>. The same transaction <b>must</b> be processed from
 * single thread only, otherwise the behavior is undefined.
 */
@Internal
@ThreadSafe
@Slf4j
class TransactionResourceManager implements ClientTransactionManager, ServerTransactionManager {

  public static TransactionResourceManager of(DirectDataOperator direct) {
    return new TransactionResourceManager(direct);
  }

  private class CachedWriters implements AutoCloseable {

    private final DirectAttributeFamilyDescriptor family;
    @Nullable OnlineAttributeWriter requestWriter;
    @Nullable OnlineAttributeWriter responseWriter;
    @Nullable CachedView stateView;

    CachedWriters(DirectAttributeFamilyDescriptor family) {
      this.family = family;
    }

    @Override
    public void close() {
      Optional.ofNullable(requestWriter).ifPresent(OnlineAttributeWriter::close);
      Optional.ofNullable(responseWriter).ifPresent(OnlineAttributeWriter::close);
      Optional.ofNullable(stateView).ifPresent(CachedView::close);
    }

    public OnlineAttributeWriter getOrCreateRequestWriter() {
      if (requestWriter == null) {
        requestWriter = Optionals.get(family.getWriter()).online();
      }
      return requestWriter;
    }

    public OnlineAttributeWriter getOrCreateResponseWriter() {
      if (responseWriter == null) {
        responseWriter = Optionals.get(family.getWriter()).online();
      }
      return responseWriter;
    }

    public CachedView getOrCreateStateView() {
      if (stateView == null) {
        stateView = stateViews.get(family);
        Preconditions.checkState(
            stateViews != null, "StateView not initialized for family %s", family);
      }
      return stateView;
    }
  }

  @VisibleForTesting
  class CachedTransaction implements AutoCloseable {

    @Getter final String transactionId;
    final Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor> attributeToFamily =
        new HashMap<>();
    final Thread owningThread = Thread.currentThread();
    final @Nullable BiConsumer<String, Response> responseConsumer;
    @Nullable OnlineAttributeWriter requestWriter;
    @Nullable OnlineAttributeWriter responseWriter;
    @Nullable CachedView stateView;

    CachedTransaction(
        String transactionId,
        Collection<KeyAttribute> attributes,
        @Nullable BiConsumer<String, Response> responseConsumer) {

      this.transactionId = transactionId;
      attributeToFamily.putAll(
          findFamilyForTransactionalAttribute(
              attributes
                  .stream()
                  .map(KeyAttribute::getAttributeDescriptor)
                  .collect(Collectors.toList())));
      this.responseConsumer = responseConsumer;
    }

    void open(List<KeyAttribute> inputAttrs) {
      checkThread();
      Preconditions.checkState(responseConsumer != null);
      addTransactionResponseConsumer(
          transactionId, attributeToFamily.get(requestDesc), responseConsumer);
      sendRequest(Request.builder().flags(Flags.OPEN).inputAttributes(inputAttrs).build(), "open");
    }

    public void commit(List<KeyAttribute> outputAttributes) {
      checkThread();
      sendRequest(
          Request.builder().flags(Request.Flags.COMMIT).outputAttributes(outputAttributes).build(),
          "commit");
    }

    public void update(List<KeyAttribute> addedAttributes) {
      checkThread();
      sendRequest(
          Request.builder().flags(Request.Flags.UPDATE).outputAttributes(addedAttributes).build(),
          "update");
    }

    public void rollback() {
      checkThread();
      sendRequest(Request.builder().flags(Flags.ROLLBACK).build(), "rollback");
    }

    private void sendRequest(Request request, String requestId) {
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<Throwable> error = new AtomicReference<>();
      getRequestWriter()
          .write(
              requestDesc.upsert(transactionId, requestId, System.currentTimeMillis(), request),
              (succ, exc) -> {
                error.set(exc);
                latch.countDown();
              });
      ExceptionUtils.ignoringInterrupted(latch::await);
      if (error.get() != null) {
        throw new IllegalStateException(error.get());
      }
    }

    @Override
    public void close() {
      requestWriter = responseWriter = null;
      stateView = null;
    }

    OnlineAttributeWriter getRequestWriter() {
      checkThread();
      if (requestWriter == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(requestDesc);
        requestWriter = getCachedAccessors(family).getOrCreateRequestWriter();
      }
      return requestWriter;
    }

    OnlineAttributeWriter getResponseWriter() {
      checkThread();
      if (responseWriter == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(requestDesc);
        responseWriter = getCachedAccessors(family).getOrCreateResponseWriter();
      }
      return responseWriter;
    }

    CachedView getStateView() {
      if (stateView == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(requestDesc);
        stateView = getCachedAccessors(family).getOrCreateStateView();
      }
      return stateView;
    }

    private void checkThread() {
      Preconditions.checkState(
          owningThread == Thread.currentThread(),
          "Conflict in owning threads of transaction %s: %s and %s",
          transactionId,
          owningThread,
          Thread.currentThread());
    }

    public State getState() {
      return getStateView()
          .get(transactionId, stateDesc)
          .map(KeyValue::getParsedRequired)
          .orElse(State.empty());
    }
  }

  private final DirectDataOperator direct;
  @Getter private final EntityDescriptor transaction;
  @Getter private final Wildcard<Request> requestDesc;
  @Getter private final Wildcard<Response> responseDesc;
  @Getter private final Regular<State> stateDesc;
  @Getter private final Regular<Commit> commitDesc;
  private final Map<String, CachedTransaction> openTransactionMap = new ConcurrentHashMap<>();
  private final Map<AttributeFamilyDescriptor, CachedWriters> cachedAccessors =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, ObserveHandle> observedFamilies =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, CachedView> stateViews =
      new ConcurrentHashMap<>();
  private final Map<String, BiConsumer<String, Response>> transactionResponseConsumers =
      new ConcurrentHashMap<>();

  private TransactionResourceManager(DirectDataOperator direct) {
    this.direct = direct;
    this.transaction = direct.getRepository().getEntity("_transaction");
    this.requestDesc = Wildcard.of(transaction, transaction.getAttribute("request.*"));
    this.responseDesc = Wildcard.of(transaction, transaction.getAttribute("response.*"));
    this.stateDesc = Regular.of(transaction, transaction.getAttribute("state"));
    this.commitDesc = Regular.of(transaction, transaction.getAttribute("commit"));
  }

  @Override
  public synchronized void close() {
    openTransactionMap.forEach((k, v) -> v.close());
    cachedAccessors.forEach((k, v) -> v.close());
    openTransactionMap.clear();
    cachedAccessors.clear();
    transactionResponseConsumers.clear();
  }

  /**
   * Observe all transactional families with given observer.
   *
   * @param name name of the observer (will be appended with name of the family)
   * @param requestObserver the observer (need not be synchronized)
   */
  @Override
  public void runObservations(
      String name,
      BiConsumer<StreamElement, Pair<Long, Object>> updateConsumer,
      LogObserver requestObserver) {

    LogObserver synchronizedObserver = LogObservers.synchronizedObserver(requestObserver);
    direct
        .getRepository()
        .getAllFamilies(true)
        .filter(af -> af.getAttributes().contains(requestDesc))
        .map(
            af ->
                Pair.of(
                    af.getName(),
                    Optionals.get(direct.getFamilyByName(af.getName()).getCommitLogReader())))
        .forEach(
            r ->
                r.getSecond()
                    .observe(
                        name + "-" + r.getFirst(),
                        hookForRepartitions(r.getFirst(), updateConsumer, synchronizedObserver)));
  }

  private LogObserver hookForRepartitions(
      String family,
      BiConsumer<StreamElement, Pair<Long, Object>> updateConsumer,
      LogObserver delegate) {

    DirectAttributeFamilyDescriptor directFamily = direct.getFamilyByName(family);
    return new ForwardingObserver(delegate) {
      @Override
      public void onRepartition(OnRepartitionContext context) {
        super.onRepartition(context);
        CachedView view =
            stateViews.computeIfAbsent(directFamily, f -> Optionals.get(f.getCachedView()));
        Set<Integer> partitionIds =
            context.partitions().stream().map(Partition::getId).collect(Collectors.toSet());
        view.assign(
            view.getPartitions()
                .stream()
                .filter(p -> partitionIds.contains(p.getId()))
                .collect(Collectors.toList()),
            updateConsumer);
      }
    };
  }

  OnlineAttributeWriter getRequestWriter(String transactionId) {
    CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    Preconditions.checkState(
        cachedTransaction != null, "Transaction %s is not open", transactionId);
    return cachedTransaction.getRequestWriter();
  }

  private void addTransactionResponseConsumer(
      String transactionId,
      DirectAttributeFamilyDescriptor responseFamily,
      BiConsumer<String, Response> responseConsumer) {

    transactionResponseConsumers.put(transactionId, responseConsumer);
    observedFamilies.computeIfAbsent(
        responseFamily,
        k -> {
          log.debug("Starting to observe family {}", k);
          return Optionals.get(k.getCommitLogReader())
              .observe(
                  "transactionResponseObserver-" + k.getDesc().getName(),
                  newTransactionResponseObserver());
        });
  }

  private LogObserver newTransactionResponseObserver() {
    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        log.debug("Received transaction event {}", ingest);
        if (ingest.getAttributeDescriptor().equals(responseDesc)) {
          String transactionId = ingest.getKey();
          BiConsumer<String, Response> consumer = transactionResponseConsumers.get(transactionId);
          Optional<Response> response = responseDesc.valueOf(ingest);
          if (consumer != null) {
            if (response.isPresent()) {
              String suffix = responseDesc.extractSuffix(ingest.getAttribute());
              consumer.accept(suffix, response.get());
            } else {
              log.error("Failed to parse response from {}", ingest);
            }
          } else {
            log.warn(
                "Missing consumer for transaction {} processing response {}",
                transactionId,
                response.orElse(null));
          }
        }
        context.confirm();
        return true;
      }
    };
  }

  private synchronized CachedWriters getCachedAccessors(DirectAttributeFamilyDescriptor family) {
    return cachedAccessors.computeIfAbsent(family.getDesc(), k -> new CachedWriters(family));
  }

  /**
   * Initialize (possibly) new transaction. If the transaction already existed prior to this call,
   * its current state is returned, otherwise the transaction is opened.
   *
   * @param transactionId ID of transaction
   * @param responseConsumer consumer of responses related to the transaction
   * @param attributes attributes affected by this transaction (both input and output)
   * @return current state of the transaction
   */
  @Override
  public void begin(
      String transactionId,
      BiConsumer<String, Response> responseConsumer,
      List<KeyAttribute> attributes) {

    log.debug("Opening transaction {} with attributes {}", transactionId, attributes);
    CachedTransaction cachedTransaction =
        openTransactionMap.computeIfAbsent(
            transactionId, k -> new CachedTransaction(transactionId, attributes, responseConsumer));
    cachedTransaction.open(attributes);
  }

  /**
   * Update the transaction with additional attributes related to the transaction.
   *
   * @param transactionId ID of transaction
   * @param newAttributes attributes to be added to the transaction
   * @return
   */
  @Override
  public void updateTransaction(String transactionId, List<KeyAttribute> newAttributes) {
    @Nullable CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    Preconditions.checkArgument(
        cachedTransaction != null, "Transaction %s is not open", transactionId);
    cachedTransaction.update(newAttributes);
  }

  @Override
  public void commit(String transactionId, List<KeyAttribute> outputAttributes) {
    @Nullable CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    Preconditions.checkArgument(
        cachedTransaction != null, "Transaction %s is not open", transactionId);
    cachedTransaction.commit(outputAttributes);
  }

  @Override
  public void rollback(String transactionId) {
    CachedTransaction cachedTransaction =
        openTransactionMap.computeIfAbsent(
            transactionId,
            k -> new CachedTransaction(transactionId, Collections.emptyList(), null));
    cachedTransaction.rollback();
  }

  /**
   * Retrieve current state of the transaction.
   *
   * @param transactionId ID of the transaction
   * @return the {@link State} associated with the transaction on server
   */
  @Override
  public State getCurrentState(String transactionId) {
    @Nullable CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    if (cachedTransaction == null) {
      return State.empty();
    }
    return cachedTransaction.getState();
  }

  @Override
  public void setCurrentState(
      String transactionId, @Nullable State state, CommitCallback callback) {

    CachedTransaction cachedTransaction =
        openTransactionMap.computeIfAbsent(
            transactionId, tmp -> createCachedTransaction(transactionId, state));

    final StreamElement update;
    if (state != null) {
      update = stateDesc.upsert(transactionId, System.currentTimeMillis(), state);
    } else {
      update = stateDesc.delete(transactionId, System.currentTimeMillis());
    }
    cachedTransaction.getStateView().write(update, callback);
  }

  @VisibleForTesting
  CachedTransaction createCachedTransaction(String transactionId, State state) {
    final Collection<KeyAttribute> attributes;
    if (!state.getCommittedAttributes().isEmpty()) {
      HashSet<KeyAttribute> committedSet = Sets.newHashSet(state.getCommittedAttributes());
      committedSet.addAll(state.getInputAttributes());
      attributes = committedSet;
    } else {
      attributes = state.getInputAttributes();
    }
    return new CachedTransaction(transactionId, attributes, null);
  }

  @Override
  public void writeResponse(
      String transactionId, String responseId, Response response, CommitCallback callback) {
    getCachedTransaction(transactionId)
        .getResponseWriter()
        .write(
            responseDesc.upsert(transactionId, responseId, System.currentTimeMillis(), response),
            callback);
  }

  private CachedTransaction getCachedTransaction(String transactionId) {
    CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    Preconditions.checkState(
        cachedTransaction != null, "Transaction %s is not currently open", transactionId);
    return cachedTransaction;
  }

  private Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor>
      findFamilyForTransactionalAttribute(List<AttributeDescriptor<?>> attributes) {

    Preconditions.checkArgument(
        !attributes.isEmpty(), "Cannot return families for empty attribute list");

    TransactionMode mode = attributes.get(0).getTransactionMode();
    Preconditions.checkArgument(
        attributes.stream().allMatch(a -> a.getTransactionMode() == mode),
        "All passed attributes must have the same transaction mode. Got attributes %s.",
        attributes);

    List<DirectAttributeFamilyDescriptor> candidates =
        attributes
            .stream()
            .flatMap(a -> a.getTransactionalManagerFamilies().stream())
            .distinct()
            .map(direct::findFamilyByName)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    List<AttributeDescriptor<?>> requestResponseState =
        candidates
            .stream()
            .flatMap(f -> f.getAttributes().stream().filter(a -> !a.equals(commitDesc)))
            .sorted(Comparator.comparing(AttributeDescriptor::getName))
            .collect(Collectors.toList());

    Preconditions.checkState(
        requestResponseState.equals(Lists.newArrayList(requestDesc, responseDesc, stateDesc)),
        "Should have received only families for unique transactional attributes, "
            + "got %s for %s with transactional mode %s",
        candidates,
        attributes,
        mode);

    return candidates
        .stream()
        .flatMap(f -> f.getAttributes().stream().map(a -> Pair.of(a, f)))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }
}
