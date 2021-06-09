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
import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
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
public class TransactionResourceManager
    implements ClientTransactionManager, ServerTransactionManager {

  @VisibleForTesting
  static TransactionResourceManager create(DirectDataOperator direct) {
    return new TransactionResourceManager(direct, Collections.emptyMap());
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
            stateView != null, "StateView not initialized for family %s", family);
      }
      return stateView;
    }
  }

  @VisibleForTesting
  class CachedTransaction implements AutoCloseable {

    @Getter final String transactionId;
    @Getter final long created = System.currentTimeMillis();
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
      this.attributeToFamily.putAll(
          findFamilyForTransactionalAttribute(
              attributes
                  .stream()
                  .map(KeyAttribute::getAttributeDescriptor)
                  .collect(Collectors.toList())));
      this.responseConsumer = responseConsumer;
    }

    void open(List<KeyAttribute> inputAttrs) {
      checkThread();
      log.debug("Opening transaction {} with inputAttrs {}", transactionId, inputAttrs);
      Preconditions.checkState(responseConsumer != null);
      addTransactionResponseConsumer(
          transactionId, attributeToFamily.get(responseDesc), responseConsumer);
      sendRequest(Request.builder().flags(Flags.OPEN).inputAttributes(inputAttrs).build(), "open");
    }

    public void commit(List<KeyAttribute> outputAttributes) {
      checkThread();
      log.debug(
          "Committing transaction {} with outputAttributes {}", transactionId, outputAttributes);
      sendRequest(
          Request.builder().flags(Request.Flags.COMMIT).outputAttributes(outputAttributes).build(),
          "commit");
    }

    public void update(List<KeyAttribute> addedAttributes) {
      checkThread();
      log.debug("Updating transaction {} with addedAttributes {}", transactionId, addedAttributes);
      sendRequest(
          Request.builder().flags(Request.Flags.UPDATE).inputAttributes(addedAttributes).build(),
          "update");
    }

    public void rollback() {
      checkThread();
      log.debug("Rolling back transaction {} (cached {})", transactionId, this);
      sendRequest(Request.builder().flags(Flags.ROLLBACK).build(), "rollback");
    }

    private void sendRequest(Request request, String requestId) {
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<Throwable> error = new AtomicReference<>();
      Pair<List<Integer>, OnlineAttributeWriter> writerWithAssignedPartitions = getRequestWriter();
      Preconditions.checkState(
          !writerWithAssignedPartitions.getFirst().isEmpty(),
          "Received empty partitions to observe for responses to transactional "
              + "requests. Please see if you have enough partitions and if your clients can correctly "
              + "resolve hostnames");
      writerWithAssignedPartitions
          .getSecond()
          .write(
              requestDesc.upsert(
                  transactionId,
                  requestId,
                  System.currentTimeMillis(),
                  request.withResponsePartitionId(
                      pickOneAtRandom(writerWithAssignedPartitions.getFirst()))),
              (succ, exc) -> {
                error.set(exc);
                latch.countDown();
              });
      ExceptionUtils.ignoringInterrupted(latch::await);
      if (error.get() != null) {
        throw new IllegalStateException(error.get());
      }
    }

    private int pickOneAtRandom(List<Integer> partitions) {
      return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
    }

    @Override
    public void close() {
      requestWriter = responseWriter = null;
      stateView = null;
    }

    Pair<List<Integer>, OnlineAttributeWriter> getRequestWriter() {
      checkThread();
      if (requestWriter == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(requestDesc);
        requestWriter = getCachedAccessors(family).getOrCreateRequestWriter();
      }
      DirectAttributeFamilyDescriptor responseFamile = attributeToFamily.get(responseDesc);
      return Pair.of(
          Objects.requireNonNull(clientObservedFamilies.get(responseFamile).getPartitions()),
          requestWriter);
    }

    OnlineAttributeWriter getResponseWriter() {
      checkThread();
      if (responseWriter == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(responseDesc);
        responseWriter = getCachedAccessors(family).getOrCreateResponseWriter();
      }
      return responseWriter;
    }

    CachedView getStateView() {
      if (stateView == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(stateDesc);
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

  private static class HandleWithAssignment {

    @Getter private final List<Integer> partitions = new ArrayList<>();
    @Getter ObserveHandle observeHandle = null;

    public void assign(Collection<Partition> partitions) {
      this.partitions.clear();
      this.partitions.addAll(
          partitions.stream().map(Partition::getId).collect(Collectors.toList()));
    }

    public void withHandle(ObserveHandle handle) {
      this.observeHandle = handle;
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
  private final Map<DirectAttributeFamilyDescriptor, ObserveHandle> serverObservedFamilies =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, HandleWithAssignment> clientObservedFamilies =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, CachedView> stateViews =
      new ConcurrentHashMap<>();
  private final Map<String, BiConsumer<String, Response>> transactionResponseConsumers =
      new ConcurrentHashMap<>();

  @Getter(AccessLevel.PACKAGE)
  private final long transactionTimeoutMs;

  public TransactionResourceManager(DirectDataOperator direct, Map<String, Object> cfg) {
    this.direct = direct;
    this.transaction = direct.getRepository().getEntity("_transaction");
    this.requestDesc = Wildcard.of(transaction, transaction.getAttribute("request.*"));
    this.responseDesc = Wildcard.of(transaction, transaction.getAttribute("response.*"));
    this.stateDesc = Regular.of(transaction, transaction.getAttribute("state"));
    this.commitDesc = Regular.of(transaction, transaction.getAttribute("commit"));
    this.transactionTimeoutMs = getTransactionTimeout(cfg);

    log.info(
        "Created {} with transaction timeout {} ms",
        getClass().getSimpleName(),
        transactionTimeoutMs);
  }

  private long getTransactionTimeout(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get("timeout"))
        .map(Object::toString)
        .map(Long::parseLong)
        .orElse(3600000L);
  }

  @Override
  public void close() {
    openTransactionMap.forEach((k, v) -> v.close());
    cachedAccessors.forEach((k, v) -> v.close());
    openTransactionMap.clear();
    cachedAccessors.clear();
    transactionResponseConsumers.clear();
    serverObservedFamilies.values().forEach(ObserveHandle::close);
    serverObservedFamilies.clear();
    clientObservedFamilies.values().forEach(p -> p.getObserveHandle().close());
    clientObservedFamilies.clear();
  }

  @Override
  public void houseKeeping() {
    // FIXME: [PROXIMA-215]
    long now = System.currentTimeMillis();
    long releaseTime = now - transactionTimeoutMs;
    openTransactionMap
        .entrySet()
        .stream()
        .filter(e -> e.getValue().getCreated() < releaseTime)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList())
        .forEach(this::release);
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
      CommitLogObserver requestObserver) {

    CommitLogObserver synchronizedObserver = LogObservers.synchronizedObserver(requestObserver);

    List<Set<String>> families =
        direct
            .getRepository()
            .getAllEntities()
            .filter(EntityDescriptor::isTransactional)
            .flatMap(e -> e.getAllAttributes().stream())
            .filter(a -> a.getTransactionMode() != TransactionMode.NONE)
            .map(AttributeDescriptor::getTransactionalManagerFamilies)
            .map(Sets::newHashSet)
            .distinct()
            .collect(Collectors.toList());

    CountDownLatch initializedLatch = new CountDownLatch(families.size());
    families
        .stream()
        .map(this::toRequestStatePair)
        .forEach(
            p -> {
              DirectAttributeFamilyDescriptor requestFamily = p.getFirst();
              DirectAttributeFamilyDescriptor stateFamily = p.getSecond();
              log.info(
                  "Starting to observe family {} with URI {}",
                  requestFamily,
                  requestFamily.getDesc().getStorageUri());
              CommitLogReader reader = Optionals.get(requestFamily.getCommitLogReader());
              serverObservedFamilies.put(
                  requestFamily,
                  reader.observe(
                      name + "-" + requestFamily.getDesc().getName(),
                      repartitionHookForView(
                          stateFamily, updateConsumer, synchronizedObserver, initializedLatch)));
            });
    ExceptionUtils.unchecked(initializedLatch::await);
  }

  private Pair<DirectAttributeFamilyDescriptor, DirectAttributeFamilyDescriptor> toRequestStatePair(
      Collection<String> families) {

    List<DirectAttributeFamilyDescriptor> candidates =
        families
            .stream()
            .map(direct::getFamilyByName)
            .filter(
                af ->
                    af.getAttributes().contains(requestDesc)
                        || af.getAttributes().contains(stateDesc))
            .collect(Collectors.toList());
    Preconditions.checkState(candidates.size() < 3 && !candidates.isEmpty());
    if (candidates.size() == 1) {
      return Pair.of(candidates.get(0), candidates.get(0));
    }
    return candidates.get(0).getAttributes().contains(requestDesc)
        ? Pair.of(candidates.get(0), candidates.get(1))
        : Pair.of(candidates.get(1), candidates.get(0));
  }

  private CommitLogObserver repartitionHookForView(
      DirectAttributeFamilyDescriptor stateFamily,
      BiConsumer<StreamElement, Pair<Long, Object>> updateConsumer,
      CommitLogObserver delegate,
      CountDownLatch initializedLatch) {

    return new ForwardingObserver(delegate) {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        if (ingest.getStamp() > System.currentTimeMillis() - transactionTimeoutMs) {
          super.onNext(ingest, context);
        } else {
          log.warn(
              "Skipping request {} due to timeout. Current timeout specified as {}",
              ingest,
              transactionTimeoutMs);
          context.confirm();
        }
        return true;
      }

      @Override
      public void onRepartition(OnRepartitionContext context) {
        CachedView view =
            stateViews.computeIfAbsent(stateFamily, f -> Optionals.get(f.getCachedView()));
        Set<Integer> partitionIds =
            context.partitions().stream().map(Partition::getId).collect(Collectors.toSet());
        if (!partitionIds.isEmpty()) {
          Duration ttl = Duration.ofMillis(transactionTimeoutMs);
          view.assign(
              view.getPartitions()
                  .stream()
                  .filter(p -> partitionIds.contains(p.getId()))
                  .collect(Collectors.toList()),
              updateConsumer,
              ttl);
          initializedLatch.countDown();
        }
        super.onRepartition(context);
      }
    };
  }

  private void addTransactionResponseConsumer(
      String transactionId,
      DirectAttributeFamilyDescriptor responseFamily,
      BiConsumer<String, Response> responseConsumer) {

    Preconditions.checkArgument(responseFamily.getAttributes().contains(responseDesc));
    transactionResponseConsumers.put(transactionId, responseConsumer);
    clientObservedFamilies.computeIfAbsent(
        responseFamily,
        k -> {
          log.debug("Starting to observe family {} with URI {}", k, k.getDesc().getStorageUri());
          HandleWithAssignment assignment = new HandleWithAssignment();
          assignment.withHandle(
              Optionals.get(k.getCommitLogReader())
                  .observe(
                      newResponseObserverNameFor(k), newTransactionResponseObserver(assignment)));
          return assignment;
        });
  }

  protected String newResponseObserverNameFor(DirectAttributeFamilyDescriptor k) {
    String localhost = "localhost";
    try {
      localhost = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      log.warn("Error getting name of localhost, using {} instead.", localhost, e);
    }
    return "transactionResponseObserver-"
        + k.getDesc().getName()
        + (localhost.hashCode() & Integer.MAX_VALUE);
  }

  private CommitLogObserver newTransactionResponseObserver(HandleWithAssignment assignment) {
    Preconditions.checkArgument(assignment != null);
    return new CommitLogObserver() {

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
            log.debug(
                "Missing consumer for transaction {} processing response {}. Ignoring.",
                transactionId,
                response.orElse(null));
          }
        }
        context.confirm();
        return true;
      }

      @Override
      public void onRepartition(OnRepartitionContext context) {
        assignment.assign(context.partitions());
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

  @Override
  public void release(String transactionId) {
    Optional.ofNullable(openTransactionMap.remove(transactionId))
        .ifPresent(CachedTransaction::close);
    transactionResponseConsumers.remove(transactionId);
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

    CachedTransaction cachedTransaction = getOrCreateCachedTransaction(transactionId, state);
    final StreamElement update;
    if (state != null) {
      update = stateDesc.upsert(transactionId, System.currentTimeMillis(), state);
    } else {
      update = stateDesc.delete(transactionId, System.currentTimeMillis());
    }
    cachedTransaction.getStateView().write(update, callback);
  }

  @Override
  public void ensureTransactionOpen(String transactionId, State state) {
    getOrCreateCachedTransaction(transactionId, state);
  }

  @VisibleForTesting
  CachedTransaction createCachedTransaction(String transactionId, State state) {
    return createCachedTransaction(transactionId, state, null);
  }

  @VisibleForTesting
  CachedTransaction createCachedTransaction(
      String transactionId, State state, BiConsumer<String, Response> responseConsumer) {

    final Collection<KeyAttribute> attributes;
    if (!state.getCommittedAttributes().isEmpty()) {
      HashSet<KeyAttribute> committedSet = Sets.newHashSet(state.getCommittedAttributes());
      committedSet.addAll(state.getInputAttributes());
      attributes = committedSet;
    } else {
      attributes = state.getInputAttributes();
    }
    return new CachedTransaction(transactionId, attributes, responseConsumer);
  }

  @Override
  public void writeResponse(
      String transactionId, String responseId, Response response, CommitCallback callback) {

    CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    if (cachedTransaction != null) {
      cachedTransaction
          .getResponseWriter()
          .write(
              responseDesc.upsert(transactionId, responseId, System.currentTimeMillis(), response),
              callback);
    } else {
      log.warn(
          "Transaction {} is not open, don't have a writer to return response {}",
          transactionId,
          response);
      callback.commit(true, null);
    }
  }

  private CachedTransaction getOrCreateCachedTransaction(String transactionId, State state) {
    return openTransactionMap.computeIfAbsent(
        transactionId, tmp -> createCachedTransaction(transactionId, state));
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
