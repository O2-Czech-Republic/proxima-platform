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

import cz.o2.proxima.core.annotations.DeclaredThreadSafe;
import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.TransactionMode;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Commit;
import cz.o2.proxima.core.transaction.Commit.TransactionUpdate;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.Request;
import cz.o2.proxima.core.transaction.Request.Flags;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObservers.ForwardingObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.ObserveHandleUtils;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private class TransactionConfig implements ServerTransactionConfig {

    @Override
    public long getCleanupInterval() {
      return cleanupIntervalMs;
    }

    @Override
    public long getTransactionTimeoutMs() {
      return transactionTimeoutMs;
    }

    @Override
    public InitialSequenceIdPolicy getInitialSeqIdPolicy() {
      return initialSequenceIdPolicy;
    }

    @Override
    public TransactionMonitoringPolicy getTransactionMonitoringPolicy() {
      return transactionMonitoringPolicy;
    }

    @Override
    public int getServerTerminationTimeoutSeconds() {
      return serverTerminationTimeoutSeconds;
    }
  }

  private class CachedWriters implements AutoCloseable {

    private final DirectAttributeFamilyDescriptor family;
    @Nullable OnlineAttributeWriter writer;
    @Nullable CachedView stateView;

    CachedWriters(DirectAttributeFamilyDescriptor family) {
      this.family = family;
    }

    @Override
    public void close() {
      Optional.ofNullable(writer).ifPresent(OnlineAttributeWriter::close);
      Optional.ofNullable(stateView).ifPresent(CachedView::close);
    }

    public OnlineAttributeWriter getOrCreateWriter() {
      if (writer == null) {
        writer = Optionals.get(family.getWriter()).online();
      }
      return writer;
    }

    public CachedView getOrCreateStateView() {
      if (stateView == null) {
        stateView = stateViews.get(family);
        Preconditions.checkState(
            stateView != null,
            "StateView not initialized for family %s. Initialized families are %s",
            family,
            stateViews.keySet());
      }
      return stateView;
    }
  }

  @VisibleForTesting
  class CachedTransaction implements AutoCloseable {

    @Getter final String transactionId;
    long touched = System.currentTimeMillis();
    final Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor> attributeToFamily =
        new HashMap<>();
    final Map<String, CompletableFuture<Response>> requestFutures = new ConcurrentHashMap<>();
    @Nullable OnlineAttributeWriter requestWriter;
    @Nullable OnlineAttributeWriter commitWriter;
    @Nullable CachedView stateView;
    int requestId = 1;

    CachedTransaction(String transactionId, Collection<KeyAttribute> attributes) {
      this.transactionId = transactionId;
      this.attributeToFamily.putAll(
          findFamilyForTransactionalAttribute(
              attributes.stream()
                  .map(KeyAttribute::getAttributeDescriptor)
                  .collect(Collectors.toList())));
    }

    CompletableFuture<Response> open(List<KeyAttribute> inputAttrs) {
      String request = "open." + (requestId++);
      log.debug(
          "Opening transaction {} with inputAttrs {} using {}", transactionId, inputAttrs, request);
      CompletableFuture<Response> res =
          requestFutures.compute(request, (k, v) -> new CompletableFuture<>());
      addTransactionResponseConsumer(
          transactionId,
          attributeToFamily.get(responseDesc),
          (reqId, response) ->
              Optional.ofNullable(requestFutures.remove(reqId))
                  .ifPresent(c -> c.complete(response)));
      return sendRequest(
              Request.builder().flags(Flags.OPEN).inputAttributes(inputAttrs).build(), request)
          .thenCompose(ign -> res);
    }

    public CompletableFuture<Response> commit(Collection<StreamElement> outputs) {
      String request = "commit";
      log.debug("Committing transaction {} with outputs {}", transactionId, outputs);
      CompletableFuture<Response> res =
          requestFutures.compute(request, (k, v) -> new CompletableFuture<>());
      return sendRequest(
              Request.builder().flags(Request.Flags.COMMIT).outputs(outputs).build(), request)
          .thenCompose(ign -> res);
    }

    public CompletableFuture<Response> update(List<KeyAttribute> addedAttributes) {
      if (addedAttributes.isEmpty()) {
        return CompletableFuture.completedFuture(Response.empty().updated());
      }
      String request = "update." + (requestId++);
      log.debug(
          "Updating transaction {} with addedAttributes {} using {}",
          transactionId,
          addedAttributes,
          request);
      CompletableFuture<Response> res =
          requestFutures.compute(request, (k, v) -> new CompletableFuture<>());
      return sendRequest(
              Request.builder()
                  .flags(Request.Flags.UPDATE)
                  .inputAttributes(addedAttributes)
                  .build(),
              request)
          .thenCompose(ign -> res);
    }

    public CompletableFuture<Response> rollback() {
      log.debug("Rolling back transaction {} (cached {})", transactionId, this);
      String request = "rollback";
      CompletableFuture<Response> res =
          requestFutures.compute(request, (k, v) -> new CompletableFuture<>());
      return sendRequest(Request.builder().flags(Flags.ROLLBACK).build(), request)
          .thenCompose(ign -> res);
    }

    private CompletableFuture<?> sendRequest(Request request, String requestId) {
      Pair<List<Integer>, OnlineAttributeWriter> writerWithAssignedPartitions = getRequestWriter();
      Preconditions.checkState(
          !writerWithAssignedPartitions.getFirst().isEmpty(),
          "Received empty partitions to observe for responses to transactional "
              + "requests. Please see if you have enough partitions and if your clients can correctly "
              + "resolve hostnames");
      CompletableFuture<?> res = new CompletableFuture<>();
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
                if (exc != null) {
                  res.completeExceptionally(exc);
                } else {
                  res.complete(null);
                }
              });
      return res;
    }

    private int pickOneAtRandom(List<Integer> partitions) {
      return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
    }

    @Override
    public void close() {
      requestWriter = commitWriter = null;
      stateView = null;
    }

    Pair<List<Integer>, OnlineAttributeWriter> getRequestWriter() {
      if (requestWriter == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(requestDesc);
        requestWriter = getCachedAccessors(family).getOrCreateWriter();
      }
      DirectAttributeFamilyDescriptor responseFamily = attributeToFamily.get(responseDesc);
      return Pair.of(
          Objects.requireNonNull(clientObservedFamilies.get(responseFamily).getPartitions()),
          requestWriter);
    }

    public DirectAttributeFamilyDescriptor getResponseFamily() {
      return Objects.requireNonNull(attributeToFamily.get(responseDesc));
    }

    public DirectAttributeFamilyDescriptor getStateFamily() {
      return Objects.requireNonNull(attributeToFamily.get(stateDesc));
    }

    public OnlineAttributeWriter getCommitWriter() {
      if (commitWriter == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(getCommitDesc());
        commitWriter = getCachedAccessors(family).getOrCreateWriter();
      }
      return commitWriter;
    }

    CachedView getStateView() {
      if (stateView == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(stateDesc);
        stateView = getCachedAccessors(family).getOrCreateStateView();
      }
      return stateView;
    }

    public State getState() {
      return getStateView()
          .get(transactionId, stateDesc)
          .map(KeyValue::getParsedRequired)
          .orElse(State.empty());
    }

    public void touch(long stamp) {
      this.touched = stamp;
    }

    public long getStamp() {
      return touched;
    }
  }

  @Getter
  private static class HandleWithAssignment {

    private final List<Integer> partitions = new ArrayList<>();
    ObserveHandle observeHandle = null;

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
  private final Random random = new Random();
  private final Map<String, CachedTransaction> openTransactionMap = new ConcurrentHashMap<>();
  private final Map<AttributeFamilyDescriptor, CachedWriters> cachedAccessors =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, ObserveHandle> serverObservedFamilies =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, HandleWithAssignment> clientObservedFamilies =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, CachedView> stateViews =
      Collections.synchronizedMap(new HashMap<>());
  private final Map<String, BiConsumer<String, Response>> transactionResponseConsumers =
      new ConcurrentHashMap<>();
  @Getter private final TransactionConfig cfg = new TransactionConfig();
  private final Map<AttributeFamilyDescriptor, AtomicBoolean> activeForFamily =
      new ConcurrentHashMap<>();

  @Getter(AccessLevel.PACKAGE)
  private long transactionTimeoutMs;

  @Getter(AccessLevel.PRIVATE)
  private final long cleanupIntervalMs;

  @Getter(AccessLevel.PACKAGE)
  private final InitialSequenceIdPolicy initialSequenceIdPolicy;

  @Getter(AccessLevel.PACKAGE)
  private final TransactionMonitoringPolicy transactionMonitoringPolicy;

  @Getter(AccessLevel.PACKAGE)
  private final int serverTerminationTimeoutSeconds;

  @VisibleForTesting
  public TransactionResourceManager(DirectDataOperator direct, Map<String, Object> cfg) {
    this.direct = direct;
    this.transaction = direct.getRepository().getEntity("_transaction");
    this.requestDesc = Wildcard.of(transaction, transaction.getAttribute("request.*"));
    this.responseDesc = Wildcard.of(transaction, transaction.getAttribute("response.*"));
    this.stateDesc = Regular.of(transaction, transaction.getAttribute("state"));
    this.commitDesc = Regular.of(transaction, transaction.getAttribute("commit"));
    this.transactionTimeoutMs = getTransactionTimeout(cfg);
    this.cleanupIntervalMs = getCleanupInterval(cfg);
    this.initialSequenceIdPolicy = getInitialSequenceIdPolicy(cfg);
    this.transactionMonitoringPolicy = getTransactionMonitoringPolicy(cfg);
    this.serverTerminationTimeoutSeconds = getServerTerminationTimeout(cfg);

    log.info(
        "Created {} with transaction timeout {} ms",
        getClass().getSimpleName(),
        transactionTimeoutMs);
  }

  @VisibleForTesting
  public void setTransactionTimeoutMs(long timeoutMs) {
    this.transactionTimeoutMs = timeoutMs;
  }

  private static long getTransactionTimeout(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get("timeout"))
        .map(Object::toString)
        .map(Long::parseLong)
        .orElse(3600000L);
  }

  private static long getCleanupInterval(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get("cleanup-interval"))
        .map(Object::toString)
        .map(Long::parseLong)
        .orElse(3600000L);
  }

  private static InitialSequenceIdPolicy getInitialSequenceIdPolicy(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get("initial-sequence-id-policy"))
        .map(Object::toString)
        .map(c -> Classpath.newInstance(c, InitialSequenceIdPolicy.class))
        .orElse(new InitialSequenceIdPolicy.Default());
  }

  private static TransactionMonitoringPolicy getTransactionMonitoringPolicy(
      Map<String, Object> cfg) {

    return Optional.ofNullable(cfg.get("monitoring-policy"))
        .map(Object::toString)
        .map(c -> Classpath.newInstance(c, TransactionMonitoringPolicy.class))
        .orElse(TransactionMonitoringPolicy.nop());
  }

  private static int getServerTerminationTimeout(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get("server-termination-timeout"))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(2);
  }

  @Override
  public void close() {
    openTransactionMap.forEach((k, v) -> v.close());
    cachedAccessors.forEach((k, v) -> v.close());
    stateViews.forEach((k, v) -> v.close());
    openTransactionMap.clear();
    cachedAccessors.clear();
    stateViews.clear();
    transactionResponseConsumers.clear();
    serverObservedFamilies.values().forEach(ObserveHandle::close);
    serverObservedFamilies.clear();
    clientObservedFamilies.values().forEach(p -> p.getObserveHandle().close());
    clientObservedFamilies.clear();
  }

  @Override
  public void houseKeeping() {
    long now = System.currentTimeMillis();
    long releaseTime = now - cleanupIntervalMs;
    openTransactionMap.entrySet().stream()
        .filter(e -> e.getValue().getStamp() < releaseTime)
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

    final CommitLogObserver effectiveObserver;
    if (isNotThreadSafe(requestObserver)) {
      effectiveObserver = requestObserver;
    } else {
      effectiveObserver =
          new ThreadPooledObserver(
              direct.getContext().getExecutorService(),
              requestObserver,
              getDeclaredParallelism(requestObserver)
                  .orElse(Runtime.getRuntime().availableProcessors()));
    }
    log.debug("Running transaction observation with observer {}", effectiveObserver);

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

    families.stream()
        .map(this::toRequestStatePair)
        .forEach(
            p -> {
              DirectAttributeFamilyDescriptor requestFamily = p.getFirst();
              DirectAttributeFamilyDescriptor stateFamily = p.getSecond();
              String consumerName = name + "-" + requestFamily.getDesc().getName();
              log.info(
                  "Starting to observe family {} with URI {} and associated state family {} as {}",
                  requestFamily,
                  requestFamily.getDesc().getStorageUri(),
                  stateFamily,
                  consumerName);
              CommitLogReader reader = Optionals.get(requestFamily.getCommitLogReader());

              stateViews.computeIfAbsent(
                  stateFamily,
                  k -> {
                    CachedView view = Optionals.get(stateFamily.getCachedView());
                    Duration ttl = Duration.ofMillis(cleanupIntervalMs);
                    view.assign(
                        view.getPartitions(), createTransactionUpdateConsumer(updateConsumer), ttl);
                    return view;
                  });
              initializedLatch.countDown();

              serverObservedFamilies.put(
                  requestFamily,
                  reader.observe(
                      consumerName,
                      repartitionHookForBeingActive(
                          stateFamily, reader.getPartitions().size(), effectiveObserver)));
            });
    ExceptionUtils.unchecked(initializedLatch::await);
  }

  private BiConsumer<StreamElement, Pair<Long, Object>> createTransactionUpdateConsumer(
      BiConsumer<StreamElement, Pair<Long, Object>> delegate) {

    return (element, cached) -> {
      if (element.getAttributeDescriptor().equals(getStateDesc())) {
        Optional<State> state = getStateDesc().valueOf(element);
        if (state.isPresent()) {
          getOrCreateCachedTransaction(element.getKey(), state.get()).touch(element.getStamp());
        }
      }
      delegate.accept(element, cached);
    };
  }

  @VisibleForTesting
  static boolean isNotThreadSafe(CommitLogObserver requestObserver) {
    return requestObserver.getClass().getDeclaredAnnotation(DeclaredThreadSafe.class) == null;
  }

  @VisibleForTesting
  static Optional<Integer> getDeclaredParallelism(CommitLogObserver requestObserver) {
    return Arrays.stream(requestObserver.getClass().getAnnotations())
        .filter(a -> a.annotationType().equals(DeclaredThreadSafe.class))
        .map(DeclaredThreadSafe.class::cast)
        .findAny()
        .map(DeclaredThreadSafe::allowedParallelism)
        .filter(i -> i != -1);
  }

  private Pair<DirectAttributeFamilyDescriptor, DirectAttributeFamilyDescriptor> toRequestStatePair(
      Collection<String> families) {

    List<DirectAttributeFamilyDescriptor> candidates =
        families.stream()
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

  private CommitLogObserver repartitionHookForBeingActive(
      DirectAttributeFamilyDescriptor stateFamily, int numPartitions, CommitLogObserver delegate) {

    activeForFamily.putIfAbsent(stateFamily.getDesc(), new AtomicBoolean());
    return new ForwardingObserver(delegate) {

      @Override
      public boolean onNext(StreamElement element, OnNextContext context) {
        Preconditions.checkArgument(activeForFamily.get(stateFamily.getDesc()).get());
        if (element.getStamp() > System.currentTimeMillis() - 2 * transactionTimeoutMs) {
          super.onNext(element, context);
        } else {
          log.warn(
              "Skipping request {} due to timeout. Current timeout specified as {}",
              element,
              transactionTimeoutMs);
          context.confirm();
        }
        return true;
      }

      @Override
      public void onRepartition(OnRepartitionContext context) {
        Preconditions.checkArgument(
            context.partitions().isEmpty() || context.partitions().size() == numPartitions,
            "All or zero partitions must be assigned to the consumer. Got %s partitions from %s",
            context.partitions().size(),
            numPartitions);
        if (!context.partitions().isEmpty()) {
          transitionToActive(stateFamily);
        } else {
          transitionToInactive(stateFamily);
        }
        super.onRepartition(context);
      }
    };
  }

  private void transitionToActive(DirectAttributeFamilyDescriptor desc) {
    boolean isAtHead = false;
    while (!Thread.currentThread().isInterrupted() && !isAtHead) {
      CachedView view = Objects.requireNonNull(stateViews.get(desc));
      CommitLogReader reader = view.getUnderlyingReader();
      Optional<ObserveHandle> handle = view.getRunningHandle();
      if (handle.isPresent()) {
        isAtHead = ObserveHandleUtils.isAtHead(handle.get(), reader);
      }
      if (!isAtHead) {
        ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(100));
      }
    }
    if (isAtHead) {
      log.info("Transitioned to ACTIVE state for {}", desc);
      activeForFamily.get(desc.getDesc()).set(true);
    }
  }

  private void transitionToInactive(DirectAttributeFamilyDescriptor desc) {
    log.info("Transitioning to INACTIVE state for {}", desc);
    activeForFamily.get(desc.getDesc()).set(false);
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
          CommitLogReader reader = Optionals.get(k.getCommitLogReader());
          List<Partition> partitions = reader.getPartitions();
          Partition partition = partitions.get(random.nextInt(partitions.size()));
          assignment.withHandle(
              reader.observePartitions(
                  Collections.singleton(partition), newTransactionResponseObserver(assignment)));
          ExceptionUtils.ignoringInterrupted(() -> assignment.getObserveHandle().waitUntilReady());
          return assignment;
        });
  }

  private CommitLogObserver newTransactionResponseObserver(HandleWithAssignment assignment) {
    Preconditions.checkArgument(assignment != null);
    return new CommitLogObserver() {

      @Override
      public boolean onNext(StreamElement element, OnNextContext context) {
        log.debug("Received transaction event {}", element);
        if (element.getAttributeDescriptor().equals(responseDesc)) {
          String transactionId = element.getKey();
          Optional<Response> response = responseDesc.valueOf(element);
          @Nullable
          BiConsumer<String, Response> consumer = transactionResponseConsumers.get(transactionId);
          if (consumer != null) {
            if (response.isPresent()) {
              String suffix = responseDesc.extractSuffix(element.getAttribute());
              consumer.accept(suffix, response.get());
            } else {
              log.error("Failed to parse response from {}", element);
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
   * Initialize new transaction. If the transaction already existed prior to this call, the state is
   * updated accordingly.
   *
   * @param transactionId ID of transaction
   * @param attributes attributes affected by this transaction (both input and output)
   */
  @Override
  public CompletableFuture<Response> begin(String transactionId, List<KeyAttribute> attributes) {
    // reopen transaction including expiration stamp
    CachedTransaction transaction = new CachedTransaction(transactionId, attributes);
    openTransactionMap.put(transactionId, transaction);
    return transaction.open(attributes);
  }

  /**
   * Update the transaction with additional attributes related to the transaction.
   *
   * @param transactionId ID of transaction
   * @param newAttributes attributes to be added to the transaction
   */
  @Override
  public CompletableFuture<Response> updateTransaction(
      String transactionId, List<KeyAttribute> newAttributes) {
    @Nullable CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    Preconditions.checkArgument(
        cachedTransaction != null, "Transaction %s is not open", transactionId);
    return cachedTransaction.update(newAttributes);
  }

  @Override
  public CompletableFuture<Response> commit(
      String transactionId, Collection<StreamElement> outputs) {
    @Nullable CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    Preconditions.checkArgument(
        cachedTransaction != null, "Transaction %s is not open", transactionId);
    return cachedTransaction.commit(outputs);
  }

  @Override
  public CompletableFuture<Response> rollback(String transactionId) {
    return Optional.ofNullable(openTransactionMap.get(transactionId))
        .map(CachedTransaction::rollback)
        .orElseGet(
            () ->
                CompletableFuture.completedFuture(
                    Response.forRequest(Request.builder().flags(Flags.ROLLBACK).build())
                        .aborted()));
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
  public void ensureTransactionOpen(String transactionId, State state) {
    getOrCreateCachedTransaction(transactionId, state);
  }

  @VisibleForTesting
  @Nullable
  CachedTransaction createCachedTransaction(String transactionId, State state) {
    Preconditions.checkArgument(state.getCommittedOutputs().isEmpty());
    if (state.getInputAttributes().isEmpty()) {
      return null;
    }
    return new CachedTransaction(transactionId, state.getInputAttributes());
  }

  @Override
  public void writeResponseAndUpdateState(
      String transactionId,
      State updateState,
      String responseId,
      Response response,
      CommitCallback callback) {

    CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    if (cachedTransaction != null) {
      DirectAttributeFamilyDescriptor responseFamily = cachedTransaction.getResponseFamily();
      DirectAttributeFamilyDescriptor stateFamily = cachedTransaction.getStateFamily();
      final OnlineAttributeWriter writer = cachedTransaction.getCommitWriter();
      final CachedView stateView = cachedTransaction.getStateView();
      long now = System.currentTimeMillis();
      StreamElement stateUpsert = getStateDesc().upsert(transactionId, now, updateState);
      List<TransactionUpdate> transactionUpdates =
          Arrays.asList(
              new TransactionUpdate(stateFamily.getDesc().getName(), stateUpsert),
              new TransactionUpdate(
                  responseFamily.getDesc().getName(),
                  getResponseDesc().upsert(transactionId, responseId, now, response)));
      log.debug("Returning response {} for transaction {}", response, transactionId);
      final Commit commit =
          updateState.getCommittedOutputs().isEmpty()
              ? Commit.updates(transactionUpdates)
              : Commit.outputs(transactionUpdates, updateState.getCommittedOutputs());

      synchronized (stateView) {
        ensureTransactionOpen(transactionId, updateState);
        stateView.cache(stateUpsert);
      }
      synchronized (writer) {
        writer.write(
            getCommitDesc().upsert(transactionId, System.currentTimeMillis(), commit), callback);
      }
    } else {
      log.warn(
          "Transaction {} is not open, don't have a writer to return response {}",
          transactionId,
          response);
      callback.commit(true, null);
    }
  }

  private @Nullable CachedTransaction getOrCreateCachedTransaction(
      String transactionId, State state) {
    return openTransactionMap.computeIfAbsent(
        transactionId, tmp -> createCachedTransaction(transactionId, state));
  }

  private Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor>
      findFamilyForTransactionalAttribute(List<AttributeDescriptor<?>> attributes) {

    Preconditions.checkArgument(
        !attributes.isEmpty(), "Cannot return families for empty attribute list");

    Set<TransactionMode> modes =
        attributes.stream()
            .map(AttributeDescriptor::getTransactionMode)
            .filter(mode -> mode != TransactionMode.NONE)
            .collect(Collectors.toSet());
    Preconditions.checkArgument(
        modes.size() == 1,
        "All passed attributes must have the same transaction mode. Got attributes %s with modes %s.",
        attributes,
        modes);
    TransactionMode mode = Iterables.getOnlyElement(modes);

    List<DirectAttributeFamilyDescriptor> candidates =
        attributes.stream()
            .flatMap(a -> a.getTransactionalManagerFamilies().stream())
            .distinct()
            .map(direct::findFamilyByName)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    List<AttributeDescriptor<?>> requestResponseState =
        candidates.stream()
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

    Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor> res =
        candidates.stream()
            .flatMap(f -> f.getAttributes().stream().map(a -> Pair.of(a, f)))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    direct
        .getRepository()
        .getAllFamilies(true)
        .filter(af -> af.getAttributes().contains(commitDesc))
        .forEach(af -> res.put(commitDesc, direct.getFamilyByName(af.getName())));

    return res;
  }
}
