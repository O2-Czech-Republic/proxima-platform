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
package cz.o2.proxima.direct.transaction.manager;

import cz.o2.proxima.core.annotations.DeclaredThreadSafe;
import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Commit;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.KeyAttributes;
import cz.o2.proxima.core.transaction.Request;
import cz.o2.proxima.core.transaction.Request.Flags;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.transaction.ServerTransactionManager;
import cz.o2.proxima.direct.core.transaction.TransactionMonitoringPolicy;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterators;
import cz.o2.proxima.internal.com.google.common.collect.MultimapBuilder;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.internal.com.google.common.collect.SortedSetMultimap;
import cz.o2.proxima.internal.com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link CommitLogObserver} performing the overall transaction logic via keeping state of
 * transactions and responding to requests.
 */
@Slf4j
@Internal
@ThreadSafe
@DeclaredThreadSafe
public class TransactionLogObserver implements CommitLogObserver {

  @Value
  private static class KeyWithAttribute {

    static KeyWithAttribute of(KeyAttribute ka) {
      if (ka.isWildcardQuery()) {
        return new KeyWithAttribute(
            ka.getAttributeDescriptor().getEntity(),
            ka.getAttributeDescriptor().toAttributePrefix(),
            ka.getKey());
      }
      return new KeyWithAttribute(
          ka.getAttributeDescriptor().getEntity(),
          ka.getAttributeDescriptor().toAttributePrefix() + ka.getAttributeSuffix().orElse(""),
          ka.getKey());
    }

    static KeyWithAttribute ofWildcard(KeyAttribute ka) {
      return new KeyWithAttribute(
          ka.getAttributeDescriptor().getEntity(),
          ka.getAttributeDescriptor().toAttributePrefix(),
          ka.getKey());
    }

    private KeyWithAttribute(String entity, String attribute, String key) {
      this.attribute = entity + "." + attribute;
      this.key = key;
    }

    // attribute including entity in dot notation
    String attribute;
    // primary key
    String key;
  }

  @Value
  private static class SeqIdWithTombstone implements Comparable<SeqIdWithTombstone> {

    public static SeqIdWithTombstone create(long committedSeqId, long commitStamp, boolean delete) {

      return new SeqIdWithTombstone(committedSeqId, delete, commitStamp);
    }

    /** sequential ID of the update */
    long seqId;

    /** marker that the write is actually a delete */
    boolean tombstone;

    /** timestamp of creation of the record. */
    long timestamp;

    @Override
    public int compareTo(SeqIdWithTombstone other) {
      return Long.compare(seqId, other.getSeqId());
    }
  }

  private static class Locker implements AutoCloseable {

    static Locker of(Lock lock) {
      return new Locker(lock);
    }

    private final Lock lock;

    Locker(Lock lock) {
      this.lock = lock;
      this.lock.lock();
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }

  private static TransactionLogObserver INSTANCE = null;

  private final DirectDataOperator direct;
  private final Metrics metrics;
  private final ServerTransactionManager unsynchronizedManager;
  private final ServerTransactionManager manager;
  private final AtomicLong sequenceId = new AtomicLong();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock timerLock = new ReentrantReadWriteLock();
  private final Object commitLock = new Object();

  @GuardedBy("timerLock")
  private final PriorityQueue<Pair<Long, Runnable>> timers =
      new PriorityQueue<>(Comparator.comparing(Pair::getFirst));

  @GuardedBy("lock")
  private final SortedSetMultimap<KeyWithAttribute, SeqIdWithTombstone> lastUpdateSeqId =
      MultimapBuilder.hashKeys().treeSetValues().build();

  @GuardedBy("lock")
  private final Map<KeyWithAttribute, Map<KeyWithAttribute, SeqIdWithTombstone>> updatesToWildcard =
      new HashMap<>();

  private final TransactionMonitoringPolicy monitoringPolicy;

  public TransactionLogObserver(DirectDataOperator direct, Metrics metrics) {
    this.direct = direct;
    this.metrics = metrics;
    this.unsynchronizedManager = getServerTransactionManager(direct);
    this.manager = synchronizedManager(unsynchronizedManager);
    this.monitoringPolicy = unsynchronizedManager.getCfg().getTransactionMonitoringPolicy();
    sequenceId.set(unsynchronizedManager.getCfg().getInitialSeqIdPolicy().apply());
    assertSingleton();
    startHouseKeeping();
  }

  @VisibleForTesting
  protected void assertSingleton() {
    synchronized (TransactionLogObserver.class) {
      Preconditions.checkState(INSTANCE == null);
      INSTANCE = this;
    }
  }

  private ServerTransactionManager synchronizedManager(ServerTransactionManager delegate) {
    return new ServerTransactionManager() {

      @Override
      public void runObservations(
          String name,
          BiConsumer<StreamElement, Pair<Long, Object>> updateConsumer,
          CommitLogObserver requestObserver) {

        delegate.runObservations(name, updateConsumer, requestObserver);
      }

      @Override
      public synchronized State getCurrentState(String transactionId) {
        return delegate.getCurrentState(transactionId);
      }

      @Override
      public synchronized void ensureTransactionOpen(String transactionId, State state) {
        delegate.ensureTransactionOpen(transactionId, state);
      }

      @Override
      public synchronized void writeResponseAndUpdateState(
          String transactionId,
          State state,
          String responseId,
          Response response,
          CommitCallback callback) {

        delegate.writeResponseAndUpdateState(transactionId, state, responseId, response, callback);
      }

      @Override
      public void close() {
        delegate.close();
      }

      @Override
      public void houseKeeping() {
        delegate.houseKeeping();
      }

      @Override
      public ServerTransactionConfig getCfg() {
        return delegate.getCfg();
      }

      @Override
      public EntityDescriptor getTransaction() {
        return delegate.getTransaction();
      }

      @Override
      public Wildcard<Request> getRequestDesc() {
        return delegate.getRequestDesc();
      }

      @Override
      public Wildcard<Response> getResponseDesc() {
        return delegate.getResponseDesc();
      }

      @Override
      public Regular<State> getStateDesc() {
        return delegate.getStateDesc();
      }

      @Override
      public Regular<Commit> getCommitDesc() {
        return delegate.getCommitDesc();
      }
    };
  }

  @VisibleForTesting
  ServerTransactionManager getServerTransactionManager(DirectDataOperator direct) {
    return direct.getServerTransactionManager();
  }

  private void startHouseKeeping() {
    direct
        .getContext()
        .getExecutorService()
        .submit(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  manager.houseKeeping();
                  long now = currentTimeMillis();
                  long cleanupInterval = manager.getCfg().getCleanupInterval();
                  long cleanup = now - cleanupInterval;
                  int cleaned;
                  try (var l = Locker.of(lock.writeLock())) {
                    List<Map.Entry<KeyWithAttribute, SeqIdWithTombstone>> toCleanUp =
                        lastUpdateSeqId.entries().stream()
                            .filter(e -> e.getValue().getTimestamp() < cleanup)
                            .collect(Collectors.toList());
                    cleaned = toCleanUp.size();
                    toCleanUp.forEach(e -> lastUpdateSeqId.remove(e.getKey(), e.getValue()));
                  }
                  // release and re-acquire lock to enable progress of any waiting threads
                  try (var l = Locker.of(lock.writeLock())) {
                    Iterator<Map<KeyWithAttribute, SeqIdWithTombstone>> it =
                        updatesToWildcard.values().iterator();
                    while (it.hasNext()) {
                      Map<KeyWithAttribute, SeqIdWithTombstone> value = it.next();
                      Iterators.removeIf(
                          value.values().iterator(), e -> e.getTimestamp() < cleanup);
                      if (value.isEmpty()) {
                        it.remove();
                      }
                    }
                  }
                  long duration = currentTimeMillis() - now;
                  log.info("Finished housekeeping in {} ms, removed {} records", duration, cleaned);
                  metrics.getWritesCleaned().increment(cleaned);
                  if (duration < cleanupInterval) {
                    sleep(cleanupInterval - duration);
                  }
                } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
                } catch (Throwable err) {
                  log.error("Error in housekeeping thread", err);
                }
              }
            });
  }

  @VisibleForTesting
  void sleep(long sleepMs) throws InterruptedException {
    TimeUnit.MILLISECONDS.sleep(sleepMs);
  }

  @VisibleForTesting
  long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @Override
  public boolean onNext(StreamElement element, OnNextContext context) {
    log.debug("Received element {} for transaction processing", element);
    Wildcard<Request> requestDesc = manager.getRequestDesc();
    if (element.getAttributeDescriptor().equals(requestDesc)) {
      handleRequest(element, context);
    } else {
      // unknown attribute, probably own response or state update, can be safely ignored
      log.debug("Unknown attribute {}. Ignored.", element.getAttributeDescriptor());
      context.confirm();
    }
    handleWatermark(context.getWatermark());
    return true;
  }

  @Override
  public void onIdle(OnIdleContext context) {
    handleWatermark(context.getWatermark());
  }

  private void handleWatermark(long watermark) {
    List<Runnable> process = new ArrayList<>();
    final boolean canFireTimer;
    try (var read = Locker.of(timerLock.readLock())) {
      if (log.isDebugEnabled()) {
        log.debug("Processing watermark {} with {} timers", watermark, timers);
      }
      canFireTimer = !timers.isEmpty() && timers.peek().getFirst() < watermark;
    }
    if (canFireTimer) {
      try (var writeLock = Locker.of(timerLock.writeLock())) {
        while (!timers.isEmpty() && timers.peek().getFirst() < watermark) {
          process.add(timers.poll().getSecond());
        }
      }
      // fire the timers outside the lock
      process.forEach(Runnable::run);
    }
  }

  @Override
  public boolean onError(Throwable error) {
    log.error("Fatal error processing transactions. Killing self.", error);
    exit(1);
    return false;
  }

  @VisibleForTesting
  void exit(int status) {
    System.exit(status);
  }

  private void handleRequest(StreamElement element, OnNextContext context) {
    String transactionId = element.getKey();
    String requestId = manager.getRequestDesc().extractSuffix(element.getAttribute());
    Optional<Request> maybeRequest = manager.getRequestDesc().valueOf(element);
    if (maybeRequest.isPresent()) {
      monitoringPolicy.incomingRequest(
          transactionId, maybeRequest.get(), element.getStamp(), context.getWatermark());
      processTransactionRequest(
          transactionId, requestId, maybeRequest.get(), context, element.getStamp());
    } else {
      log.error("Unable to parse request at offset {}", context.getOffset());
      context.confirm();
    }
  }

  private void processTransactionRequest(
      String transactionId,
      String requestId,
      Request request,
      OnNextContext context,
      long requestTimestamp) {

    if (request.getFlags() == Flags.COMMIT) {
      // enqueue this for processing after watermark
      try (var l = Locker.of(timerLock.writeLock())) {
        timers.add(
            Pair.of(
                requestTimestamp,
                () -> processTransactionUpdateRequest(transactionId, requestId, request, context)));
      }
      log.debug(
          "Scheduled transaction {} commit for watermark {}", transactionId, requestTimestamp);
    } else {
      processTransactionUpdateRequest(transactionId, requestId, request, context);
    }
  }

  private void processTransactionUpdateRequest(
      String transactionId, String requestId, Request request, OnNextContext context) {

    try {
      log.debug(
          "Processing request to {} with {} for transaction {}", requestId, request, transactionId);
      State currentState = manager.getCurrentState(transactionId);
      State newState = transitionState(transactionId, currentState, request);
      monitoringPolicy.stateUpdate(transactionId, currentState, newState);
      // we have successfully computed new state, produce response
      log.info(
          "Transaction {} transitioned to state {} from {}",
          transactionId,
          newState.getFlags(),
          currentState.getFlags());
      Response response = getResponseForNewState(request, currentState, newState);
      manager.ensureTransactionOpen(transactionId, newState);
      monitoringPolicy.outgoingResponse(transactionId, response);
      manager.writeResponseAndUpdateState(
          transactionId, newState, requestId, response, context::commit);
    } catch (Throwable err) {
      log.warn("Error during processing transaction {} request {}", transactionId, request, err);
      context.commit(false, err);
    }
  }

  private Response getResponseForNewState(Request request, State oldState, State newState) {
    switch (newState.getFlags()) {
      case OPEN:
        return (oldState.getFlags() == State.Flags.UNKNOWN
                || (oldState.getFlags() == State.Flags.ABORTED
                        || oldState.getFlags() == State.Flags.OPEN)
                    && request.getFlags() == Request.Flags.OPEN)
            ? Response.forRequest(request).open(newState.getSequentialId(), newState.getStamp())
            : Response.forRequest(request).updated();
      case COMMITTED:
        if (request.getFlags() == Request.Flags.OPEN) {
          return Response.forRequest(request).duplicate(newState.getSequentialId());
        }
        return Response.forRequest(request).committed();
      case ABORTED:
        return Response.forRequest(request).aborted();
      default:
        throw new IllegalArgumentException(
            "Cannot produce response for state " + newState.getFlags());
    }
  }

  @VisibleForTesting
  State transitionState(String transactionId, State currentState, Request request) {
    switch (currentState.getFlags()) {
      case UNKNOWN:
        if (request.getFlags() == Flags.OPEN) {
          return transitionToOpen(transactionId, request);
        }
        break;
      case OPEN:
        switch (request.getFlags()) {
          case COMMIT:
            return transitionToCommitted(transactionId, currentState, request);
          case UPDATE:
            return transitionToUpdated(currentState, request);
          case ROLLBACK:
            return transitionToAborted(currentState);
        }
        break;
      case COMMITTED:
        return currentState;
      case ABORTED:
        if (request.getFlags() == Flags.OPEN) {
          return transitionToOpen(transactionId, request);
        }
        return currentState;
    }
    return currentState.aborted();
  }

  private State transitionToAborted(State currentState) {
    metrics.getTransactionsRolledBack().increment();
    return currentState.aborted();
  }

  private State transitionToUpdated(State currentState, Request request) {
    if (!isCompatibleWildcardUpdate(currentState, request.getInputAttributes())) {
      metrics.getTransactionsRejected().increment();
      return currentState.aborted();
    }
    metrics.getTransactionsUpdated().increment();
    return currentState.update(request.getInputAttributes());
  }

  private static Map<String, Set<KeyAttribute>> getPartitionedWildcards(
      Stream<KeyAttribute> attributes) {

    return attributes
        .filter(
            ka ->
                ka.isWildcardQuery()
                    // deletes are not part of the results retrieved by wildcard query
                    || (ka.getAttributeDescriptor().isWildcard() && !ka.isDelete()))
        .collect(
            Collectors.groupingBy(TransactionLogObserver::getWildcardQueryId, Collectors.toSet()));
  }

  @Internal
  private static String getWildcardQueryId(KeyAttribute ka) {
    return ka.getKey()
        + "/"
        + ka.getEntity().getName()
        + ":"
        + ka.getAttributeDescriptor().getName();
  }

  private boolean isCompatibleWildcardUpdate(
      State state, Collection<KeyAttribute> additionalAttributes) {

    Map<String, Set<KeyAttribute>> existingWildcards =
        getPartitionedWildcards(state.getInputAttributes().stream());
    Map<String, Set<KeyAttribute>> updatesPartitioned =
        getPartitionedWildcards(additionalAttributes.stream());

    return updatesPartitioned.entrySet().stream()
        .allMatch(
            e -> {
              // either the wildcard has not been queried yet
              // or both outcomes are the same
              Set<KeyAttribute> currentKeyAttributes = existingWildcards.get(e.getKey());
              Set<KeyAttribute> updatedKeyAttributes = e.getValue();
              if (currentKeyAttributes == null) {
                // no conflict possible
                return true;
              }
              boolean currentContainQuery =
                  currentKeyAttributes.stream().anyMatch(KeyAttribute::isWildcardQuery);
              boolean updatesContainQuery =
                  updatedKeyAttributes.stream().anyMatch(KeyAttribute::isWildcardQuery);

              if (updatesContainQuery) {
                // we either did not query the data previously, or we have compatible results
                if (!Sets.difference(currentKeyAttributes, updatedKeyAttributes).isEmpty()) {
                  return false;
                }
              }
              if (currentContainQuery) {
                return Sets.difference(updatedKeyAttributes, currentKeyAttributes).isEmpty();
              }
              return true;
            });
  }

  private State transitionToCommitted(String transactionId, State currentState, Request request) {
    synchronized (commitLock) {
      List<KeyAttribute> outputKeyAttributes =
          request.getOutputs().stream()
              .map(KeyAttributes::ofStreamElement)
              .collect(Collectors.toList());
      if (!verifyNotInConflict(
          transactionId,
          currentState,
          currentState.getSequentialId(),
          currentState.getStamp(),
          concatInputsAndOutputs(currentState.getInputAttributes(), outputKeyAttributes))) {
        log.info("Transaction {} seqId {} aborted", transactionId, currentState.getSequentialId());
        metrics.getTransactionsRejected().increment();
        return currentState.aborted();
      }
      State proposedState = currentState.committed(request.getOutputs());
      transactionPostCommit(proposedState);
      log.info("Transaction {} seqId {} committed", transactionId, currentState.getSequentialId());
      metrics.getTransactionsCommitted().increment();
      return proposedState;
    }
  }

  @VisibleForTesting
  static Iterable<KeyAttribute> concatInputsAndOutputs(
      Collection<KeyAttribute> inputAttributes, Collection<KeyAttribute> outputAttributes) {

    Map<KeyWithAttribute, KeyAttribute> mapOfInputs =
        inputAttributes.stream()
            .collect(
                Collectors.toMap(
                    KeyWithAttribute::of,
                    Function.identity(),
                    (a, b) -> a.getSequentialId() < b.getSequentialId() ? a : b));
    outputAttributes.forEach(
        ka -> {
          Preconditions.checkArgument(
              !ka.isWildcardQuery(), "Got KeyAttribute %s, which is not allowed output.", ka);
          KeyWithAttribute kwa = KeyWithAttribute.of(ka);
          if (ka.getAttributeSuffix().isEmpty()) {
            mapOfInputs.putIfAbsent(kwa, ka);
          } else {
            // we can add wildcard output only if the inputs do not contain wildcard query
            Optional<KeyAttribute> wildcardKa =
                KeyAttributes.ofWildcardQueryElements(
                        ka.getEntity(),
                        ka.getKey(),
                        ka.getAttributeDescriptor(),
                        Collections.emptyList())
                    .stream()
                    .filter(KeyAttribute::isWildcardQuery)
                    .findAny();
            if (wildcardKa.isPresent()) {
              KeyWithAttribute wildcardKwa = KeyWithAttribute.of(wildcardKa.get());
              if (!mapOfInputs.containsKey(wildcardKwa)) {
                mapOfInputs.putIfAbsent(kwa, ka);
              }
            }
          }
        });
    return mapOfInputs.values();
  }

  private State transitionToOpen(String transactionId, Request request) {
    long seqId = sequenceId.getAndIncrement();
    long now = currentTimeMillis();
    State proposedState = State.open(seqId, now, new HashSet<>(request.getInputAttributes()));
    if (verifyNotInConflict(
        transactionId, proposedState, seqId, now, request.getInputAttributes())) {
      log.info("Transaction {} seqId {} is now {}", transactionId, seqId, proposedState.getFlags());
      metrics.getTransactionsOpen().increment();
      return proposedState;
    }
    log.info("Transaction {} seqId {} aborted", transactionId, seqId);
    metrics.getTransactionsRejected().increment();
    return proposedState.aborted();
  }

  private boolean verifyNotInConflict(
      String transactionId,
      State currentState,
      long transactionSeqId,
      long commitStamp,
      Iterable<KeyAttribute> attributes) {

    final boolean detailedReport =
        monitoringPolicy.shouldReportRejected(transactionId, currentState);

    if (checkRequiredWildcards(transactionId, attributes, detailedReport)) {
      return false;
    }

    if (containsUnequalSeqIds(transactionId, attributes, detailedReport)) {
      return false;
    }

    return verifyInputsUpToDate(
        transactionId, transactionSeqId, commitStamp, attributes, detailedReport);
  }

  private boolean verifyInputsUpToDate(
      String transactionId,
      long transactionSeqId,
      long commitStamp,
      Iterable<KeyAttribute> attributes,
      boolean detailedReport) {

    try (var l = Locker.of(this.lock.readLock())) {
      return Streams.stream(attributes)
          .filter(ka -> !ka.isWildcardQuery())
          .noneMatch(
              ka -> {
                if (!ka.isDelete()
                    && ka.getSequentialId() < Long.MAX_VALUE
                    && ka.getSequentialId() > transactionSeqId) {
                  // disallow any (well-defined) sequenceId with higher value than current
                  // transaction
                  if (detailedReport) {
                    log.info("Transaction {} has invalid seqId in {}", transactionId, ka);
                  }
                  return true;
                }
                SortedSet<SeqIdWithTombstone> updated =
                    lastUpdateSeqId.get(KeyWithAttribute.of(ka));
                if (updated.isEmpty()) {
                  return false;
                }
                SeqIdWithTombstone lastUpdated = updated.last();
                long lastUpdatedSeqId = lastUpdated.getSeqId();
                long lastUpdatedStamp = lastUpdated.getTimestamp();
                boolean outdatedRead =
                    lastUpdatedSeqId > transactionSeqId
                        || lastUpdatedStamp > commitStamp
                        || (lastUpdatedSeqId > ka.getSequentialId()
                            // we can accept somewhat stale data if the state is equal => both agree
                            // that the field was deleted
                            && (!lastUpdated.isTombstone() || !ka.isDelete()));
                if (outdatedRead && detailedReport) {
                  log.info(
                      "Transaction {} has outdated read in {}, last write was {}",
                      transactionId,
                      ka,
                      lastUpdated);
                }
                return outdatedRead;
              });
    }
  }

  private boolean checkRequiredWildcards(
      String transactionId, Iterable<KeyAttribute> attributes, boolean detailedReport) {

    final Set<KeyWithAttribute> requiredWildcards;
    try (var l = Locker.of(this.lock.readLock())) {
      requiredWildcards =
          Streams.stream(attributes)
              .filter(KeyAttribute::isWildcardQuery)
              .map(KeyWithAttribute::of)
              .map(updatesToWildcard::get)
              .filter(Objects::nonNull)
              .map(Map::entrySet)
              .flatMap(Collection::stream)
              .filter(e -> lastIsNotTombstone(lastUpdateSeqId.get(e.getKey())))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());
    }

    if (!requiredWildcards.isEmpty()) {
      Set<KeyWithAttribute> presentInputs =
          Streams.stream(attributes)
              .filter(ka -> !ka.isWildcardQuery())
              .filter(ka -> ka.getAttributeDescriptor().isWildcard())
              .map(KeyWithAttribute::of)
              .collect(Collectors.toSet());
      if (!Sets.difference(requiredWildcards, presentInputs).isEmpty()) {
        // not all required inputs present
        if (detailedReport) {
          log.info(
              "Transaction {} is missing required wildcards {}",
              transactionId,
              Sets.difference(requiredWildcards, presentInputs));
        }
        return true;
      }
    }
    return false;
  }

  private boolean containsUnequalSeqIds(
      String transactionId, Iterable<KeyAttribute> attributes, boolean detailedReport) {

    Map<KeyWithAttribute, Long> seqIds = new HashMap<>();
    for (KeyAttribute ka : attributes) {
      Long current = seqIds.putIfAbsent(KeyWithAttribute.of(ka), ka.getSequentialId());
      if (current != null && current != ka.getSequentialId()) {
        if (detailedReport) {
          log.info("Transaction {} contains unequal seqIds in {}", transactionId, attributes);
        }
        return true;
      }
    }
    return false;
  }

  private static boolean lastIsNotTombstone(
      @Nullable SortedSet<SeqIdWithTombstone> seqIdWithTombstones) {

    return seqIdWithTombstones == null
        || seqIdWithTombstones.isEmpty()
        || !seqIdWithTombstones.last().isTombstone();
  }

  private void stateUpdate(StreamElement newUpdate, Pair<Long, Object> oldValue) {
    if (newUpdate.getAttributeDescriptor().equals(manager.getStateDesc())) {
      if (!newUpdate.isDelete()) {
        State state = Optionals.get(manager.getStateDesc().valueOf(newUpdate));
        log.debug("New state update for transaction {}: {}", newUpdate.getKey(), state);
        sequenceId.accumulateAndGet(state.getSequentialId() + 1, Math::max);
        manager.ensureTransactionOpen(newUpdate.getKey(), state);
        if (state.getFlags() == State.Flags.COMMITTED) {
          transactionPostCommit(state);
        }
      }
    }
  }

  @VisibleForTesting
  void transactionPostCommit(State state) {
    long committedSeqId = state.getSequentialId();
    long committedStamp = state.getStamp();
    log.debug(
        "Storing committed outputs {} of transaction seqId {}",
        state.getCommittedOutputs(),
        committedSeqId);

    try (var l = Locker.of(this.lock.writeLock())) {
      state
          .getCommittedOutputs()
          .forEach(
              element -> {
                KeyAttribute ka = KeyAttributes.ofStreamElement(element);
                KeyWithAttribute kwa = KeyWithAttribute.of(ka);
                SeqIdWithTombstone seqIdWithTombstone =
                    SeqIdWithTombstone.create(committedSeqId, committedStamp, element.isDelete());
                lastUpdateSeqId.put(kwa, seqIdWithTombstone);
                if (element.getAttributeDescriptor().isWildcard()) {
                  Map<KeyWithAttribute, SeqIdWithTombstone> updated =
                      updatesToWildcard.computeIfAbsent(
                          KeyWithAttribute.ofWildcard(ka), k -> new HashMap<>());
                  updated.compute(
                      kwa,
                      (k, current) ->
                          current == null || current.getSeqId() < seqIdWithTombstone.getSeqId()
                              ? seqIdWithTombstone
                              : current);
                }
              });
      metrics.getNumWritesCached().increment(lastUpdateSeqId.size());
    }
  }

  public void run(String name) {
    manager.runObservations(name, this::stateUpdate, this);
  }

  @VisibleForTesting
  public ServerTransactionManager getRawManager() {
    return unsynchronizedManager;
  }
}
