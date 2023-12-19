/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterators;
import cz.o2.proxima.internal.com.google.common.collect.MultimapBuilder;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.internal.com.google.common.collect.SortedSetMultimap;
import cz.o2.proxima.internal.com.google.common.collect.Streams;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  private final ServerTransactionManager unsynchronizedManager;
  private final ServerTransactionManager manager;
  private final AtomicLong sequenceId = new AtomicLong();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Object commitLock = new Object();

  @GuardedBy("lock")
  private final SortedSetMultimap<KeyWithAttribute, SeqIdWithTombstone> lastUpdateSeqId =
      MultimapBuilder.hashKeys().treeSetValues().build();

  @GuardedBy("lock")
  private final Map<KeyWithAttribute, Map<KeyWithAttribute, SeqIdWithTombstone>> updatesToWildcard =
      new HashMap<>();

  public TransactionLogObserver(DirectDataOperator direct) {
    this.direct = direct;
    this.unsynchronizedManager = getServerTransactionManager(direct);
    this.manager = synchronizedManager(unsynchronizedManager);
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
                  try (Locker l = Locker.of(lock.writeLock())) {
                    List<Map.Entry<KeyWithAttribute, SeqIdWithTombstone>> toCleanUp =
                        lastUpdateSeqId.entries().stream()
                            .filter(e -> e.getValue().getTimestamp() < cleanup)
                            .collect(Collectors.toList());
                    cleaned = toCleanUp.size();
                    toCleanUp.forEach(e -> lastUpdateSeqId.remove(e.getKey(), e.getValue()));
                  }
                  // release and re-acquire lock to enable progress of any waiting threads
                  try (Locker l = Locker.of(lock.writeLock())) {
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
      handleRequest(
          element.getKey(),
          requestDesc.extractSuffix(element.getAttribute()),
          context,
          requestDesc.valueOf(element).orElse(null));
    } else {
      // unknown attribute, probably own response or state update, can be safely ignored
      log.debug("Unknown attribute {}. Ignored.", element.getAttributeDescriptor());
      context.confirm();
    }
    return true;
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

  private void handleRequest(
      String transactionId,
      String requestId,
      OnNextContext context,
      @Nullable Request maybeRequest) {

    if (maybeRequest != null) {
      processTransactionRequest(transactionId, requestId, maybeRequest, context);
    } else {
      log.error("Unable to parse request at offset {}", context.getOffset());
      context.confirm();
    }
  }

  private void processTransactionRequest(
      String transactionId, String requestId, Request request, OnNextContext context) {

    log.debug(
        "Processing request to {} with {} for transaction {}", requestId, request, transactionId);
    State currentState = manager.getCurrentState(transactionId);
    @Nullable State newState = transitionState(transactionId, currentState, request);
    if (newState != null) {
      // we have successfully computed new state, produce response
      Response response = getResponseForNewState(request, currentState, newState);
      manager.ensureTransactionOpen(transactionId, newState);
      manager.writeResponseAndUpdateState(
          transactionId, newState, requestId, response, context::commit);
    } else if (request.getFlags() == Request.Flags.OPEN
        && (currentState.getFlags() == State.Flags.OPEN
            || currentState.getFlags() == State.Flags.COMMITTED)) {

      manager.writeResponseAndUpdateState(
          transactionId,
          currentState,
          requestId,
          Response.forRequest(request).duplicate(currentState.getSequentialId()),
          context::commit);
    } else {
      log.warn(
          "Unexpected {} request for transaction {} seqId {} when the state is {}. "
              + "Refusing to respond, because the correct response is unknown.",
          request.getFlags(),
          transactionId,
          currentState.getSequentialId(),
          currentState.getFlags());
      context.confirm();
    }
  }

  private void abortTransaction(String transactionId, State state) {
    long seqId = state.getSequentialId();
    // we need to rollback all updates to lastUpdateSeqId with the same seqId
    try (Locker lock = Locker.of(this.lock.writeLock())) {
      state.getCommittedAttributes().stream()
          .map(KeyWithAttribute::ofWildcard)
          .map(updatesToWildcard::get)
          .filter(Objects::nonNull)
          .forEach(
              map ->
                  Iterators.removeIf(
                      map.entrySet().iterator(), e -> e.getValue().getSeqId() == seqId));
      state.getCommittedAttributes().stream()
          .map(KeyWithAttribute::of)
          .forEach(kwa -> lastUpdateSeqId.get(kwa).removeIf(s -> s.getSeqId() == seqId));
    }
  }

  private Response getResponseForNewState(Request request, State oldState, State state) {
    switch (state.getFlags()) {
      case OPEN:
        return (oldState.getFlags() == State.Flags.UNKNOWN
                || oldState.getFlags() == State.Flags.OPEN
                    && request.getFlags() == Request.Flags.OPEN)
            ? Response.forRequest(request).open(state.getSequentialId(), state.getStamp())
            : Response.forRequest(request).updated();
      case COMMITTED:
        return Response.forRequest(request).committed();
      case ABORTED:
        return Response.forRequest(request).aborted();
    }
    throw new IllegalArgumentException("Cannot produce response for state " + state.getFlags());
  }

  @VisibleForTesting
  @Nullable
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
            abortTransaction(transactionId, currentState);
            return currentState.aborted();
        }
        break;
      case COMMITTED:
        if (request.getFlags() == Request.Flags.ROLLBACK) {
          return transitionToAborted(transactionId, currentState);
        }
        break;
      case ABORTED:
        if (request.getFlags() == Request.Flags.ROLLBACK) {
          return currentState;
        }
        break;
    }
    return null;
  }

  private State transitionToAborted(String transactionId, State state) {
    log.info("Transaction {} seqId {} rolled back", transactionId, state.getSequentialId());
    abortTransaction(transactionId, state);
    return state.aborted();
  }

  private State transitionToUpdated(State currentState, Request request) {
    return currentState.update(request.getInputAttributes());
  }

  private State transitionToCommitted(String transactionId, State currentState, Request request) {
    synchronized (commitLock) {
      if (!verifyNotInConflict(
          currentState.getSequentialId(),
          currentState.getStamp(),
          concatInputsAndOutputs(
              currentState.getInputAttributes(), request.getOutputAttributes()))) {
        log.info("Transaction {} seqId {} aborted", transactionId, currentState.getSequentialId());
        return currentState.aborted();
      }
      State proposedState = currentState.committed(request.getOutputAttributes());
      transactionPostCommit(proposedState);
      log.info("Transaction {} seqId {} committed", transactionId, currentState.getSequentialId());
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
                    (a, b) -> a.getSequenceId() < b.getSequenceId() ? a : b));
    outputAttributes.forEach(
        ka -> {
          Preconditions.checkArgument(
              !ka.isWildcardQuery(), "Got KeyAttribute %s, which is not allowed output.", ka);
          KeyWithAttribute kwa = KeyWithAttribute.of(ka);
          if (!ka.getAttributeSuffix().isPresent()) {
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
    if (verifyNotInConflict(seqId, now, request.getInputAttributes())) {
      log.info("Transaction {} seqId {} is now {}", transactionId, seqId, proposedState.getFlags());
      return proposedState;
    }
    log.info("Transaction {} seqId {} aborted", transactionId, seqId);
    return proposedState.aborted();
  }

  private boolean verifyNotInConflict(
      long transactionSeqId, long commitStamp, Iterable<KeyAttribute> attributes) {

    final Set<KeyWithAttribute> requiredWildcards;
    try (Locker lock = Locker.of(this.lock.readLock())) {
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
        return false;
      }
    }

    try (Locker lock = Locker.of(this.lock.readLock())) {
      return Streams.stream(attributes)
          .filter(ka -> !ka.isWildcardQuery())
          .noneMatch(
              ka -> {
                if (!ka.isDelete()
                    && ka.getSequenceId() < Long.MAX_VALUE
                    && ka.getSequenceId() > transactionSeqId) {
                  // disallow any (well-defined) sequenceId with higher value than current
                  // transaction
                  return true;
                }
                SortedSet<SeqIdWithTombstone> lastUpdated =
                    lastUpdateSeqId.get(KeyWithAttribute.of(ka));
                if (lastUpdated == null || lastUpdated.isEmpty()) {
                  return false;
                }
                long lastUpdatedSeqId = lastUpdated.last().getSeqId();
                long lastUpdatedStamp = lastUpdated.last().getTimestamp();
                return lastUpdatedSeqId > transactionSeqId
                    || lastUpdatedStamp == commitStamp
                    || (lastUpdatedSeqId > ka.getSequenceId()
                        // we can accept somewhat stale data if the state is equal => both agree
                        // that the field was deleted
                        && (!lastUpdated.last().isTombstone() || !ka.isDelete()));
              });
    }
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
        } else if (state.getFlags() == State.Flags.ABORTED) {
          abortTransaction(newUpdate.getKey(), state);
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
        state.getCommittedAttributes(),
        committedSeqId);

    try (Locker lock = Locker.of(this.lock.writeLock())) {
      state
          .getCommittedAttributes()
          .forEach(
              ka -> {
                KeyWithAttribute kwa = KeyWithAttribute.of(ka);
                SeqIdWithTombstone seqIdWithTombstone =
                    SeqIdWithTombstone.create(committedSeqId, committedStamp, ka.isDelete());
                lastUpdateSeqId.put(kwa, seqIdWithTombstone);
                if (ka.getAttributeDescriptor().isWildcard()) {
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
