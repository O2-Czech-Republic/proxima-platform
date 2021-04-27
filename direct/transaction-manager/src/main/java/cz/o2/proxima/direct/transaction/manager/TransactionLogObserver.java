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
package cz.o2.proxima.direct.transaction.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.ServerTransactionManager;
import cz.o2.proxima.direct.transaction.TransactionManager;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link LogObserver} performing the overall transaction logic via keeping state of transactions
 * and responding to requests.
 */
@Slf4j
class TransactionLogObserver implements LogObserver {

  @Value
  private static class KeyWithAttribute {

    static KeyWithAttribute of(KeyAttribute ka) {
      if (ka.isWildcardQuery()) {
        return new KeyWithAttribute(ka.getKey(), ka.getAttributeDescriptor().toAttributePrefix());
      }
      return new KeyWithAttribute(
          ka.getKey(), ka.getAttribute().orElse(ka.getAttributeDescriptor().getName()));
    }

    static KeyWithAttribute ofWildcard(KeyAttribute ka) {
      return new KeyWithAttribute(ka.getKey(), ka.getAttributeDescriptor().toAttributePrefix());
    }

    String key;
    String attribute;
  }

  private final DirectDataOperator direct;
  private final ServerTransactionManager manager;
  private final AtomicLong sequenceId = new AtomicLong(1000L);
  private final Map<KeyWithAttribute, Long> lastUpdateSeqId = new HashMap<>();
  private final Map<KeyWithAttribute, List<KeyWithAttribute>> updatesToWildcard = new HashMap<>();

  TransactionLogObserver(DirectDataOperator direct) {
    this.direct = direct;
    this.manager = TransactionManager.server(direct);
  }

  @Override
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    log.debug("Received element {} for transaction processing", ingest);
    Wildcard<Request> requestDesc = manager.getRequestDesc();
    if (ingest.getAttributeDescriptor().equals(requestDesc)) {
      handleRequest(
          ingest.getKey(),
          requestDesc.extractSuffix(ingest.getAttribute()),
          requestDesc.valueOf(ingest),
          context);
    } else {
      // unknown attribute, probably own response or state update, can be safely ignored
      log.debug("Unknown attribute {}. Ignored.", ingest.getAttributeDescriptor());
      context.confirm();
    }
    return true;
  }

  private void handleRequest(
      String transactionId,
      String requestId,
      Optional<Request> maybeRequest,
      OnNextContext context) {

    if (maybeRequest.isPresent()) {
      processTransactionRequest(transactionId, requestId, maybeRequest.get(), context);
    } else {
      log.error("Unable to parse request at offset {}", context.getOffset());
      context.confirm();
    }
  }

  private void processTransactionRequest(
      String transactionId, String requestId, Request request, OnNextContext context) {

    log.debug("Processing request {} for transaction {}", requestId, transactionId);
    State currentState = manager.getCurrentState(transactionId);
    @Nullable State newState = transitionState(currentState, request);

    if (newState != null) {
      // we have successfully computed new state, produce response
      Response response = getResponseForNewState(currentState, newState);
      if (response.getFlags() == Response.Flags.OPEN) {
        // we need to advance our sequenceId for new transaction
        long seqId = response.getSeqId();
      }
      CommitCallback commitCallback = CommitCallback.afterNumCommits(2, context::commit);
      manager.setCurrentState(transactionId, newState, commitCallback);
      manager.writeResponse(transactionId, requestId, response, commitCallback);
    } else {
      // we cannot transition from current state
      if (request.getFlags() == Request.Flags.OPEN
          && (currentState.getFlags() == State.Flags.OPEN
              || currentState.getFlags() == State.Flags.COMMITTED)) {
        manager.writeResponse(transactionId, requestId, Response.duplicate(), context::commit);
      } else {
        log.warn(
            "Unexpected OPEN request for transaction {} when the state is {}",
            transactionId,
            currentState.getFlags());
        manager.writeResponse(transactionId, requestId, Response.aborted(), context::commit);
      }
    }
  }

  private Response getResponseForNewState(State oldState, State state) {
    switch (state.getFlags()) {
      case OPEN:
        return oldState.getFlags() == State.Flags.UNKNOWN
            ? Response.open(state.getSequentialId())
            : Response.updated();
      case COMMITTED:
        return Response.committed();
      case ABORTED:
        return Response.aborted();
    }
    throw new IllegalArgumentException("Cannot produce response for state " + state.getFlags());
  }

  @VisibleForTesting
  @Nullable
  State transitionState(State currentState, Request request) {
    switch (currentState.getFlags()) {
      case UNKNOWN:
        if (request.getFlags() == Request.Flags.OPEN) {
          long seqId = sequenceId.getAndIncrement();
          State proposedState = State.open(seqId, new HashSet<>(request.getInputAttributes()));
          if (notInConflict(request.getInputAttributes())) {
            return proposedState;
          }
          return proposedState.aborted();
        }
        break;
      case OPEN:
        if (request.getFlags() == Request.Flags.COMMIT) {
          if (!notInConflict(currentState.getInputAttributes())) {
            return currentState.aborted();
          }
          State proposedState = currentState.committed(request.getOutputAttributes());
          transactionPostCommit(proposedState);
          return proposedState;
        } else if (request.getFlags() == Request.Flags.UPDATE) {
          HashSet<KeyAttribute> newAttributes =
              new HashSet<>(currentState.getCommittedAttributes());
          newAttributes.addAll(request.getInputAttributes());
          return currentState.update(newAttributes);
        }
        break;
    }
    return null;
  }

  private boolean notInConflict(Collection<KeyAttribute> inputAttributes) {

    Set<KeyWithAttribute> requiredInputs =
        inputAttributes
            .stream()
            .filter(KeyAttribute::isWildcardQuery)
            .map(KeyWithAttribute::of)
            .map(updatesToWildcard::get)
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .collect(Collectors.toSet());

    if (!requiredInputs.isEmpty()) {
      Set<KeyWithAttribute> presentInputs =
          inputAttributes
              .stream()
              .filter(ka -> !ka.isWildcardQuery())
              .filter(ka -> ka.getAttributeDescriptor().isWildcard())
              .map(KeyWithAttribute::of)
              .collect(Collectors.toSet());
      if (!Sets.difference(requiredInputs, presentInputs).isEmpty()) {
        // not all required inputs present
        return false;
      }
    }

    return inputAttributes
        .stream()
        .filter(ka -> !ka.isWildcardQuery())
        .noneMatch(
            ka -> {
              Long lastUpdated = lastUpdateSeqId.get(KeyWithAttribute.of(ka));
              return lastUpdated != null && lastUpdated > ka.getSequenceId();
            });
  }

  private void stateUpdate(StreamElement newUpdate, Pair<Long, Object> oldValue) {
    // FIXME: reconstruct the state
  }

  private void transactionPostCommit(State state) {
    long committedSeqId = state.getSequentialId();
    Set<KeyWithAttribute> committedAttributes =
        state
            .getCommittedAttributes()
            .stream()
            .map(KeyWithAttribute::of)
            .collect(Collectors.toSet());

    state
        .getCommittedAttributes()
        .forEach(
            ka -> {
              KeyWithAttribute kwa = KeyWithAttribute.of(ka);
              lastUpdateSeqId.compute(
                  kwa, (k, v) -> v == null ? committedSeqId : Math.max(v, committedSeqId));
              if (ka.getAttributeDescriptor().isWildcard()) {
                List<KeyWithAttribute> list =
                    updatesToWildcard.computeIfAbsent(
                        KeyWithAttribute.ofWildcard(ka), k -> new ArrayList<>());
                list.add(kwa);
              }
            });
  }

  public void run(String name) {
    manager.runObservations(name, this::stateUpdate, this);
  }
}
