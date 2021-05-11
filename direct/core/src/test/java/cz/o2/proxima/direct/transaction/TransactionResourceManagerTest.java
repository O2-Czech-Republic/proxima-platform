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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.TransactionResourceManager.CachedTransaction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

/** Test transactions are working according to the specification. */
public class TransactionResourceManagerTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-transactions.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<?> status = gateway.getAttribute("status");
  private final EntityDescriptor transaction = repo.getEntity("_transaction");
  private final Wildcard<Request> requestDesc =
      Wildcard.wildcard(transaction, transaction.getAttribute("request.*"));
  private final Wildcard<Response> response =
      Wildcard.wildcard(transaction, transaction.getAttribute("response.*"));

  @Test(expected = IllegalArgumentException.class)
  public void testDirectWriterFetchFails() {
    direct.getWriter(requestDesc);
  }

  @Test
  public void testTransactionRequestResponse() {
    try (TransactionResourceManager manager = TransactionResourceManager.of(direct)) {
      String transactionId = UUID.randomUUID().toString();
      List<Pair<String, Response>> receivedResponses = new ArrayList<>();

      // create a simple ping-pong communication
      manager.runObservations(
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              assertEquals(1, request.getInputAttributes().size());
              CountDownLatch latch = new CountDownLatch(1);
              manager.setCurrentState(
                  key,
                  State.open(1L, new HashSet<>(request.getInputAttributes())),
                  (succ, exc) -> {
                    latch.countDown();
                  });
              ExceptionUtils.ignoringInterrupted(latch::await);
              manager.writeResponse(key, requestId, Response.open(1L), context::commit);
            } else {
              context.confirm();
            }
            return true;
          });

      manager.begin(
          transactionId,
          (k, v) -> receivedResponses.add(Pair.of(k, v)),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)));

      assertEquals(1, receivedResponses.size());
      assertEquals(Response.Flags.OPEN, receivedResponses.get(0).getSecond().getFlags());

      State state = manager.getCurrentState(transactionId);
      assertNotNull(state);
      assertEquals(State.Flags.OPEN, state.getFlags());
    }
  }

  @Test
  public void testTransactionRequestCommit() throws InterruptedException {
    try (TransactionResourceManager manager = TransactionResourceManager.of(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> receivedResponses = new ArrayBlockingQueue<>(1);

      // create a simple ping-pong communication
      manager.runObservations(
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  CommitCallback.afterNumCommits(
                      2,
                      (succ, exc) -> {
                        latch.countDown();
                        context.commit(succ, exc);
                      });
              if (request.getFlags() == Request.Flags.COMMIT) {
                manager.setCurrentState(
                    key,
                    State.open(1L, Collections.emptyList())
                        .committed(new HashSet<>(request.getOutputAttributes())),
                    commit);
                manager.writeResponse(key, requestId, Response.committed(), commit);
              } else {
                manager.setCurrentState(
                    key, State.open(1L, new HashSet<>(request.getInputAttributes())), commit);
                manager.writeResponse(key, requestId, Response.open(1L), commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      manager.begin(
          transactionId,
          (k, v) -> receivedResponses.add(Pair.of(k, v)),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)));

      receivedResponses.take();
      manager.commit(
          transactionId,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)));

      Pair<String, Response> response = receivedResponses.take();
      assertEquals("commit", response.getFirst());
      assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
    }
  }

  @Test
  public void testTransactionRequestRollback() throws InterruptedException {
    try (TransactionResourceManager manager = TransactionResourceManager.of(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> receivedResponses = new ArrayBlockingQueue<>(1);

      // create a simple ping-pong communication
      manager.runObservations(
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  CommitCallback.afterNumCommits(
                      2,
                      (succ, exc) -> {
                        latch.countDown();
                        context.commit(succ, exc);
                      });
              if (request.getFlags() == Request.Flags.ROLLBACK) {
                manager.setCurrentState(key, null, commit);
                manager.writeResponse(key, requestId, Response.aborted(), commit);
              } else if (request.getFlags() == Request.Flags.OPEN) {
                manager.setCurrentState(
                    key, State.open(1L, new HashSet<>(request.getInputAttributes())), commit);
                manager.writeResponse(key, requestId, Response.open(1L), commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      manager.begin(
          transactionId,
          (k, v) -> receivedResponses.add(Pair.of(k, v)),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)));

      receivedResponses.take();
      manager.rollback(transactionId);

      Pair<String, Response> response = receivedResponses.take();
      assertEquals("rollback", response.getFirst());
      assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());

      State currentState = manager.getCurrentState(transactionId);
      assertEquals(State.Flags.UNKNOWN, currentState.getFlags());
    }
  }

  @Test
  public void testTransactionRequestUpdate() throws InterruptedException {
    try (TransactionResourceManager manager = TransactionResourceManager.of(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> receivedResponses = new ArrayBlockingQueue<>(1);

      // create a simple ping-pong communication
      manager.runObservations(
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  CommitCallback.afterNumCommits(
                      2,
                      (succ, exc) -> {
                        latch.countDown();
                        context.commit(succ, exc);
                      });
              if (request.getFlags() == Request.Flags.UPDATE) {
                manager.setCurrentState(
                    key,
                    State.open(1L, Collections.emptyList())
                        .update(new HashSet<>(request.getInputAttributes())),
                    commit);
                manager.writeResponse(key, requestId, Response.updated(), commit);
              } else {
                manager.setCurrentState(
                    key, State.open(1L, new HashSet<>(request.getInputAttributes())), commit);
                manager.writeResponse(key, requestId, Response.open(1L), commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      manager.begin(
          transactionId,
          (k, v) -> receivedResponses.add(Pair.of(k, v)),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)));

      receivedResponses.take();
      manager.updateTransaction(
          transactionId,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(gateway, "gw2", status, 1L)));

      Pair<String, Response> response = receivedResponses.take();
      assertEquals("update", response.getFirst());
      assertEquals(Response.Flags.UPDATED, response.getSecond().getFlags());
    }
  }

  @Test
  public void testCreateCachedTransactionWhenMissing() {
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "g", status, 1L);
    try (TransactionResourceManager manager = TransactionResourceManager.of(direct)) {
      CachedTransaction transaction =
          manager.createCachedTransaction(
              "transaction", State.open(1L, Collections.singletonList(ka)));
      assertEquals("transaction", transaction.getTransactionId());
    }
    try (TransactionResourceManager manager = TransactionResourceManager.of(direct)) {
      CachedTransaction transaction =
          manager.createCachedTransaction(
              "transaction",
              State.open(2L, Collections.emptyList()).committed(Collections.singletonList(ka)));
      assertEquals("transaction", transaction.getTransactionId());
    }
  }
}
