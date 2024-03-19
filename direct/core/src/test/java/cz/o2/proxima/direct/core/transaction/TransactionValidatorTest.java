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

import static cz.o2.proxima.direct.core.transaction.TransactionResourceManagerTest.runObservations;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.TransactionMode;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Request;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.Response.Flags;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.core.util.TransformationRunner;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.Transaction;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionPreconditionFailedException;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionValidator;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionValidatorTest {

  private final Repository repo =
      Repository.ofTest(
          ConfigFactory.parseResources("test-validator.conf")
              .withFallback(ConfigFactory.load("test-transactions.conf"))
              .resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<Integer> intField = Regular.of(gateway, gateway.getAttribute("intField"));
  private DirectDataOperator direct;
  private ServerTransactionManager manager;
  private UnaryFunction<Request, Response> toReturn;
  private AtomicLong seqId;

  @Before
  public void setUp() {
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    manager = direct.getServerTransactionManager();
    seqId = new AtomicLong();
    runObservations(
        manager,
        "test",
        (ingest, context) -> {
          String transactionId = ingest.getKey();
          if (ingest.getAttributeDescriptor().equals(manager.getRequestDesc())) {
            String responseId = manager.getRequestDesc().extractSuffix(ingest.getAttribute());
            Request request = Optionals.get(manager.getRequestDesc().valueOf(ingest));
            Response response = toReturn.apply(request);
            State state = manager.getCurrentState(transactionId);
            if (response.getFlags() == Response.Flags.OPEN) {
              state =
                  State.open(
                      response.getSeqId(), response.getStamp(), request.getInputAttributes());
            } else if (response.getFlags() == Response.Flags.COMMITTED) {
              state = state.committed(request.getOutputs());
            }
            manager.writeResponseAndUpdateState(
                transactionId, state, responseId, response, context::commit);
          } else {
            context.confirm();
          }
          return true;
        });
    TransformationRunner.runTransformations(repo, direct);
  }

  @After
  public void tearDown() {
    manager.close();
    direct.close();
  }

  @Test
  public void testWriteNonDuplicate() {
    this.toReturn = this::getResponse;
    assertNull(write("k1", 1));
    assertNull(write("k2", 2));
  }

  @Test(timeout = 10_000)
  public void testWriteNonDuplicateRejected() {
    AtomicBoolean oneCommitted = new AtomicBoolean();
    this.toReturn =
        request -> {
          if (!oneCommitted.get()) {
            Response response = getResponse(request);
            if (response.getFlags() == Flags.COMMITTED) {
              oneCommitted.set(true);
            }
            return response;
          }
          return getRejectedResponse(request);
        };
    assertNull(write("k1", 1));
    assertTrue(write("k2", 2) instanceof TransactionRejectedException);
  }

  @Test
  public void testWriteDuplicate() {
    this.toReturn = this::getResponse;
    assertNull(write("k1", 1));
    assertTrue(write("k2", 1) instanceof TransactionPreconditionFailedException);
  }

  @Test
  public void testTransactionValidatorRejected() throws InterruptedException {
    StreamElement element = intField.upsert("key", System.currentTimeMillis(), 1);
    TransactionValidator validator =
        new TransactionValidator() {
          @Override
          public void validate(StreamElement element, Transaction transaction)
              throws TransactionPreconditionFailedException, TransactionRejectedException {

            throw new TransactionRejectedException("t", Flags.ABORTED) {};
          }

          @Override
          public void close() {}
        };
    validator.setup(repo, direct, Collections.emptyMap());

    try (Transaction t = mock(Transaction.class)) {
      validator.setTransaction(t);
      BlockingQueue<Throwable> err = new ArrayBlockingQueue<>(1);
      validator.transform(element, (succ, exc) -> err.add(exc));
      Throwable thrown = err.take();
      assertNotNull(thrown);
      assertTrue(thrown instanceof TransactionRejectedException);
    }
  }

  @Test
  public void testTransactionValidatorRetryFailed() throws InterruptedException {
    StreamElement element = intField.upsert("key", System.currentTimeMillis(), 1);
    TransactionValidator validator =
        new TransactionValidator() {
          @Override
          public void validate(StreamElement element, Transaction transaction)
              throws TransactionPreconditionFailedException, TransactionRejectedException {

            throw new TransactionRejectedException("t", Flags.ABORTED) {};
          }

          @Override
          public void close() {}
        };
    validator.setup(repo, direct, Collections.emptyMap());

    try (Transaction t = mock(Transaction.class)) {
      validator.setTransaction(t);
      BlockingQueue<Throwable> err = new ArrayBlockingQueue<>(1);
      validator.transform(element, (succ, exc) -> err.add(exc));
      Throwable thrown = err.take();
      assertNotNull(thrown);
      assertTrue(thrown instanceof TransactionRejectedException);
    }
  }

  private Response getRejectedResponse(Request request) {
    switch (request.getFlags()) {
      case COMMIT:
        return Response.forRequest(request).committed();
      case OPEN:
        return Response.forRequest(request)
            .open(seqId.incrementAndGet(), System.currentTimeMillis());
      case UPDATE:
        return Response.forRequest(request).aborted();
    }
    throw new IllegalArgumentException("Invalid request " + request);
  }

  private Response getResponse(Request request) {
    switch (request.getFlags()) {
      case COMMIT:
        return Response.forRequest(request).committed();
      case OPEN:
        return Response.forRequest(request)
            .open(seqId.incrementAndGet(), System.currentTimeMillis());
      case UPDATE:
        return Response.forRequest(request).updated();
    }
    throw new IllegalArgumentException("Invalid request " + request);
  }

  private Throwable write(String key, int value) {
    assertEquals(TransactionMode.ALL, intField.getTransactionMode());
    BlockingQueue<Pair<Boolean, Throwable>> err = new ArrayBlockingQueue<>(1);
    Optionals.get(direct.getWriter(intField))
        .write(
            intField.upsert(key, System.currentTimeMillis(), value),
            (succ, exc) -> ExceptionUtils.unchecked(() -> err.add(Pair.of(succ, exc))));
    Pair<Boolean, Throwable> status = ExceptionUtils.uncheckedFactory(err::take);
    if (!status.getFirst()) {
      return status.getSecond();
    }
    return null;
  }
}
