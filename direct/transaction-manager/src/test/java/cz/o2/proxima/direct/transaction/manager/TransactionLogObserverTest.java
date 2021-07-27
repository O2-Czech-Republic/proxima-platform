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

import static org.junit.Assert.*;

import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.ClientTransactionManager;
import cz.o2.proxima.direct.transaction.ServerTransactionManager;
import cz.o2.proxima.direct.transaction.TransactionResourceManager;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Test;

/** Test suite for {@link TransactionLogObserver}. */
public class TransactionLogObserverTest {

  private final Config conf =
      ConfigFactory.defaultApplication()
          .withFallback(ConfigFactory.load("test-transactions.conf"))
          .resolve();
  private final Repository repo = Repository.ofTest(conf);
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final EntityDescriptor user = repo.getEntity("user");
  private final AttributeDescriptor<byte[]> gatewayStatus = gateway.getAttribute("status");
  private final Wildcard<byte[]> userGateways = Wildcard.of(user, user.getAttribute("gateway.*"));
  private final EntityDescriptor transaction = repo.getEntity("_transaction");
  private final Wildcard<Request> request =
      Wildcard.of(transaction, transaction.getAttribute("request.*"));
  private final Wildcard<Response> response =
      Wildcard.of(transaction, transaction.getAttribute("response.*"));
  private long now;
  private TransactionLogObserver observer;

  public void createObserver() {
    createObserver(new TransactionLogObserverFactory.Default());
  }

  private void createObserver(TransactionLogObserverFactory factory) {
    now = System.currentTimeMillis();
    observer = factory.create(direct);
    observer.run(getClass().getSimpleName());
  }

  @After
  public void tearDown() {
    direct.close();
  }

  @Test
  public void testErrorExits() {
    try {
      new Rethrowing().create(direct).onError(new RuntimeException("error"));
      fail("Should have thrown exception");
    } catch (RuntimeException ex) {
      assertEquals("System.exit(1)", ex.getMessage());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransaction() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
    Pair<String, Response> response = responseQueue.take();
    assertEquals("open", response.getFirst());
    assertEquals(Response.Flags.OPEN, response.getSecond().getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommit() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
    responseQueue.take();
    clientManager.commit(
        transactionId,
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
    Pair<String, Response> response = responseQueue.take();
    assertEquals("commit", response.getFirst());
    assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionDuplicate() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
    Pair<String, Response> firstResponse = responseQueue.take();
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
    Pair<String, Response> response = responseQueue.take();
    assertEquals("open", response.getFirst());
    assertEquals(Response.Flags.DUPLICATE, response.getSecond().getFlags());
    assertEquals(firstResponse.getSecond().getSeqId(), response.getSecond().getSeqId());
  }

  @Test(timeout = 10000)
  public void testTransactionUpdate() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
    // discard this
    responseQueue.take();
    clientManager.updateTransaction(
        transactionId,
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user2", userGateways, 2L, "1")));
    Pair<String, Response> response = responseQueue.take();
    assertEquals("update", response.getFirst());
    assertEquals(Response.Flags.UPDATED, response.getSecond().getFlags());
    assertFalse(response.getSecond().hasStamp());
    State currentState = direct.getServerTransactionManager().getCurrentState(transactionId);
    assertNotNull(currentState);
    assertEquals(2, currentState.getInputAttributes().size());
    assertEquals("user", Iterables.get(currentState.getInputAttributes(), 0).getKey());
    assertEquals("user2", Iterables.get(currentState.getInputAttributes(), 1).getKey());
  }

  @Test(timeout = 10000)
  public void testTransactionRollback() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
    // discard this
    responseQueue.take();
    clientManager.updateTransaction(
        transactionId,
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user2", userGateways, 2L, "1")));
    Pair<String, Response> response = responseQueue.take();
    assertEquals("update", response.getFirst());
    assertEquals(Response.Flags.UPDATED, response.getSecond().getFlags());
    clientManager.rollback(transactionId);
    response = responseQueue.take();
    assertEquals("rollback", response.getFirst());
    assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());
  }

  @Test(timeout = 10000)
  public void testHousekeepingOfWildcardAttributes() throws InterruptedException {
    now = System.currentTimeMillis();
    AtomicLong stamp = new AtomicLong(now);
    createObserver(new WithTransactionTimeout(100, 50, stamp));
    TransactionResourceManager manager = (TransactionResourceManager) observer.getRawManager();
    manager.houseKeeping();
    String transactionId = "t1";
    BlockingQueue<Pair<String, Response>> queue = new ArrayBlockingQueue<>(10);
    manager.begin(
        transactionId,
        (s, r) -> ExceptionUtils.unchecked(() -> queue.put(Pair.of(s, r))),
        KeyAttributes.ofWildcardQueryElements(
            this.user, "user", userGateways, Collections.emptyList()));
    Pair<String, Response> response = queue.take();
    assertEquals(Response.Flags.OPEN, response.getSecond().getFlags());
    assertTrue("Expected empty queue, got " + queue, queue.isEmpty());
    StreamElement wildcardUpsert =
        userGateways.upsert(response.getSecond().getSeqId(), "user", "1", now, new byte[] {});
    manager.commit(
        transactionId, Collections.singletonList(KeyAttributes.ofStreamElement(wildcardUpsert)));
    assertEquals(Response.Flags.COMMITTED, queue.take().getSecond().getFlags());
    assertTrue(queue.isEmpty());

    // wait till housekeeping time expires
    stamp.addAndGet(200L);

    transactionId = "t2";
    manager.begin(
        transactionId,
        (s, r) -> ExceptionUtils.unchecked(() -> queue.put(Pair.of(s, r))),
        KeyAttributes.ofWildcardQueryElements(
            user, "user", userGateways, Collections.singletonList(wildcardUpsert)));
    assertEquals(Response.Flags.OPEN, queue.take().getSecond().getFlags());
    assertTrue(queue.isEmpty());
    wildcardUpsert = userGateways.upsert(1001L, "user", "1", now, new byte[] {});
    manager.commit(
        transactionId, Collections.singletonList(KeyAttributes.ofStreamElement(wildcardUpsert)));
    assertEquals(Response.Flags.COMMITTED, queue.take().getSecond().getFlags());
    assertTrue(queue.isEmpty());

    TimeUnit.MILLISECONDS.sleep(150);

    transactionId = "t3";
    manager.begin(
        transactionId,
        (s, r) -> ExceptionUtils.unchecked(() -> queue.put(Pair.of(s, r))),
        KeyAttributes.ofWildcardQueryElements(
            this.user, "user", userGateways, Collections.emptyList()));
    assertEquals(Response.Flags.ABORTED, queue.take().getSecond().getFlags());
    assertTrue(queue.isEmpty());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitAfterFailover() throws InterruptedException {
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    try (ClientTransactionManager clientManager =
        new TransactionResourceManager(direct, Collections.emptyMap())) {
      createObserver();
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      responseQueue.take();
      observer.getRawManager().close();
      createObserver();
      clientManager.commit(
          transactionId,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> response = responseQueue.take();
      assertEquals("commit", response.getFirst());
      assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionRollbackAfterFailover() throws InterruptedException {
    String t1 = "t1-" + UUID.randomUUID();
    String t2 = "t2-" + UUID.randomUUID();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    try (ClientTransactionManager clientManager =
        new TransactionResourceManager(direct, Collections.emptyMap())) {
      createObserver();
      clientManager.begin(
          t1,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      responseQueue.take();
      clientManager.begin(
          t2,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      responseQueue.take();
      clientManager.commit(
          t2,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      responseQueue.take();
      observer.getRawManager().close();
      createObserver();
      clientManager.commit(
          t1,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> response = responseQueue.take();
      assertEquals("commit", response.getFirst());
      assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());
      tearDown();
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitAfterFailoverDuplicateTransaction()
      throws InterruptedException {

    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    try (ClientTransactionManager clientManager =
        new TransactionResourceManager(direct, Collections.emptyMap())) {
      createObserver();
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      Pair<String, Response> firstResponse = responseQueue.take();
      observer.getRawManager().close();
      createObserver();
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      Pair<String, Response> secondResponse = responseQueue.take();
      assertEquals(firstResponse.getSecond().getSeqId(), secondResponse.getSecond().getSeqId());
      assertEquals(Response.Flags.DUPLICATE, secondResponse.getSecond().getFlags());
      clientManager.commit(
          transactionId,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> commitResponse = responseQueue.take();
      assertEquals("commit", commitResponse.getFirst());
      assertEquals(Response.Flags.COMMITTED, commitResponse.getSecond().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitRollback() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
    responseQueue.take();
    clientManager.commit(
        transactionId,
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
    responseQueue.take();
    clientManager.rollback(transactionId);
    Pair<String, Response> response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());

    // now, when we start new transaction, it must be let through
    transactionId = UUID.randomUUID().toString();
    responseQueue.clear();
    clientManager.begin(
        transactionId,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        KeyAttributes.ofWildcardQueryElements(user, "user", userGateways, Collections.emptyList()));
    responseQueue.take();
    clientManager.commit(
        transactionId,
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
    response = responseQueue.take();
    assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
  }

  static class WithTransactionTimeout implements TransactionLogObserverFactory {

    private final long timeout;
    private final AtomicLong stamp;

    WithTransactionTimeout(long timeout, long sleepMs, AtomicLong stamp) {
      this.timeout = timeout;
      this.stamp = stamp;
    }

    @Override
    public TransactionLogObserver create(DirectDataOperator direct) {
      return new TransactionLogObserver(direct) {
        @Override
        ServerTransactionManager getServerTransactionManager(DirectDataOperator direct) {
          TransactionResourceManager manager =
              (TransactionResourceManager) super.getServerTransactionManager(direct);
          manager.setTransactionTimeoutMs(timeout);
          return manager;
        }

        @Override
        long currentTimeMillis() {
          return stamp.get();
        }

        @Override
        void sleep(long sleepMs) throws InterruptedException {
          super.sleep(timeout);
        }
      };
    }
  }

  static class Rethrowing implements TransactionLogObserverFactory {
    @Override
    public TransactionLogObserver create(DirectDataOperator direct) {
      return new TransactionLogObserver(direct) {
        @Override
        void exit(int status) {
          throw new RuntimeException("System.exit(" + status + ")");
        }
      };
    }
  }
}
