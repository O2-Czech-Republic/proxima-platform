/**
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import cz.o2.proxima.repository.config.ConfigUtils;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.util.TransformationRunner;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link TransactionLogObserver}. */
public class TransactionLogObserverTest {

  private final Set<String> transactionFamilies =
      Sets.newHashSet(
          "all-transaction-commit-log-request",
          "all-transaction-commit-log-response",
          "all-transaction-commit-log-state");

  private final Config conf =
      ConfigUtils.withStorageReplacement(
          ConfigFactory.defaultApplication()
              .withFallback(ConfigFactory.load("test-transactions.conf"))
              .resolve(),
          transactionFamilies::contains,
          name -> URI.create("kafka-test://broker/topic_" + name));
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
    createObserver(
        direct ->
            new TransactionLogObserver(direct) {
              @Override
              protected void assertSingleton() {}
            });
  }

  private void createObserver(TransactionLogObserverFactory factory) {
    now = System.currentTimeMillis();
    observer = factory.create(direct);
    observer.run(getClass().getSimpleName());
  }

  @Before
  public void setUp() {
    TransformationRunner.runTransformations(repo, direct);
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
    assertTrue(response.getFirst().startsWith("open."));
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
  public void testCreateTransactionCommitWithConflictInInputs() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String t1 = UUID.randomUUID().toString();
    String t2 = UUID.randomUUID().toString();
    List<KeyAttribute> inputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, 1L, "1"));
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        t1, ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))), inputs);
    responseQueue.take();
    clientManager.commit(
        t1,
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, 2L, "1")));
    Pair<String, Response> response = responseQueue.take();
    assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
    clientManager.begin(
        t2, ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))), inputs);
    response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitWithConflictInOutputs() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String t1 = UUID.randomUUID().toString();
    String t2 = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        t1,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "u1", userGateways, 1L, "1")));
    Pair<String, Response> t1OpenResponse = responseQueue.take();
    List<KeyAttribute> t1Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t1OpenResponse.getSecond().getSeqId(), "1"));
    clientManager.begin(
        t2,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        // not conflicting with the previous one
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "u2", userGateways, 1L, "1")));
    Pair<String, Response> t2OpenResponse = responseQueue.take();
    List<KeyAttribute> t2Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t2OpenResponse.getSecond().getSeqId(), "1"));
    clientManager.commit(t2, t2Outputs);
    Pair<String, Response> response = responseQueue.take();
    assertEquals("commit", response.getFirst());
    assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
    clientManager.commit(t1, t1Outputs);
    response = responseQueue.take();
    assertEquals("commit", response.getFirst());
    assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitWithSameTimestampInOutputs() throws InterruptedException {
    long stamp = System.currentTimeMillis();
    WithFixedTime factory = new WithFixedTime(stamp);
    createObserver(factory);
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String t1 = UUID.randomUUID().toString();
    String t2 = UUID.randomUUID().toString();
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager.begin(
        t1,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "u1", userGateways, 1L, "1")));
    Pair<String, Response> t1OpenResponse = responseQueue.take();
    assertEquals(stamp, t1OpenResponse.getSecond().getStamp());
    List<KeyAttribute> t1Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t1OpenResponse.getSecond().getSeqId(), "1"));
    clientManager.begin(
        t2,
        ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
        // not conflicting with the previous one
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(user, "u2", userGateways, 1L, "1")));
    Pair<String, Response> t2OpenResponse = responseQueue.take();
    assertEquals(stamp, t2OpenResponse.getSecond().getStamp());
    List<KeyAttribute> t2Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t2OpenResponse.getSecond().getSeqId(), "1"));
    clientManager.commit(t1, t1Outputs);
    Pair<String, Response> response = responseQueue.take();
    assertEquals("commit", response.getFirst());
    assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
    clientManager.commit(t2, t2Outputs);
    response = responseQueue.take();
    assertEquals("commit", response.getFirst());
    assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());
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
    assertTrue(response.getFirst().startsWith("open."));
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
    assertTrue(response.getFirst().startsWith("update."));
    assertEquals(Response.Flags.UPDATED, response.getSecond().getFlags());
    assertFalse(response.getSecond().hasStamp());
    do {
      State currentState = direct.getServerTransactionManager().getCurrentState(transactionId);
      assertNotNull(currentState);
      if (currentState.getInputAttributes().size() == 2) {
        assertEquals("user", Iterables.get(currentState.getInputAttributes(), 0).getKey());
        assertEquals("user2", Iterables.get(currentState.getInputAttributes(), 1).getKey());
        break;
      }
    } while (true);
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
    assertTrue(response.getFirst().startsWith("update."));
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
    createObserver(new WithTransactionTimeout(100, stamp));
    ServerTransactionManager serverManager = observer.getRawManager();
    try (ClientTransactionManager clientManager =
        new TransactionResourceManager(direct, Collections.emptyMap())) {
      serverManager.houseKeeping();
      String transactionId = "t1";
      BlockingQueue<Pair<String, Response>> queue = new ArrayBlockingQueue<>(10);
      clientManager.begin(
          transactionId,
          (s, r) -> ExceptionUtils.unchecked(() -> queue.put(Pair.of(s, r))),
          KeyAttributes.ofWildcardQueryElements(
              this.user, "user", userGateways, Collections.emptyList()));
      Pair<String, Response> response = queue.take();
      assertEquals(Response.Flags.OPEN, response.getSecond().getFlags());
      assertTrue("Expected empty queue, got " + queue, queue.isEmpty());
      StreamElement wildcardUpsert =
          userGateways.upsert(response.getSecond().getSeqId(), "user", "1", now, new byte[] {});
      clientManager.commit(
          transactionId, Collections.singletonList(KeyAttributes.ofStreamElement(wildcardUpsert)));
      assertEquals(Response.Flags.COMMITTED, queue.take().getSecond().getFlags());
      assertTrue(queue.isEmpty());

      // wait till housekeeping time expires
      stamp.addAndGet(200L);

      transactionId = "t2";
      clientManager.begin(
          transactionId,
          (s, r) -> ExceptionUtils.unchecked(() -> queue.put(Pair.of(s, r))),
          KeyAttributes.ofWildcardQueryElements(
              user, "user", userGateways, Collections.singletonList(wildcardUpsert)));
      response = queue.take();
      assertEquals(Response.Flags.OPEN, response.getSecond().getFlags());
      assertTrue(queue.isEmpty());
      wildcardUpsert =
          userGateways.upsert(response.getSecond().getSeqId(), "user", "1", now, new byte[] {});
      clientManager.commit(
          transactionId, Collections.singletonList(KeyAttributes.ofStreamElement(wildcardUpsert)));
      assertEquals(Response.Flags.COMMITTED, queue.take().getSecond().getFlags());
      assertTrue(queue.isEmpty());

      TimeUnit.MILLISECONDS.sleep(150);

      transactionId = "t3";
      clientManager.begin(
          transactionId,
          (s, r) -> ExceptionUtils.unchecked(() -> queue.put(Pair.of(s, r))),
          KeyAttributes.ofWildcardQueryElements(
              this.user, "user", userGateways, Collections.emptyList()));
      assertEquals(Response.Flags.ABORTED, queue.take().getSecond().getFlags());
      assertTrue(queue.isEmpty());
    }
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
      takeResponseFor(responseQueue, "open.1");
      observer.getRawManager().close();
      createObserver();
      clientManager.commit(
          transactionId,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> response = takeResponseFor(responseQueue, "commit");
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
      Pair<String, Response> t1openResponse = takeResponseFor(responseQueue, "open.1");
      clientManager.begin(
          t2,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      Pair<String, Response> t2openResponse = takeResponseFor(responseQueue, "open.1");
      assertTrue(t2openResponse.getSecond().getSeqId() > t1openResponse.getSecond().getSeqId());
      clientManager.commit(
          t2,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> t2commitResponse = takeResponseFor(responseQueue, "commit");
      assertEquals("commit", t2commitResponse.getFirst());
      observer.getRawManager().close();
      createObserver();
      clientManager.commit(
          t1,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> response = takeResponseFor(responseQueue, "commit");
      assertTrue(
          "Expected exactly one committed transaction, got "
              + t2commitResponse.getSecond()
              + " and "
              + response.getSecond(),
          t2commitResponse.getSecond().getFlags() == Response.Flags.COMMITTED
              ^ response.getSecond().getFlags() == Response.Flags.COMMITTED);
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
      Pair<String, Response> firstResponse = takeResponseFor(responseQueue, "open.1");
      observer.getRawManager().close();
      createObserver();
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      Pair<String, Response> secondResponse = takeResponseFor(responseQueue, "open.2");
      assertEquals(firstResponse.getSecond().getSeqId(), secondResponse.getSecond().getSeqId());
      assertEquals(Response.Flags.DUPLICATE, secondResponse.getSecond().getFlags());
      clientManager.commit(
          transactionId,
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> commitResponse = takeResponseFor(responseQueue, "commit");
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

  @Test(timeout = 10000)
  public void testCreateTransactionRollbackRollback() throws InterruptedException {
    createObserver();
    try (ClientTransactionManager clientManager = direct.getClientTransactionManager()) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      responseQueue.take();
      clientManager.rollback(transactionId);
      Pair<String, Response> rollbackResponse1 = responseQueue.take();
      assertEquals(Response.Flags.ABORTED, rollbackResponse1.getSecond().getFlags());
      clientManager.rollback(transactionId);
      Pair<String, Response> rollbackResponse2 = responseQueue.take();
      assertEquals(Response.Flags.ABORTED, rollbackResponse2.getSecond().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testPostCommitOutOfOrder() throws InterruptedException {
    createObserver();
    State committed =
        State.open(2001L, now, Collections.emptyList())
            .committed(
                Collections.singletonList(
                    KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2001L, "1")));
    observer.transactionPostCommit(committed);
    State committed2 =
        State.open(1001L, now - 1, Collections.emptyList())
            .committed(
                Collections.singletonList(
                    KeyAttributes.ofStreamElement(
                        userGateways.delete(1001L, "user", "1", now - 1))));
    observer.transactionPostCommit(committed2);
    BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
    try (ClientTransactionManager clientManager = direct.getClientTransactionManager()) {
      String t = "t";
      clientManager.begin(
          t,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          KeyAttributes.ofWildcardQueryElements(
              user, "user", userGateways, Collections.emptyList()));
      Pair<String, Response> response = responseQueue.take();
      assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testTwoOpenedCommitWildcard() throws InterruptedException {
    createObserver();
    try (ClientTransactionManager clientManager = direct.getClientTransactionManager()) {
      String t1 = UUID.randomUUID().toString();
      String t2 = UUID.randomUUID().toString();
      List<KeyAttribute> inputs =
          KeyAttributes.ofWildcardQueryElements(
              user,
              "user",
              userGateways,
              Collections.singletonList(
                  userGateways.upsert(100L, "user", "1", now, new byte[] {})));

      BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
      clientManager.begin(
          t1,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          inputs);
      Pair<String, Response> t1OpenResponse = responseQueue.take();
      clientManager.begin(
          t2,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          inputs);
      Pair<String, Response> t2OpenResponse = responseQueue.take();

      assertEquals(Response.Flags.OPEN, t1OpenResponse.getSecond().getFlags());
      assertEquals(Response.Flags.OPEN, t2OpenResponse.getSecond().getFlags());

      List<KeyAttribute> t1Outputs =
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(
                  user, "user", userGateways, t1OpenResponse.getSecond().getSeqId(), "1"));

      List<KeyAttribute> t2Outputs =
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(
                  user, "user", userGateways, t2OpenResponse.getSecond().getSeqId(), "1"));

      clientManager.commit(t1, t1Outputs);
      Pair<String, Response> response = responseQueue.take();
      assertEquals("commit", response.getFirst());
      assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
      clientManager.commit(t2, t2Outputs);
      response = responseQueue.take();
      assertEquals("commit", response.getFirst());
      assertEquals(Response.Flags.ABORTED, response.getSecond().getFlags());
    }
  }

  @Test
  public void testKeyAttributeConcat() {
    long now = System.currentTimeMillis();
    StreamElement first = userGateways.upsert(1000L, "u", "1", now, new byte[] {});
    StreamElement second = userGateways.upsert(1000L, "u", "2", now, new byte[] {});
    List<KeyAttribute> inputs =
        KeyAttributes.ofWildcardQueryElements(
            user, "u", userGateways, Collections.singletonList(first));
    List<KeyAttribute> outputs = Collections.singletonList(KeyAttributes.ofStreamElement(second));
    List<KeyAttribute> result =
        Lists.newArrayList(TransactionLogObserver.concatInputsAndOutputs(inputs, outputs));
    assertTrue(result.containsAll(inputs));
    assertTrue(outputs.stream().noneMatch(result::contains));
  }

  static class WithTransactionTimeout implements TransactionLogObserverFactory {

    private final long timeout;
    private final AtomicLong stamp;

    WithTransactionTimeout(long timeout, AtomicLong stamp) {
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

  private Pair<String, Response> takeResponseFor(
      BlockingQueue<Pair<String, Response>> responseQueue, String responseId)
      throws InterruptedException {

    for (; ; ) {
      Pair<String, Response> res = responseQueue.take();
      if (res.getFirst().equals(responseId)) {
        return res;
      }
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

        @Override
        protected void assertSingleton() {}
      };
    }
  }

  static class WithFixedTime implements TransactionLogObserverFactory {
    final long time;

    WithFixedTime(long stamp) {
      this.time = stamp;
    }

    @Override
    public TransactionLogObserver create(DirectDataOperator direct) {
      return new TransactionLogObserver(direct) {
        @Override
        long currentTimeMillis() {
          return time;
        }

        @Override
        protected void assertSingleton() {}
      };
    }
  }
}
