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

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableMap;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.config.ConfigUtils;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.KeyAttributes;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.TransformationRunner;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.transaction.ClientTransactionManager;
import cz.o2.proxima.direct.core.transaction.ServerTransactionManager;
import cz.o2.proxima.direct.core.transaction.TransactionResourceManager;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.Arrays;
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
              .withFallback(
                  ConfigFactory.parseMap(
                      ImmutableMap.of(
                          "transactions.monitoring-policy", TestMonitoringPolicy.class.getName())))
              .withFallback(ConfigFactory.load("test-transactions.conf"))
              .resolve(),
          transactionFamilies::contains,
          name -> URI.create("kafka-test://broker/topic_" + name));
  private final Repository repo = Repository.ofTest(conf);
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor user = repo.getEntity("user");
  private final Wildcard<byte[]> userGateways = Wildcard.of(user, user.getAttribute("gateway.*"));
  private long now;
  private TransactionLogObserver observer;
  private Metrics metrics;

  public void createObserver() {
    createObserver(
        (direct, metrics) ->
            new TransactionLogObserver(direct, metrics) {
              @Override
              protected void assertSingleton() {}
            });
  }

  private void createObserver(TransactionLogObserverFactory factory) {
    now = System.currentTimeMillis();
    metrics = new Metrics();
    observer = factory.create(direct, metrics);
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
      new Rethrowing().create(direct, new Metrics()).onError(new RuntimeException("error"));
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
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.OPEN, response.getFlags());
    TimeUnit.SECONDS.sleep(1);
    assertTrue(metrics.getTransactionsOpen().getValue() > 0.0);
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommit() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    responseQueue.take();
    clientManager
        .commit(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.COMMITTED, response.getFlags());
    TimeUnit.SECONDS.sleep(1);
    assertTrue(metrics.getTransactionsCommitted().getValue() > 0.0);
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitWithMonitoring() throws InterruptedException {
    TestMonitoringPolicy.clear();
    assertTrue(TestMonitoringPolicy.isEmpty());
    testCreateTransactionCommit();
    assertFalse(TestMonitoringPolicy.isEmpty());
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
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager.begin(t1, inputs).thenAccept(responseQueue::add);
    responseQueue.take();
    clientManager
        .commit(
            t1,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, 2L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.COMMITTED, response.getFlags());
    clientManager.begin(t2, inputs).thenAccept(responseQueue::add);
    response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
    TimeUnit.SECONDS.sleep(1);
    assertTrue(metrics.getTransactionsRejected().getValue() > 0.0);
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitWithMultipleSeqIds() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String t1 = UUID.randomUUID().toString();
    List<KeyAttribute> inputs =
        Arrays.asList(
            KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, 1L, "1"),
            KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, 2L, "1"));
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager.begin(t1, inputs).thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitWithConflictInOutputs() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String t1 = UUID.randomUUID().toString();
    String t2 = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            t1,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "u1", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response t1OpenResponse = responseQueue.take();
    List<KeyAttribute> t1Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t1OpenResponse.getSeqId(), "1"));
    clientManager
        .begin(
            t2,
            // not conflicting with the previous one
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "u2", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response t2OpenResponse = responseQueue.take();
    List<KeyAttribute> t2Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t2OpenResponse.getSeqId(), "1"));
    clientManager.commit(t2, t2Outputs).thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.COMMITTED, response.getFlags());
    clientManager.commit(t1, t1Outputs).thenAccept(responseQueue::add);
    response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitWithSameTimestampInOutputs() throws InterruptedException {
    long stamp = System.currentTimeMillis();
    WithFixedTime factory = new WithFixedTime(stamp);
    createObserver(factory);
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String t1 = UUID.randomUUID().toString();
    String t2 = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            t1,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "u1", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response t1OpenResponse = responseQueue.take();
    assertEquals(stamp, t1OpenResponse.getStamp());
    List<KeyAttribute> t1Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t1OpenResponse.getSeqId(), "1"));
    clientManager
        .begin(
            t2,
            // not conflicting with the previous one
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "u2", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response t2OpenResponse = responseQueue.take();
    assertEquals(stamp, t2OpenResponse.getStamp());
    List<KeyAttribute> t2Outputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(
                user, "user", userGateways, t2OpenResponse.getSeqId(), "1"));
    clientManager.commit(t1, t1Outputs).thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.COMMITTED, response.getFlags());
    clientManager.commit(t2, t2Outputs).thenAccept(responseQueue::add);
    response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionDuplicate() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response firstResponse = responseQueue.take();
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionReopenAfterAbort() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String t1 = UUID.randomUUID().toString();
    String t2 = UUID.randomUUID().toString();
    List<KeyAttribute> inputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, 1L, "1"));
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager.begin(t1, inputs).thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    long t1SeqId = response.getSeqId();
    clientManager
        .commit(
            t1,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, 2L, "1")))
        .thenAccept(responseQueue::add);
    responseQueue.take();
    clientManager.begin(t2, inputs).thenAccept(responseQueue::add);
    // t2 is aborted
    responseQueue.take();
    inputs =
        Collections.singletonList(
            KeyAttributes.ofAttributeDescriptor(this.user, "user", userGateways, t1SeqId, "1"));
    clientManager.begin(t2, inputs).thenApply(responseQueue::add);
    response = responseQueue.take();
    assertEquals(Response.Flags.OPEN, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionDuplicateAfterCommit() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response firstResponse = responseQueue.take();
    clientManager.commit(transactionId, Collections.emptyList()).thenAccept(responseQueue::add);
    assertEquals(Response.Flags.COMMITTED, responseQueue.take().getFlags());
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.DUPLICATE, response.getFlags());
    assertEquals(firstResponse.getSeqId(), response.getSeqId());
  }

  @Test(timeout = 10000)
  public void testTransactionUpdate() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    // discard this
    responseQueue.take();
    clientManager
        .updateTransaction(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user2", userGateways, 2L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.UPDATED, response.getFlags());
    assertFalse(response.hasStamp());
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
  public void testTransactionUpdateWithWildcardConflict() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    // discard this
    responseQueue.take();
    clientManager
        .updateTransaction(
            transactionId,
            KeyAttributes.ofWildcardQueryElements(
                user, "user", userGateways, Collections.emptyList()))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testTransactionUpdateWithWildcardConflict2() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            KeyAttributes.ofWildcardQueryElements(
                user, "user", userGateways, Collections.emptyList()))
        .thenAccept(responseQueue::add);
    // discard this
    responseQueue.take();
    clientManager
        .updateTransaction(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testTransactionUpdateWithWildcardConflict3() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    // first read "userGateways.1"
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    // discard this
    responseQueue.take();
    // then read "userGateways.2"
    clientManager
        .updateTransaction(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "2")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.UPDATED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testTransactionUpdateWithWildcardConflict4() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    // first read "userGateways.1"
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    // discard this
    responseQueue.take();
    // then query "userGateways.*"
    clientManager
        .updateTransaction(
            transactionId,
            KeyAttributes.ofWildcardQueryElements(
                user,
                "user",
                userGateways,
                Arrays.asList(
                    // one update matches the previous query
                    userGateways.upsert(1L, "user", "1", now, new byte[] {}),
                    // one is added due to the query
                    userGateways.upsert(2L, "user", "2", now, new byte[] {}))))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.UPDATED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testTransactionUpdateWithWildcardConflict5() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    // first read "userGateways.1"
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    // discard this
    responseQueue.take();
    // then query "userGateways.*", but get different results
    clientManager
        .updateTransaction(
            transactionId,
            KeyAttributes.ofWildcardQueryElements(
                user,
                "user",
                userGateways,
                Arrays.asList(
                    // one update DOES NOT match the previous query
                    userGateways.upsert(3L, "user", "1", now, new byte[] {}),
                    // one is added due to the query
                    userGateways.upsert(2L, "user", "2", now, new byte[] {}))))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testTransactionRollback() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(1);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    // discard this
    responseQueue.take();
    clientManager
        .updateTransaction(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user2", userGateways, 2L, "1")))
        .thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.UPDATED, response.getFlags());
    clientManager.rollback(transactionId).thenAccept(responseQueue::add);
    response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());
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
      BlockingQueue<Response> queue = new ArrayBlockingQueue<>(10);
      clientManager
          .begin(
              transactionId,
              KeyAttributes.ofWildcardQueryElements(
                  this.user, "user", userGateways, Collections.emptyList()))
          .thenAccept(queue::add);
      Response response = queue.take();
      assertEquals(Response.Flags.OPEN, response.getFlags());
      assertTrue("Expected empty queue, got " + queue, queue.isEmpty());
      StreamElement wildcardUpsert =
          userGateways.upsert(response.getSeqId(), "user", "1", now, new byte[] {});
      clientManager
          .commit(
              transactionId,
              Collections.singletonList(KeyAttributes.ofStreamElement(wildcardUpsert)))
          .thenAccept(queue::add);
      assertEquals(Response.Flags.COMMITTED, queue.take().getFlags());
      assertTrue(queue.isEmpty());

      // wait till housekeeping time expires
      stamp.addAndGet(200L);

      transactionId = "t2";
      clientManager
          .begin(
              transactionId,
              KeyAttributes.ofWildcardQueryElements(
                  user, "user", userGateways, Collections.singletonList(wildcardUpsert)))
          .thenAccept(queue::add);
      response = queue.take();
      assertEquals(Response.Flags.OPEN, response.getFlags());
      assertTrue(queue.isEmpty());
      wildcardUpsert = userGateways.upsert(response.getSeqId(), "user", "1", now, new byte[] {});
      clientManager
          .commit(
              transactionId,
              Collections.singletonList(KeyAttributes.ofStreamElement(wildcardUpsert)))
          .thenAccept(queue::add);
      assertEquals(Response.Flags.COMMITTED, queue.take().getFlags());
      assertTrue(queue.isEmpty());

      TimeUnit.MILLISECONDS.sleep(150);

      transactionId = "t3";
      clientManager
          .begin(
              transactionId,
              KeyAttributes.ofWildcardQueryElements(
                  this.user, "user", userGateways, Collections.emptyList()))
          .thenAccept(queue::add);
      assertEquals(Response.Flags.ABORTED, queue.take().getFlags());
      assertTrue(queue.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitAfterFailover() throws InterruptedException {
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    try (ClientTransactionManager clientManager =
        new TransactionResourceManager(direct, Collections.emptyMap())) {
      createObserver();
      clientManager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
          .thenAccept(responseQueue::add);
      responseQueue.take();
      observer.getRawManager().close();
      createObserver();
      clientManager
          .commit(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")))
          .thenAccept(responseQueue::add);
      Response response = responseQueue.take();
      assertEquals(Response.Flags.COMMITTED, response.getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionRollbackAfterFailover() throws InterruptedException {
    String t1 = "t1-" + UUID.randomUUID();
    String t2 = "t2-" + UUID.randomUUID();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    try (ClientTransactionManager clientManager =
        new TransactionResourceManager(direct, Collections.emptyMap())) {
      createObserver();
      clientManager
          .begin(
              t1,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
          .thenAccept(responseQueue::add);
      Response t1openResponse = responseQueue.take();
      clientManager
          .begin(
              t2,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
          .thenAccept(responseQueue::add);
      Response t2openResponse = responseQueue.take();
      assertTrue(t2openResponse.getSeqId() > t1openResponse.getSeqId());
      clientManager
          .commit(
              t2,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")))
          .thenAccept(responseQueue::add);
      Response t2commitResponse = responseQueue.take();
      observer.getRawManager().close();
      createObserver();
      clientManager
          .commit(
              t1,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")))
          .thenAccept(responseQueue::add);
      Response response = responseQueue.take();
      assertTrue(
          "Expected exactly one committed transaction, got "
              + t2commitResponse
              + " and "
              + response,
          t2commitResponse.getFlags() == Response.Flags.COMMITTED
              ^ response.getFlags() == Response.Flags.COMMITTED);
      tearDown();
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitAfterFailoverDuplicateTransaction()
      throws InterruptedException {

    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    try (ClientTransactionManager clientManager =
        new TransactionResourceManager(direct, Collections.emptyMap())) {
      createObserver();
      clientManager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
          .thenAccept(responseQueue::add);
      Response firstResponse = responseQueue.take();
      clientManager.commit(transactionId, Collections.emptyList()).thenAccept(responseQueue::add);
      assertEquals(Response.Flags.COMMITTED, responseQueue.take().getFlags());
      observer.getRawManager().close();
      createObserver();
      clientManager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
          .thenAccept(responseQueue::add);
      Response secondResponse = responseQueue.take();
      assertEquals(firstResponse.getSeqId(), secondResponse.getSeqId());
      assertEquals(Response.Flags.DUPLICATE, secondResponse.getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommitRollback() throws InterruptedException {
    createObserver();
    ClientTransactionManager clientManager = direct.getClientTransactionManager();
    String transactionId = UUID.randomUUID().toString();
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    clientManager
        .begin(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
        .thenAccept(responseQueue::add);
    responseQueue.take();
    clientManager
        .commit(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")))
        .thenAccept(responseQueue::add);
    responseQueue.take();
    clientManager.rollback(transactionId).thenAccept(responseQueue::add);
    Response response = responseQueue.take();
    assertEquals(Response.Flags.ABORTED, response.getFlags());

    // now, when we start new transaction, it must be let through
    transactionId = UUID.randomUUID().toString();
    responseQueue.clear();
    clientManager
        .begin(
            transactionId,
            KeyAttributes.ofWildcardQueryElements(
                user, "user", userGateways, Collections.emptyList()))
        .thenAccept(responseQueue::add);
    responseQueue.take();
    clientManager
        .commit(
            transactionId,
            Collections.singletonList(
                KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")))
        .thenAccept(responseQueue::add);
    response = responseQueue.take();
    assertEquals(Response.Flags.COMMITTED, response.getFlags());
  }

  @Test(timeout = 10000)
  public void testCreateTransactionRollbackRollback() throws InterruptedException {
    createObserver();
    try (ClientTransactionManager clientManager = direct.getClientTransactionManager()) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(1);
      clientManager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")))
          .thenAccept(responseQueue::add);
      responseQueue.take();
      clientManager.rollback(transactionId).thenAccept(responseQueue::add);
      Response rollbackResponse1 = responseQueue.take();
      assertEquals(Response.Flags.ABORTED, rollbackResponse1.getFlags());
      clientManager.rollback(transactionId).thenAccept(responseQueue::add);
      Response rollbackResponse2 = responseQueue.take();
      assertEquals(Response.Flags.ABORTED, rollbackResponse2.getFlags());
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
    BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
    try (ClientTransactionManager clientManager = direct.getClientTransactionManager()) {
      String t = "t";
      clientManager
          .begin(
              t,
              KeyAttributes.ofWildcardQueryElements(
                  user, "user", userGateways, Collections.emptyList()))
          .thenAccept(responseQueue::add);
      Response response = responseQueue.take();
      assertEquals(Response.Flags.ABORTED, response.getFlags());
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

      BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);
      clientManager.begin(t1, inputs).thenAccept(responseQueue::add);
      Response t1OpenResponse = responseQueue.take();
      clientManager.begin(t2, inputs).thenAccept(responseQueue::add);
      Response t2OpenResponse = responseQueue.take();

      assertEquals(Response.Flags.OPEN, t1OpenResponse.getFlags());
      assertEquals(Response.Flags.OPEN, t2OpenResponse.getFlags());

      List<KeyAttribute> t1Outputs =
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(
                  user, "user", userGateways, t1OpenResponse.getSeqId(), "1"));

      List<KeyAttribute> t2Outputs =
          Collections.singletonList(
              KeyAttributes.ofAttributeDescriptor(
                  user, "user", userGateways, t2OpenResponse.getSeqId(), "1"));

      clientManager.commit(t1, t1Outputs).thenAccept(responseQueue::add);
      Response response = responseQueue.take();
      assertEquals(Response.Flags.COMMITTED, response.getFlags());
      clientManager.commit(t2, t2Outputs).thenAccept(responseQueue::add);
      response = responseQueue.take();
      assertEquals(Response.Flags.ABORTED, response.getFlags());
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
    public TransactionLogObserver create(DirectDataOperator direct, Metrics metrics) {
      return new TransactionLogObserver(direct, metrics) {
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
    public TransactionLogObserver create(DirectDataOperator direct, Metrics metrics) {
      return new TransactionLogObserver(direct, metrics) {
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
    public TransactionLogObserver create(DirectDataOperator direct, Metrics metrics) {
      return new TransactionLogObserver(direct, metrics) {
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
