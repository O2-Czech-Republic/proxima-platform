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

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.Transaction;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.TransformationRunner;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionalOnlineAttributeWriterTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final EntityDescriptor user = repo.getEntity("user");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");
  private final AttributeDescriptor<byte[]> userGateways = user.getAttribute("gateway.*");
  private final Deque<Response> toReturn = new ArrayDeque<>(100);

  private ServerTransactionManager manager;

  @Before
  public void setUp() {
    toReturn.clear();
    manager = direct.getServerTransactionManager();
    manager.runObservations(
        "test",
        (ingest, context) -> {
          String transactionId = ingest.getKey();
          if (ingest.getAttributeDescriptor().equals(manager.getRequestDesc())) {
            String responseId = manager.getRequestDesc().extractSuffix(ingest.getAttribute());
            Request request = Optionals.get(manager.getRequestDesc().valueOf(ingest));
            Response response = Optional.ofNullable(toReturn.poll()).orElse(Response.empty());
            CommitCallback toCommit = context::commit;
            if (response.getFlags() == Response.Flags.OPEN) {
              toCommit = CommitCallback.afterNumCommits(2, context::commit);
              manager.setCurrentState(
                  transactionId,
                  State.open(
                      response.getSeqId(), response.getStamp(), request.getInputAttributes()),
                  toCommit);
            }
            manager.writeResponse(transactionId, responseId, response, toCommit);
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

  @Test(timeout = 10_000)
  public void testSimpleWriteToTransaction() throws InterruptedException {
    CachedView view = Optionals.get(direct.getCachedView(status));
    view.assign(view.getPartitions());
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    assertTrue(writer.isTransactional());
    // we successfully open and commit the transaction
    long stamp = 1234567890000L;
    toReturn.add(Response.forRequest(anyRequest()).open(1L, stamp));
    toReturn.add(Response.forRequest(anyRequest()).committed());
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3}),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    Optional<KeyValue<byte[]>> res = view.get("key", status);
    assertTrue(res.isPresent());
    assertEquals("key", res.get().getKey());
    assertTrue(res.get().hasSequentialId());
    assertEquals(1L, res.get().getSequentialId());
    assertEquals(stamp, res.get().getStamp());
  }

  @Test(timeout = 10_000)
  public void testTransactionCreateUpdateCommit()
      throws InterruptedException, TransactionRejectedException {

    CachedView view = Optionals.get(direct.getCachedView(status));
    view.assign(view.getPartitions());
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    assertTrue(writer.isTransactional());
    long stamp = 123456789000L;
    // we successfully open and commit the transaction
    toReturn.add(Response.forRequest(anyRequest()).open(1L, stamp));
    toReturn.add(Response.forRequest(anyRequest()).updated());
    toReturn.add(Response.forRequest(anyRequest()).committed());
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "key", status, 1L);
    try (TransactionalOnlineAttributeWriter.Transaction t = writer.transactional().begin()) {
      t.update(Collections.singletonList(ka));
      t.update(Collections.singletonList(ka));
      t.commitWrite(
          Collections.singletonList(
              StreamElement.upsert(
                  gateway,
                  status,
                  UUID.randomUUID().toString(),
                  "key",
                  status.getName(),
                  System.currentTimeMillis(),
                  new byte[] {1, 2, 3})),
          (succ, exc) -> {
            assertTrue(succ);
            assertNull(exc);
          });
    }
    Optional<KeyValue<byte[]>> res = view.get("key", status);
    assertTrue(res.isPresent());
    assertEquals("key", res.get().getKey());
    assertTrue(res.get().hasSequentialId());
    assertEquals(1L, res.get().getSequentialId());
    assertEquals(stamp, res.get().getStamp());
  }

  @Test(timeout = 10_000)
  public void testTransactionCreateUpdateCommitMultipleOutputs()
      throws InterruptedException, TransactionRejectedException {

    try (ObserveHandle handle =
        TransformationRunner.runTransformation(
            direct,
            "_transaction-commit",
            repo.getTransformations().get("_transaction-commit"),
            ign -> {})) {
      CachedView view = Optionals.get(direct.getCachedView(status));
      view.assign(view.getPartitions());
      OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
      assertTrue(writer.isTransactional());
      long stamp = 123456789000L;
      // we successfully open and commit the transaction
      toReturn.add(Response.forRequest(anyRequest()).open(1L, stamp));
      toReturn.add(Response.forRequest(anyRequest()).updated());
      toReturn.add(Response.forRequest(anyRequest()).committed());
      KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "key", status, 1L);
      try (TransactionalOnlineAttributeWriter.Transaction t = writer.transactional().begin()) {
        t.update(Collections.singletonList(ka));
        t.update(Collections.singletonList(ka));
        t.commitWrite(
            Arrays.asList(
                StreamElement.upsert(
                    gateway,
                    status,
                    UUID.randomUUID().toString(),
                    "key",
                    status.getName(),
                    System.currentTimeMillis(),
                    new byte[] {1, 2, 3}),
                StreamElement.upsert(
                    gateway,
                    status,
                    UUID.randomUUID().toString(),
                    "key2",
                    status.getName(),
                    System.currentTimeMillis() + 1,
                    new byte[] {1, 2, 3})),
            (succ, exc) -> {
              assertTrue(succ);
              assertNull(exc);
            });
      }
      while (!view.get("key", status).isPresent()) {
        // need to wait for the transformation
        TimeUnit.MILLISECONDS.sleep(100);
      }
      Optional<KeyValue<byte[]>> res = view.get("key", status);
      assertTrue(res.isPresent());
      assertEquals("key", res.get().getKey());
      assertTrue(res.get().hasSequentialId());
      assertEquals(1L, res.get().getSequentialId());
      assertEquals(stamp, res.get().getStamp());

      while (!view.get("key2", status).isPresent()) {
        // need to wait for the transformation
        TimeUnit.MILLISECONDS.sleep(100);
      }
      res = view.get("key2", status);
      assertTrue(res.isPresent());
      assertEquals("key2", res.get().getKey());
      assertTrue(res.get().hasSequentialId());
      assertEquals(1L, res.get().getSequentialId());
      assertEquals(stamp, res.get().getStamp());
    }
  }

  @Test(timeout = 10_000, expected = TransactionRejectedException.class)
  public void testTransactionCreateUpdateCommitRejected()
      throws InterruptedException, TransactionRejectedException {

    CachedView view = Optionals.get(direct.getCachedView(status));
    view.assign(view.getPartitions());
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    assertTrue(writer.isTransactional());
    long stamp = 123456789000L;
    // we successfully open and commit the transaction
    toReturn.add(Response.forRequest(anyRequest()).open(1L, stamp));
    toReturn.add(Response.forRequest(anyRequest()).updated());
    toReturn.add(Response.forRequest(anyRequest()).aborted());
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "key", status, 1L);
    try (TransactionalOnlineAttributeWriter.Transaction t = writer.transactional().begin()) {
      t.update(Collections.singletonList(ka));
      t.update(Collections.singletonList(ka));
      t.commitWrite(
          Collections.singletonList(
              StreamElement.upsert(
                  gateway,
                  status,
                  UUID.randomUUID().toString(),
                  "key",
                  status.getName(),
                  System.currentTimeMillis(),
                  new byte[] {1, 2, 3})),
          (succ, exc) -> {
            assertTrue(succ);
            assertNull(exc);
          });
    }
  }

  @Test(timeout = 10_000)
  public void testTransactionCommitReject() throws InterruptedException {
    CachedView view = Optionals.get(direct.getCachedView(status));
    view.assign(view.getPartitions());
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    assertTrue(writer.isTransactional());
    // we successfully open and commit the transaction
    toReturn.add(Response.forRequest(anyRequest()).open(1L, 123456789000L));
    toReturn.add(Response.forRequest(anyRequest()).aborted());
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3}),
        (succ, exc) -> {
          assertFalse(succ);
          assertNotNull(exc);
          assertTrue(exc instanceof TransactionRejectedException);
          latch.countDown();
        });
    latch.await();
  }

  @Test(timeout = 10000)
  public void testGlobalTransactionWriter()
      throws InterruptedException, TransactionRejectedException {

    TransactionalOnlineAttributeWriter writer = direct.getGlobalTransactionWriter();
    assertTrue(user.isTransactional());
    // we successfully open and commit the transaction
    long stamp = 1234567890000L;
    toReturn.add(Response.forRequest(anyRequest()).open(1L, stamp));
    toReturn.add(Response.forRequest(anyRequest()).committed());
    try (Transaction t = writer.begin()) {
      CachedView view = Optionals.get(direct.getCachedView(userGateways));
      view.assign(view.getPartitions());
      CountDownLatch latch = new CountDownLatch(1);
      t.commitWrite(
          Collections.singletonList(
              StreamElement.upsert(
                  user,
                  userGateways,
                  UUID.randomUUID().toString(),
                  "key",
                  userGateways.toAttributePrefix() + "gw1",
                  System.currentTimeMillis(),
                  new byte[] {1, 2, 3})),
          (succ, exc) -> {
            assertTrue(succ);
            assertNull(exc);
            latch.countDown();
          });
      latch.await();
      Optional<KeyValue<byte[]>> res;
      while (true) {
        res = view.get("key", userGateways.toAttributePrefix() + "gw1", userGateways);
        if (res.isPresent()) {
          break;
        }
        TimeUnit.MILLISECONDS.sleep(100);
      }
      assertEquals("key", res.get().getKey());
      assertTrue(res.get().hasSequentialId());
      assertEquals(1L, res.get().getSequentialId());
      assertEquals(stamp, res.get().getStamp());
    }
  }

  private Request anyRequest() {
    return Request.builder().build();
  }
}
