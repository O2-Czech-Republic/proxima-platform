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
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
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
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionalOnlineAttributeWriterTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");
  private final BlockingQueue<Response> toReturn = new ArrayBlockingQueue<>(100);

  private TransactionResourceManager manager;

  @Before
  public void setUp() {
    toReturn.clear();
    manager = TransactionResourceManager.of(direct);
    manager.runObservations(
        "test",
        (ingest, context) -> {
          String transactionId = ingest.getKey();
          if (ingest.getAttributeDescriptor().equals(manager.getRequestDesc())) {
            String responseId = manager.getResponseDesc().extractSuffix(ingest.getAttribute());
            Response response = ExceptionUtils.uncheckedFactory(toReturn::take);
            Request request = manager.getRequestDesc().valueOf(ingest).get();
            CommitCallback toCommit = context::commit;
            if (response.getFlags() == Response.Flags.OPEN) {
              toCommit = CommitCallback.afterNumCommits(2, context::commit);
              manager.setCurrentState(
                  transactionId,
                  State.open(response.getSeqId(), request.getInputAttributes()),
                  toCommit);
            }
            manager.writeResponse(transactionId, responseId, response, toCommit);
          } else {
            context.confirm();
          }
          return true;
        });
  }

  @After
  public void tearDown() {
    manager.close();
  }

  @Test(timeout = 10_000)
  public void testSimpleWriteToTransaction() throws InterruptedException {
    CachedView view = Optionals.get(direct.getCachedView(status));
    view.assign(view.getPartitions());
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    assertTrue(writer.isTransactional());
    // we successfully open and commit the transaction
    toReturn.put(Response.open(1L));
    toReturn.put(Response.committed());
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
        });
    Optional<KeyValue<byte[]>> res = view.get("key", status);
    assertTrue(res.isPresent());
    assertEquals("key", res.get().getKey());
    assertTrue(res.get().hasSequentialId());
    assertEquals(1L, res.get().getSequentialId());
  }

  @Test(timeout = 10_000)
  public void testTransactionCreateUpdateCommit()
      throws InterruptedException, TransactionRejectedException {

    CachedView view = Optionals.get(direct.getCachedView(status));
    view.assign(view.getPartitions());
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    assertTrue(writer.isTransactional());
    // we successfully open and commit the transaction
    toReturn.put(Response.open(1L));
    toReturn.put(Response.updated());
    toReturn.put(Response.committed());
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "key", status, 1L);
    try (TransactionalOnlineAttributeWriter.Transaction t = writer.toTransactional().begin()) {
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
  }

  @Test(timeout = 10_000, expected = TransactionRejectedException.class)
  public void testTransactionCreateUpdateCommitRejected()
      throws InterruptedException, TransactionRejectedException {

    CachedView view = Optionals.get(direct.getCachedView(status));
    view.assign(view.getPartitions());
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    assertTrue(writer.isTransactional());
    // we successfully open and commit the transaction
    toReturn.put(Response.open(1L));
    toReturn.put(Response.updated());
    toReturn.put(Response.aborted());
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "key", status, 1L);
    try (TransactionalOnlineAttributeWriter.Transaction t = writer.toTransactional().begin()) {
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
    toReturn.put(Response.open(1L));
    toReturn.put(Response.aborted());
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
}
