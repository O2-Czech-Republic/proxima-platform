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
import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.AttributeWriterBase.Type;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.Transaction;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.direct.view.CachedView.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.util.Optionals;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionalCachedViewTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");
  private ServerTransactionManager server;

  @Before
  public void setUp() {
    server = direct.getServerTransactionManager();
    AtomicLong seqId = new AtomicLong(1000L);
    server.runObservations(
        "dummy",
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OnNextContext context) {
            if (ingest.getAttributeDescriptor().equals(server.getRequestDesc())) {
              Optional<Request> request = server.getRequestDesc().valueOf(ingest);
              if (request.isPresent()) {
                switch (request.get().getFlags()) {
                  case OPEN:
                    server.writeResponse(
                        ingest.getKey(),
                        "open",
                        Response.open(seqId.getAndIncrement(), System.currentTimeMillis()),
                        context::commit);
                    break;
                  case COMMIT:
                    server.writeResponse(
                        ingest.getKey(), "commit", Response.committed(), context::commit);
                    break;
                  case UPDATE:
                    server.writeResponse(
                        ingest.getKey(), "update", Response.updated(), context::commit);
                    break;
                  case ROLLBACK:
                    server.writeResponse(
                        ingest.getKey(), "rollback", Response.aborted(), context::commit);
                    break;
                }
              }
            }
            context.confirm();
            return true;
          }
        });
  }

  @After
  public void tearDown() {
    direct.close();
  }

  @Test(timeout = 10000)
  public void testViewReadWrite() throws InterruptedException {
    try (CachedView view = Optionals.get(direct.getCachedView(status, device))) {
      view.assign(view.getPartitions());
      CountDownLatch latch = new CountDownLatch(1);
      view.write(
          StreamElement.upsert(
              gateway,
              status,
              UUID.randomUUID().toString(),
              "gw",
              status.getName(),
              System.currentTimeMillis(),
              new byte[] {1, 2, 3}),
          (succ, exc) -> latch.countDown());
      latch.await();
      Optional<KeyValue<byte[]>> result = view.get("gw", status);
      assertTrue(result.isPresent());
      assertTrue(result.get().hasSequentialId());
      assertEquals(1000L, result.get().getSequentialId());
    }
  }

  @Test(timeout = 10000)
  public void testViewReadWriteExplicitTransaction()
      throws InterruptedException, TransactionRejectedException {

    try (CachedView view = Optionals.get(direct.getCachedView(status, device))) {
      view.assign(view.getPartitions());
      CountDownLatch latch = new CountDownLatch(1);
      for (int i = 0; i < 10; i++) {
        try (Transaction t = view.transactional().begin()) {
          Optional<KeyValue<byte[]>> kv = view.get("gw", status);
          List<KeyAttribute> inputs = new ArrayList<>();
          if (kv.isPresent()) {
            inputs.add(KeyAttributes.ofStreamElement(kv.get()));
          } else {
            inputs.add(KeyAttributes.ofMissingAttribute(gateway, "gw", status));
          }
          t.update(inputs);
          t.commitWrite(
              Collections.singletonList(
                  StreamElement.upsert(
                      gateway,
                      status,
                      UUID.randomUUID().toString(),
                      "gw",
                      status.getName(),
                      System.currentTimeMillis(),
                      new byte[] {1, 2, 3})),
              (succ, exc) -> latch.countDown());
          latch.await();
          Optional<KeyValue<byte[]>> result = view.get("gw", status, Long.MAX_VALUE);
          assertTrue(result.isPresent());
          assertTrue(result.get().hasSequentialId());
          assertEquals(1000L + i, result.get().getSequentialId());
        }
      }
    }
  }

  @Test
  public void testInvariants() {
    try (CachedView view = Optionals.get(direct.getCachedView(status, device))) {
      Factory factory = view.asFactory();
      try (CachedView newView = factory.apply(repo)) {
        assertTrue(newView instanceof TransactionalCachedView);
      }
      assertEquals(Type.ONLINE, view.getType());
      assertSame(view, view.online());
    }
  }
}
