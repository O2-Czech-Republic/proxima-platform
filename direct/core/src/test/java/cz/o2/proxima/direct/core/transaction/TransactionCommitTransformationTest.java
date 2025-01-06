/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.ConfigConstants;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Commit;
import cz.o2.proxima.core.transaction.Commit.TransactionUpdate;
import cz.o2.proxima.core.transaction.Request;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.LogObserverUtils;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionCommitTransformationTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<byte[]> status = Regular.of(gateway, gateway.getAttribute("status"));
  private final AttributeDescriptor<?> device = gateway.getAttribute("device.*");
  private final EntityDescriptor transaction = repo.getEntity(ConfigConstants.TRANSACTION_ENTITY);
  private final AttributeDescriptor<Commit> commitDesc = transaction.getAttribute("commit");
  private final Regular<State> stateDesc =
      Regular.of(transaction, transaction.getAttribute("state"));
  private final Wildcard<Response> responseDesc =
      Wildcard.of(transaction, transaction.getAttribute("response.*"));
  private TransactionCommitTransformation transformation;

  @Before
  public void setUp() {
    transformation = new TransactionCommitTransformation();
    transformation.setup(repo, direct, Collections.emptyMap());
  }

  @After
  public void tearDown() {
    transformation.close();
  }

  @Test
  public void testTransformUpdates() throws InterruptedException {
    StreamElement upsert = status.upsert(1L, "key", 1L, new byte[] {1, 2, 3});
    StreamElement delete = status.delete(1L, "key", 1L);
    Commit commit = Commit.outputs(Collections.emptyList(), Arrays.asList(upsert, delete));
    List<StreamElement> outputs = new ArrayList<>();
    CommitLogObserver observer = LogObserverUtils.toList(outputs, ign -> {});
    Optionals.get(direct.getCommitLogReader(status)).observe("name", observer).waitUntilReady();
    transformation.transform(
        StreamElement.upsert(
            transaction,
            commitDesc,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            commitDesc.getName(),
            System.currentTimeMillis(),
            commitDesc.getValueSerializer().serialize(commit)),
        (succ, exc) -> assertTrue(succ));
    assertEquals(2, outputs.size());
    assertEquals(Iterables.get(commit.getOutputs(), 0), outputs.get(0));
    assertEquals(Iterables.get(commit.getOutputs(), 1), outputs.get(1));
  }

  @Test
  public void testTransformTransactionUpdate() {
    long now = System.currentTimeMillis();
    Commit commit =
        Commit.updates(
            Arrays.asList(
                new TransactionUpdate(
                    "all-transaction-commit-log-state", stateDesc.upsert("t", now, State.empty())),
                new TransactionUpdate(
                    "all-transaction-commit-log-response",
                    responseDesc.upsert(
                        "t",
                        "1",
                        now,
                        Response.forRequest(Request.builder().responsePartitionId(0).build())))));
    List<StreamElement> requests = new ArrayList<>();
    List<StreamElement> states = new ArrayList<>();
    CommitLogObserver requestObserver = LogObserverUtils.toList(requests, ign -> {});
    CommitLogObserver stateObserver = LogObserverUtils.toList(states, ign -> {});
    Optionals.get(direct.getFamilyByName("all-transaction-commit-log-state").getCommitLogReader())
        .observe("first", stateObserver);
    Optionals.get(
            direct.getFamilyByName("all-transaction-commit-log-response").getCommitLogReader())
        .observe("second", requestObserver);
    transformation.transform(
        StreamElement.upsert(
            transaction,
            commitDesc,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            commitDesc.getName(),
            System.currentTimeMillis(),
            commitDesc.getValueSerializer().serialize(commit)),
        (succ, exc) -> assertTrue(succ));
    assertEquals(1, requests.size());
    assertEquals(1, states.size());
  }

  @Test
  public void testElementCommitted() {
    // invalid input attribute
    AtomicBoolean committed = new AtomicBoolean();
    StreamElement responseElement = responseDesc.upsert("t1", "1", 1L, Response.empty());
    transformation.transform(responseElement, (succ, exc) -> committed.set(succ));
    assertTrue(committed.get());
    committed.set(false);

    // invalid value
    StreamElement invalidElement =
        StreamElement.upsert(
            transaction, commitDesc, 1L, "t", commitDesc.getName(), 1L, new byte[] {2, 3, 4});
    transformation.transform(invalidElement, (succ, exc) -> committed.set(succ));
    assertTrue(committed.get());
  }
}
