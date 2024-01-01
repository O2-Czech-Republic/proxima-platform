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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.ConfigConstants;
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
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionCommitTransformationTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<?> status = gateway.getAttribute("status");
  private final AttributeDescriptor<?> device = gateway.getAttribute("device.*");
  private final EntityDescriptor transaction = repo.getEntity(ConfigConstants.TRANSACTION_ENTITY);
  private final AttributeDescriptor<Commit> commitDesc = transaction.getAttribute("commit");
  private final AttributeDescriptor<State> stateDesc = transaction.getAttribute("state");
  private final AttributeDescriptor<Response> responseDesc = transaction.getAttribute("response.*");
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
    StreamElement upsert =
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key",
            status.getName(),
            0L,
            new byte[] {1, 2, 3});
    StreamElement delete =
        StreamElement.delete(
            gateway, status, UUID.randomUUID().toString(), "key", status.getName(), 1L);
    Commit commit = Commit.of(1L, 1234567890000L, Arrays.asList(upsert, delete));
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
    assertEquals(commit.getUpdates().get(0), outputs.get(0));
    assertEquals(commit.getUpdates().get(1), outputs.get(1));
  }

  @Test
  public void testTransformTransactionUpdate() throws InterruptedException {
    long now = System.currentTimeMillis();
    Commit commit =
        Commit.of(
            Arrays.asList(
                new TransactionUpdate(
                    "all-transaction-commit-log-state",
                    StreamElement.upsert(
                        transaction,
                        stateDesc,
                        UUID.randomUUID().toString(),
                        "t",
                        stateDesc.getName(),
                        now,
                        stateDesc.getValueSerializer().serialize(State.empty()))),
                new TransactionUpdate(
                    "all-transaction-commit-log-response",
                    StreamElement.upsert(
                        transaction,
                        responseDesc,
                        UUID.randomUUID().toString(),
                        "t",
                        responseDesc.toAttributePrefix() + "1",
                        now,
                        responseDesc
                            .getValueSerializer()
                            .serialize(
                                Response.forRequest(
                                    Request.builder().responsePartitionId(0).build()))))));
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
}
