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
package cz.o2.proxima.direct.server.transaction;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.TransformationRunner;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.transaction.ServerTransactionManager;
import cz.o2.proxima.direct.server.IngestService;
import cz.o2.proxima.direct.server.RetrieveService;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.BeginTransactionRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.BeginTransactionResponse;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.GetRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.GetResponse;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.Ingest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.IngestBulk;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.StatusBulk;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.TransactionCommitRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.TransactionCommitResponse;
import cz.o2.proxima.direct.transaction.manager.Metrics;
import cz.o2.proxima.direct.transaction.manager.TransactionLogObserver;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test {@link IngestService} and {@link RetrieveService} regarding transactions. */
@Slf4j
public class TransactionsTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-transactions.conf").resolve());
  private final EntityDescriptor user = repo.getEntity("user");
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Wildcard<byte[]> userGateways = Wildcard.of(user, user.getAttribute("gateway.*"));
  private final Wildcard<byte[]> gatewayUsers =
      Wildcard.of(gateway, gateway.getAttribute("user.*"));
  private final Regular<Integer> intField = Regular.of(gateway, gateway.getAttribute("intField"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private ScheduledThreadPoolExecutor scheduler;
  private ObserveHandle transformationHandle;
  private TransactionContext transactionContext;
  private IngestService ingest;
  private RetrieveService retrieve;
  private TransactionLogObserver observer;

  @Before
  public void setUp() {
    transformationHandle =
        TransformationRunner.runTransformation(
            direct,
            "_transaction-commit",
            repo.getTransformations().get("_transaction-commit"),
            ign -> {});
    observer =
        new TransactionLogObserver(direct, new Metrics()) {
          @Override
          protected void assertSingleton() {}
        };
    observer.run("testObserver");
    scheduler = new ScheduledThreadPoolExecutor(1);
    transactionContext = new TransactionContext(direct);
    ingest = new IngestService(repo, direct, transactionContext, scheduler);
    retrieve = new RetrieveService(repo, direct, transactionContext);
  }

  @After
  public void tearDown() {
    scheduler.shutdown();
    observer.onCancelled();
    transformationHandle.close();
    transactionContext.close();
  }

  @Test(timeout = 10000)
  public void testTransactionReadWrite() {
    BeginTransactionResponse response = begin();
    String transactionId = response.getTransactionId();
    assertFalse(transactionId.isEmpty());
    long stamp = System.currentTimeMillis();
    StatusBulk status =
        ingestBulk(
            transactionId,
            StreamElement.upsert(
                gateway,
                gatewayUsers,
                UUID.randomUUID().toString(),
                "gw1",
                gatewayUsers.toAttributePrefix() + "usr1",
                stamp,
                new byte[] {1, 2, 3}),
            StreamElement.upsert(
                user,
                userGateways,
                UUID.randomUUID().toString(),
                "usr1",
                userGateways.toAttributePrefix() + "gw1",
                stamp,
                new byte[] {1, 2, 3}));

    assertEquals(200, status.getStatus(0).getStatus());
    assertEquals(200, status.getStatus(1).getStatus());

    // written, but not yet committed
    GetResponse getResponse =
        get(
            GetRequest.newBuilder()
                .setEntity("user")
                .setKey("usr1")
                .setAttribute("gateway.gw1")
                .build());
    assertEquals(404, getResponse.getStatus());

    TransactionCommitResponse commitResponse = commit(transactionId);
    assertEquals(TransactionCommitResponse.Status.COMMITTED, commitResponse.getStatus());

    // verify we can read the results
    getResponse =
        get(
            GetRequest.newBuilder()
                .setEntity("user")
                .setKey("usr1")
                .setAttribute("gateway.gw1")
                .build());
    assertEquals(200, getResponse.getStatus());
    assertEquals(3, getResponse.getValue().size());
  }

  @Test(timeout = 20000)
  public void testTransactionCommitRejected() {
    // we need two simultaneous transactions
    String firstTransaction = begin().getTransactionId();
    String secondTransaction = begin().getTransactionId();
    long stamp = System.currentTimeMillis();
    // we read some data in the first transaction, ignore the result, must be empty
    get(
        GetRequest.newBuilder()
            .setTransactionId(firstTransaction)
            .setEntity("gateway")
            .setKey("gw1")
            .setAttribute("user.usr1")
            .build());
    // write the attribute in second transaction
    ingestBulk(
        secondTransaction,
        StreamElement.upsert(
            gateway,
            gatewayUsers,
            UUID.randomUUID().toString(),
            "gw1",
            gatewayUsers.toAttributePrefix() + "usr1",
            stamp,
            new byte[] {1, 2, 3}));
    assertEquals(TransactionCommitResponse.Status.COMMITTED, commit(secondTransaction).getStatus());
    // write the same in different transaction
    ingestBulk(
        firstTransaction,
        StreamElement.upsert(
            gateway,
            gatewayUsers,
            UUID.randomUUID().toString(),
            "gw1",
            gatewayUsers.toAttributePrefix() + "usr1",
            stamp,
            new byte[] {1, 2, 3, 4}));
    assertEquals(TransactionCommitResponse.Status.REJECTED, commit(firstTransaction).getStatus());

    GetResponse getResponse =
        get(
            GetRequest.newBuilder()
                .setEntity("gateway")
                .setKey("gw1")
                .setAttribute("user.usr1")
                .build());
    assertEquals(200, getResponse.getStatus());
    // the value corresponds to the second transaction
    assertEquals(3, getResponse.getValue().size());
  }

  @Test(timeout = 10000)
  public void testTransactionRollbackOnClose() {
    ListStreamObserver<BeginTransactionResponse> beginObserver = intoList();
    retrieve.begin(BeginTransactionRequest.newBuilder().build(), beginObserver);
    List<BeginTransactionResponse> responses = beginObserver.getOutputs();
    assertEquals(1, responses.size());
    String transactionId = responses.get(0).getTransactionId();
    transactionContext.close();
    ServerTransactionManager manager = observer.getRawManager();
    while (manager.getCurrentState(transactionId).getFlags() != State.Flags.UNKNOWN
        && manager.getCurrentState(transactionId).getFlags() != State.Flags.ABORTED) {
      if (ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(100))) {
        break;
      }
    }
  }

  @Test
  public void testCommitInvalidIngest() {
    BeginTransactionResponse response = begin();
    String transactionId = response.getTransactionId();
    long stamp = System.currentTimeMillis();
    StatusBulk status =
        ingestBulk(
            transactionId,
            StreamElement.upsert(
                gateway,
                intField,
                UUID.randomUUID().toString(),
                "gw1",
                intField.getName(),
                stamp,
                new byte[] {}));

    assertEquals(412, status.getStatus(0).getStatus());
    TransactionCommitResponse commitResponse = commit(transactionId);
    assertEquals(TransactionCommitResponse.Status.FAILED, commitResponse.getStatus());
  }

  @Test
  public void testBeginDuplicate() {
    BeginTransactionResponse response = begin();
    String transactionId = response.getTransactionId();
    assertFalse(transactionId.isEmpty());
    long stamp = System.currentTimeMillis();
    StatusBulk status =
        ingestBulk(
            transactionId,
            StreamElement.upsert(
                gateway,
                gatewayUsers,
                UUID.randomUUID().toString(),
                "gw1",
                gatewayUsers.toAttributePrefix() + "usr1",
                stamp,
                new byte[] {1, 2, 3}));

    assertEquals(200, status.getStatus(0).getStatus());

    TransactionCommitResponse commitResponse = commit(transactionId);
    assertEquals(TransactionCommitResponse.Status.COMMITTED, commitResponse.getStatus());

    response = begin(transactionId);
    assertEquals(transactionId, response.getTransactionId());

    GetResponse getResponse =
        get(
            GetRequest.newBuilder()
                .setTransactionId(transactionId)
                .setEntity("gateway")
                .setKey("gw1")
                .setAttribute("user.usr1")
                .build());
    assertEquals(204, getResponse.getStatus());
  }

  @Test
  public void testTransactionRollbackOnCleanup() {
    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    try (TransactionContext context = new TransactionContext(direct, clock::get)) {
      context.run();
      String transactionId = context.create();
      context.clearAnyStaleTransactions();
      assertEquals(transactionId, context.get(transactionId).getTransactionId());
      clock.addAndGet(86400000L);
      context.clearAnyStaleTransactions();
      assertNull(context.getTransactionMap().get(transactionId));
    }
  }

  private TransactionCommitResponse commit(String transactionId) {
    ListStreamObserver<TransactionCommitResponse> commitResponseObserver = intoList();
    // commit
    ingest.commit(
        TransactionCommitRequest.newBuilder().setTransactionId(transactionId).build(),
        commitResponseObserver);
    return Iterables.getOnlyElement(commitResponseObserver.getOutputs());
  }

  private GetResponse get(GetRequest request) {
    ListStreamObserver<GetResponse> getResponseObserver = intoList();
    // written, but not yet committed
    retrieve.get(request, getResponseObserver);
    return Iterables.getOnlyElement(getResponseObserver.getOutputs());
  }

  private BeginTransactionResponse begin() {
    return begin(null);
  }

  private BeginTransactionResponse begin(@Nullable String transactionId) {
    ListStreamObserver<BeginTransactionResponse> beginObserver = intoList();
    retrieve.begin(
        BeginTransactionRequest.newBuilder()
            .setTranscationId(MoreObjects.firstNonNull(transactionId, ""))
            .build(),
        beginObserver);
    return Iterables.getOnlyElement(beginObserver.getOutputs());
  }

  private StatusBulk ingestBulk(String transactionId, StreamElement... updates) {
    return ingestBulk(transactionId, Arrays.asList(updates));
  }

  private StatusBulk ingestBulk(String transactionId, List<StreamElement> updates) {
    ListStreamObserver<StatusBulk> ingestObserver = intoList();
    StreamObserver<IngestBulk> ingestBulk = ingest.ingestBulk(ingestObserver);
    ingestBulk.onNext(
        IngestBulk.newBuilder()
            .addAllIngest(
                updates.stream()
                    .map(
                        el ->
                            Ingest.newBuilder()
                                .setUuid(el.getUuid())
                                .setTransactionId(transactionId)
                                .setEntity(el.getEntityDescriptor().getName())
                                .setKey(el.getKey())
                                .setAttribute(el.getAttribute())
                                .setStamp(el.getStamp())
                                .setValue(ByteString.copyFrom(el.getValue()))
                                .build())
                    .collect(Collectors.toList()))
            .build());
    ingestBulk.onCompleted();
    return Iterables.getOnlyElement(ingestObserver.getOutputs());
  }

  private static <T> ListStreamObserver<T> intoList() {
    return new ListStreamObserver<>();
  }

  private static class ListStreamObserver<T> implements StreamObserver<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final List<T> outputs = new ArrayList<>();

    @Override
    public void onNext(T t) {
      outputs.add(t);
    }

    @Override
    public void onError(Throwable throwable) {
      throw new RuntimeException(throwable);
    }

    @Override
    public void onCompleted() {
      latch.countDown();
    }

    public List<T> getOutputs() {
      ExceptionUtils.unchecked(latch::await);
      return outputs;
    }
  }
}
