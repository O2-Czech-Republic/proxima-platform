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
package cz.o2.proxima.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.manager.TransactionLogObserver;
import cz.o2.proxima.proto.service.Rpc.BeginTransactionRequest;
import cz.o2.proxima.proto.service.Rpc.BeginTransactionResponse;
import cz.o2.proxima.proto.service.Rpc.GetRequest;
import cz.o2.proxima.proto.service.Rpc.GetResponse;
import cz.o2.proxima.proto.service.Rpc.Ingest;
import cz.o2.proxima.proto.service.Rpc.IngestBulk;
import cz.o2.proxima.proto.service.Rpc.StatusBulk;
import cz.o2.proxima.proto.service.Rpc.TransactionCommitRequest;
import cz.o2.proxima.proto.service.Rpc.TransactionCommitResponse;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.transaction.TransactionContext;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.TransformationRunner;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test {@link IngestService} and {@link RetrieveService} with regards to transactions. */
public class TransactionsTest {

  private final Random random = new Random();
  private final Repository repo =
      Repository.of(ConfigFactory.load("test-transactions.conf").resolve());
  private final EntityDescriptor user = repo.getEntity("user");
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Wildcard<byte[]> userGateways = Wildcard.of(user, user.getAttribute("gateway.*"));
  private final Wildcard<byte[]> gatewayUsers =
      Wildcard.of(gateway, gateway.getAttribute("user.*"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private ScheduledThreadPoolExecutor scheduler;
  private ObserveHandle transformationHandle;
  private TransactionContext transactionContext;
  private IngestService ingest;
  private RetrieveService retrieve;

  @Before
  public void setUp() {
    transformationHandle =
        TransformationRunner.runTransformation(
            direct,
            "_transaction-commit",
            repo.getTransformations().get("_transaction-commit"),
            ign -> {});
    direct
        .getServerTransactionManager()
        .runObservations("testObserver", new TransactionLogObserver(direct));
    scheduler = new ScheduledThreadPoolExecutor(1);
    transactionContext = new TransactionContext(direct);
    ingest = new IngestService(repo, direct, transactionContext, scheduler);
    retrieve = new RetrieveService(repo, direct, transactionContext);
  }

  @After
  public void tearDown() {
    scheduler.shutdown();
    transformationHandle.close();
    direct.close();
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

  @Test(timeout = 10000)
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
    ListStreamObserver<StatusBulk> ingestObserver = intoList();
    retrieve.begin(BeginTransactionRequest.newBuilder().build(), beginObserver);
    List<BeginTransactionResponse> responses = beginObserver.getOutputs();
    assertEquals(1, responses.size());
    String transactionId = responses.get(0).getTransactionId();
    transactionContext.close();
    assertEquals(
        State.Flags.ABORTED,
        direct.getServerTransactionManager().getCurrentState(transactionId).getFlags());
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
    ListStreamObserver<BeginTransactionResponse> beginObserver = intoList();
    retrieve.begin(BeginTransactionRequest.newBuilder().build(), beginObserver);
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
                updates
                    .stream()
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
