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
package cz.o2.proxima.direct.transaction.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.annotations.DeclaredThreadSafe;
import cz.o2.proxima.core.repository.ConfigConstants;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Commit;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.KeyAttributes;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.Response.Flags;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.TransformationRunner;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.transaction.ClientTransactionManager;
import cz.o2.proxima.direct.core.transaction.TransactionResourceManager;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.Transaction;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** A complete integration test for transaction processing. */
@Slf4j
public class TransactionIT_Large {

  private final Random random = new Random();
  private final Config config = ConfigFactory.load("transactions-it.conf").resolve();
  private final Repository repo = Repository.ofTest(config);
  private final EntityDescriptor user = repo.getEntity("user");
  private final Regular<Double> amount = Regular.of(user, user.getAttribute("amount"));
  private final Wildcard<Integer> numDevices = Wildcard.of(user, user.getAttribute("numDevices.*"));
  private final Wildcard<byte[]> device = Wildcard.of(user, user.getAttribute("device.*"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private CachedView view;
  private ClientTransactionManager client;
  private ObserveHandle transformationHandle;
  private volatile @Nullable CountDownLatch replicatedLatch;

  @Before
  public void setUp() {
    TransactionLogObserver observer = new LimitedParallelismTransactionLogObserver(direct);
    Map<String, Object> cfg =
        config.hasPath(ConfigConstants.TRANSACTIONS)
            ? config.getObject(ConfigConstants.TRANSACTIONS).unwrapped()
            : Collections.emptyMap();
    client = new TransactionResourceManager(direct, cfg);
    view = Optionals.get(direct.getCachedView(amount));
    view.assign(view.getPartitions());
    observer.run("transaction-observer");
    EntityDescriptor transaction = repo.getEntity("_transaction");
    Regular<Commit> commit = Regular.of(transaction, transaction.getAttribute("commit"));
    transformationHandle =
        TransformationRunner.runTransformation(
            direct,
            "_transaction-commit",
            repo.getTransformations().get("_transaction-commit"),
            elem -> {
              Commit value = Optionals.get(commit.valueOf(elem));
              if (!value.getOutputs().isEmpty()) {
                Optional.ofNullable(replicatedLatch).ifPresent(CountDownLatch::countDown);
              }
            });
  }

  @After
  public void tearDown() {
    transformationHandle.close();
    client.close();
    direct.close();
  }

  @Test
  public void testAtomicAmountTransfer() throws InterruptedException {
    // we begin with all amounts equal to zero
    // we randomly reshuffle random amounts between users and then we verify, that the sum is zero

    int numThreads = 50;
    int numSwaps = 100000;
    int numUsers = 100;
    ExecutorService service = direct.getContext().getExecutorService();
    AtomicReference<Throwable> err = new AtomicReference<>();
    replicatedLatch = new CountDownLatch(numSwaps);

    for (int i = 0; i < numThreads; i++) {
      service.submit(
          () -> {
            try {
              for (int j = 0; j < numSwaps / numThreads; j++) {
                transferAmountRandomly(numUsers);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Throwable ex) {
              log.error("Failed to run the transaction", ex);
              err.set(ex);
            }
          });
    }
    replicatedLatch.await();
    if (err.get() != null) {
      throw new RuntimeException(err.get());
    }
    verifyZeroSum(numUsers);
  }

  @Test
  public void testWildcardAttributeListAtomic() throws InterruptedException {
    // in each transaction, we add a new device into device.* and write sum of all current
    // devices into numDevices. After the test, the numDevice must match the total number of writes

    int numWrites = 50000;
    int numThreads = 50;
    int numUsers = 100;

    CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService service = direct.getContext().getExecutorService();
    AtomicReference<Throwable> err = new AtomicReference<>();
    int numWritesPerThread = numWrites / numThreads;
    for (int i = 0; i < numThreads; i++) {
      service.submit(
          () -> {
            try {
              for (int j = 0; j < numWritesPerThread; j++) {
                writeSingleDevice(numUsers);
              }
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            } catch (Throwable ex) {
              err.set(ex);
            }
            latch.countDown();
          });
    }

    latch.await();
    if (err.get() != null) {
      throw new RuntimeException(err.get());
    }
    TimeUnit.SECONDS.sleep(1);
    verifyNumDevicesMatch(numWrites, numUsers);
  }

  @Test
  public void testWildcardAttributeListWithDeleteAtomic() throws InterruptedException {
    // in each transaction, we remove random device and add two new into device.* and write sum of
    // all current devices into numDevices. After the test, the numDevice must match the total
    // number of writes

    int numWrites = 15000;
    int numThreads = 50;
    int numUsers = 50;

    CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService service = direct.getContext().getExecutorService();
    AtomicReference<Throwable> err = new AtomicReference<>();
    int numWritesPerThread = numWrites / numThreads;
    for (int i = 0; i < numThreads; i++) {
      service.submit(
          () -> {
            try {
              for (int j = 0; j < numWritesPerThread; j++) {
                writeSingleDevice(numUsers, true);
                writeSingleDevice(numUsers, true);
                removeSingleDevice(numUsers);
              }
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            } catch (Throwable ex) {
              err.set(ex);
            }
            latch.countDown();
          });
    }

    latch.await();
    TimeUnit.SECONDS.sleep(1);
    if (err.get() != null) {
      throw new RuntimeException(err.get());
    }
    verifyNumDevicesMatch(numWrites, numUsers, true);
  }

  @Test
  public void testDeletedAttributeGet() throws InterruptedException {
    // check atomic swap of data between two attributes
    // a value is read from attribute X, incremented and written to attribute Y and deleted from X
    // if value is not present in attribute X, it is read from attribute Y, and written to X

    // this test causes a lot of contention, so the throughput is limited
    int numWrites = 5000;
    int numThreads = 50;

    CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService service = direct.getContext().getExecutorService();
    AtomicReference<Throwable> err = new AtomicReference<>();
    int numWritesPerThread = numWrites / numThreads;
    assertEquals(numWritesPerThread * numThreads, numWrites);
    String attrA = device.toAttributePrefix() + "A";
    String attrB = device.toAttributePrefix() + "B";
    String key = "key";
    // seed value
    writeSeedValue(attrA, key);
    for (int i = 0; i < numThreads; i++) {
      service.submit(
          () -> {
            try {
              for (int j = 0; j < numWritesPerThread; j++) {
                swapValueBetween(key, attrA, attrB);
              }
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            } catch (Throwable ex) {
              err.set(ex);
            }
            latch.countDown();
          });
    }

    latch.await();
    TimeUnit.SECONDS.sleep(1);
    if (err.get() != null) {
      throw new RuntimeException(err.get());
    }
    verifyNumInAttributeIs(key, numWrites + 1, attrA);
  }

  @Test
  public void testDeletedAttributeGetWithFailedWrite() throws InterruptedException {
    // check atomic swap of data between two attributes
    // a value is read from attribute X, incremented and written to attribute Y and deleted from X
    // if value is not present in attribute X, it is read from attribute Y, and written to X

    int numWrites = 5000;
    int numThreads = 50;

    CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService service = direct.getContext().getExecutorService();
    AtomicReference<Throwable> err = new AtomicReference<>();
    int numWritesPerThread = numWrites / numThreads;
    assertEquals(numWritesPerThread * numThreads, numWrites);
    String attrA = device.toAttributePrefix() + "A";
    String attrB = device.toAttributePrefix() + "B";
    String key = "key";
    // seed value
    writeSeedValue(attrA, key);
    AtomicInteger failedWrites = new AtomicInteger();
    AtomicInteger succeeded = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      service.submit(
          () -> {
            try {
              for (int j = 0; j < numWritesPerThread; j++) {
                if (!swapValueBetween(key, attrA, attrB, true)) {
                  // failed write, repeat
                  j--;
                  failedWrites.incrementAndGet();
                }
              }
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            } catch (Throwable ex) {
              err.set(ex);
            }
            latch.countDown();
          });
    }

    latch.await();
    TimeUnit.SECONDS.sleep(1);
    if (err.get() != null) {
      throw new RuntimeException(err.get());
    }
    verifyNumInAttributeIs(key, numWrites + 1, attrA);
    assertTrue(failedWrites.get() > 0);
  }

  private void writeSeedValue(String attribute, String key) {
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(device));
    StreamElement upsert =
        device.upsert(
            key,
            device.extractSuffix(attribute),
            System.currentTimeMillis(),
            ByteBuffer.allocate(4).putInt(1).array());
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(upsert, (succ, exc) -> latch.countDown());
    ExceptionUtils.unchecked(latch::await);
  }

  private void swapValueBetween(String key, String attrA, String attrB)
      throws InterruptedException {

    swapValueBetween(key, attrA, attrB, false);
  }

  private boolean swapValueBetween(String key, String attrA, String attrB, boolean canFailWrite)
      throws InterruptedException {

    long retrySleep = 1;
    do {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Response> responses = new ArrayBlockingQueue<>(5);
      Optional<KeyValue<byte[]>> valA = view.get(key, attrA, device);
      Optional<KeyValue<byte[]>> valB = view.get(key, attrB, device);
      final List<KeyAttribute> fetched =
          Arrays.asList(
              valA.isPresent()
                  ? KeyAttributes.ofStreamElement(valA.get())
                  : KeyAttributes.ofMissingAttribute(
                      user, key, device, device.extractSuffix(attrA)),
              valB.isPresent()
                  ? KeyAttributes.ofStreamElement(valB.get())
                  : KeyAttributes.ofMissingAttribute(
                      user, key, device, device.extractSuffix(attrB)));

      client.begin(transactionId, fetched).thenApply(responses::add);

      Response response = responses.take();
      if (response.getFlags() != Flags.OPEN) {
        TimeUnit.MILLISECONDS.sleep(Math.min(8, retrySleep *= 2));
        continue;
      }
      long sequentialId = response.getSeqId();

      final List<StreamElement> updates;
      if (valA.isPresent()) {
        int currentVal = ByteBuffer.wrap(valA.get().getParsedRequired()).getInt();
        updates = updateAttributeAndRemove(sequentialId, key, attrB, attrA, currentVal);
      } else {
        int currentVal = ByteBuffer.wrap(valB.get().getParsedRequired()).getInt();
        updates = updateAttributeAndRemove(sequentialId, key, attrA, attrB, currentVal);
      }

      client.commit(transactionId, updates).thenApply(responses::add);
      response = responses.take();
      if (response.getFlags() != Flags.COMMITTED) {
        TimeUnit.MILLISECONDS.sleep(Math.min(8, retrySleep *= 2));
        continue;
      }

      CountDownLatch latch = new CountDownLatch(1);
      AtomicBoolean succeeded = new AtomicBoolean();
      CommitCallback callback =
          (succ, exc) -> {
            if (!succ) {
              client.rollback(transactionId);
            }
            succeeded.set(succ);
            latch.countDown();
          };
      if (canFailWrite && random.nextBoolean()) {
        callback.commit(false, new RuntimeException("Failed!"));
      } else {
        CommitCallback multiCallback = CommitCallback.afterNumCommits(updates.size(), callback);
        updates.forEach(u -> view.write(u, multiCallback));
      }
      latch.await();
      return succeeded.get();
    } while (true);
  }

  private List<StreamElement> updateAttributeAndRemove(
      long sequentialId, String key, String toUpdate, String toDelete, int currentVal) {
    List<StreamElement> ret = new ArrayList<>();
    ret.add(
        device.upsert(
            sequentialId,
            key,
            device.extractSuffix(toUpdate),
            System.currentTimeMillis(),
            ByteBuffer.allocate(4).putInt(currentVal + 1).array()));
    ret.add(
        device.delete(
            sequentialId, key, device.extractSuffix(toDelete), System.currentTimeMillis()));
    Collections.shuffle(ret);
    return ret;
  }

  private void verifyNumInAttributeIs(String key, int numWrites, String attr) {
    Optional<KeyValue<byte[]>> value = view.get(key, attr, device, Long.MAX_VALUE);
    assertTrue(value.isPresent());
    assertEquals(numWrites, ByteBuffer.wrap(value.get().getParsedRequired()).getInt());
  }

  private void removeSingleDevice(int numUsers) throws InterruptedException {
    do {
      TransactionalOnlineAttributeWriter writer =
          Optionals.get(direct.getWriter(device)).transactional();
      try (Transaction t = writer.begin()) {
        String userId = "user" + random.nextInt(numUsers);
        List<StreamElement> devices = new ArrayList<>();
        view.scanWildcard(userId, device, devices::add);

        List<KeyAttribute> keyAttributes =
            KeyAttributes.ofWildcardQueryElements(user, userId, device, devices);

        if (devices.isEmpty()) {
          continue;
        }

        long stamp = System.currentTimeMillis();
        t.update(keyAttributes);
        String name =
            device.extractSuffix(devices.get(random.nextInt(devices.size())).getAttribute());
        StreamElement deviceUpdate = device.delete(userId, name, stamp);
        StreamElement numDevicesUpdate =
            numDevices.upsert(userId, "all", stamp, devices.size() - 1);
        BlockingQueue<Optional<Throwable>> err = new ArrayBlockingQueue<>(1);
        t.commitWrite(
            Arrays.asList(deviceUpdate, numDevicesUpdate),
            (succ, exc) -> err.add(Optional.ofNullable(exc)));
        Optional<Throwable> res = err.take();
        if (res.isEmpty()) {
          break;
        }
        if (!(res.get() instanceof TransactionRejectedException)) {
          throw new IllegalStateException(res.get());
        }
      }
    } while (true);
  }

  private void writeSingleDevice(int numUsers) throws InterruptedException {
    writeSingleDevice(numUsers, false);
  }

  private void writeSingleDevice(int numUsers, boolean intoAll) throws InterruptedException {
    String name = UUID.randomUUID().toString();
    String userId = "user" + random.nextInt(numUsers);
    do {
      TransactionalOnlineAttributeWriter writer =
          Optionals.get(direct.getWriter(device)).transactional();

      long stamp = System.currentTimeMillis();
      try (Transaction t = writer.begin()) {
        List<StreamElement> devices = new ArrayList<>();
        view.scanWildcard(userId, device, devices::add);
        List<KeyAttribute> keyAttributes =
            KeyAttributes.ofWildcardQueryElements(user, userId, device, devices);
        t.update(keyAttributes);
        StreamElement deviceUpdate = device.upsert(userId, name, stamp, new byte[] {});
        final StreamElement numDevicesUpdate;
        int count = devices.size() + 1;
        if (intoAll) {
          numDevicesUpdate = numDevices.upsert(userId, "all", stamp, count);
        } else {
          numDevicesUpdate = numDevices.upsert(userId, String.valueOf(count), stamp, count);
        }

        BlockingQueue<Optional<Throwable>> err = new ArrayBlockingQueue<>(1);
        t.commitWrite(
            Arrays.asList(deviceUpdate, numDevicesUpdate),
            (succ, exc) -> err.add(Optional.ofNullable(exc)));
        Optional<Throwable> res = err.take();
        if (res.isEmpty()) {
          break;
        }
        if (!(res.get() instanceof TransactionRejectedException)) {
          throw new IllegalStateException(res.get());
        }
      }
    } while (true);
  }

  private void verifyNumDevicesMatch(int numWrites, int numUsers) {
    verifyNumDevicesMatch(numWrites, numUsers, false);
  }

  private void verifyNumDevicesMatch(int numWrites, int numUsers, boolean inCountAll) {
    int sum = 0;
    for (int i = 0; i < numUsers; i++) {
      String userId = "user" + i;
      AtomicInteger numDeviceAttrs = new AtomicInteger();
      AtomicInteger deviceAttrs = new AtomicInteger();
      view.scanWildcard(userId, device, d -> deviceAttrs.incrementAndGet());
      if (inCountAll) {
        Optional<KeyValue<Integer>> numAllDevices =
            view.get(userId, numDevices.toAttributePrefix() + "all", numDevices);
        numDeviceAttrs.set(numAllDevices.map(KeyValue::getParsedRequired).orElse(0));
      } else {
        view.scanWildcard(userId, numDevices, d -> numDeviceAttrs.incrementAndGet());
      }
      assertEquals(numDeviceAttrs.get(), deviceAttrs.get());
      sum += numDeviceAttrs.get();
    }
    assertEquals(numWrites, sum);
  }

  private void verifyZeroSum(int numUsers) {
    double sum = 0.0;
    int nonZeros = 0;
    for (int i = 0; i < numUsers; i++) {
      double value =
          view.get("user" + i, amount, Long.MAX_VALUE).map(KeyValue::getParsedRequired).orElse(0.0);
      if (value != 0.0) {
        nonZeros++;
      }
      sum += value;
    }
    assertEquals(0.0, sum, 0.0001);
    assertTrue(nonZeros > 0);
  }

  private void transferAmountRandomly(int numUsers) throws InterruptedException {
    int first = random.nextInt(numUsers);
    int second = (first + 1 + random.nextInt(numUsers - 1)) % numUsers;
    String userFirst = "user" + first;
    String userSecond = "user" + second;
    double swap = random.nextDouble() * 1000;
    TransactionalOnlineAttributeWriter writer =
        Optionals.get(direct.getWriter(amount)).transactional();
    String transactionId = UUID.randomUUID().toString();
    do {
      Optional<KeyValue<Double>> firstAmount = view.get(userFirst, amount);
      Optional<KeyValue<Double>> secondAmount = view.get(userSecond, amount);
      try (Transaction t = writer.begin(transactionId)) {
        t.update(
            Arrays.asList(
                KeyAttributes.ofAttributeDescriptor(
                    user, userFirst, amount, firstAmount.map(KeyValue::getSequentialId).orElse(1L)),
                KeyAttributes.ofAttributeDescriptor(
                    user,
                    userSecond,
                    amount,
                    secondAmount.map(KeyValue::getSequentialId).orElse(1L))));

        double firstWillHave = firstAmount.map(KeyValue::getParsedRequired).orElse(0.0) - swap;
        double secondWillHave = secondAmount.map(KeyValue::getParsedRequired).orElse(0.0) + swap;
        List<StreamElement> outputs =
            Arrays.asList(
                amount.upsert(userFirst, System.currentTimeMillis(), firstWillHave),
                amount.upsert(userSecond, System.currentTimeMillis(), secondWillHave));
        BlockingQueue<Optional<Throwable>> err = new ArrayBlockingQueue<>(1);
        t.commitWrite(outputs, (succ, exc) -> err.add(Optional.ofNullable(exc)));
        Optional<Throwable> res = err.take();
        if (res.isEmpty()) {
          break;
        }
        if (!(res.get() instanceof TransactionRejectedException)) {
          throw new IllegalStateException(res.get());
        }
        TransactionRejectedException ex = (TransactionRejectedException) res.get();
        if (ex.getResponseFlags() == Response.Flags.DUPLICATE) {
          return;
        } else if (ex.getResponseFlags() == Flags.ABORTED) {
          transactionId = UUID.randomUUID().toString();
        }
        // repeat
      }
    } while (true);
  }

  @DeclaredThreadSafe(allowedParallelism = 4)
  private static class LimitedParallelismTransactionLogObserver extends TransactionLogObserver {
    public LimitedParallelismTransactionLogObserver(DirectDataOperator direct) {
      super(direct, new Metrics());
    }
  }
}
