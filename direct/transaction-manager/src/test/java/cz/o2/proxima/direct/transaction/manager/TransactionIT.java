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
package cz.o2.proxima.direct.transaction.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.transaction.ClientTransactionManager;
import cz.o2.proxima.direct.transaction.TransactionManager;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.Response.Flags;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

/** A complete integration test for transaction processing. */
@Slf4j
public class TransactionIT {

  private final Random random = new Random();
  private final Repository repo =
      Repository.of(ConfigFactory.load("transactions-it.conf").resolve());
  private final EntityDescriptor user = repo.getEntity("user");
  private final Regular<Double> amount = Regular.regular(user, user.getAttribute("amount"));
  private final Wildcard<Integer> numDevices =
      Wildcard.wildcard(user, user.getAttribute("numDevices.*"));
  private final Wildcard<byte[]> device = Wildcard.wildcard(user, user.getAttribute("device.*"));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private TransactionLogObserver observer;
  private CachedView view;
  private ClientTransactionManager client;

  @Before
  public void setUp() {
    observer = new TransactionLogObserver(direct);
    client = TransactionManager.client(direct);
    view = Optionals.get(direct.getCachedView(amount));
    view.assign(view.getPartitions());
    observer.run("transaction-observer");
  }

  @Test(timeout = 100_000)
  public void testAtomicAmountTransfer() throws InterruptedException {
    // we begin with all amounts equal to zero
    // we randomly reshuffle random amounts between users and then we verify, that the sum is zero

    int numThreads = 50;
    int numSwaps = 1000;
    int numUsers = 20;
    CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService service = direct.getContext().getExecutorService();
    AtomicReference<Throwable> err = new AtomicReference<>();

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
            latch.countDown();
          });
    }
    latch.await();
    if (err.get() != null) {
      throw new RuntimeException(err.get());
    }
    verifyZeroSum(numUsers);
  }

  @Test(timeout = 100_000)
  public void testWildcardAttributeListAtomic() throws InterruptedException {
    // in each transaction, we add a new device into device.* and write sum of all current
    // devices into numDevices. After the test, the numDevice must match the total number of writes

    int numWrites = 1000;
    int numThreads = 20;
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
    verifyNumDevicesMatch(numWrites, numUsers);
  }

  @Test(timeout = 100_000)
  public void testWildcardAttributeListWithDeleteAtomic() throws InterruptedException {
    // in each transaction, we remove random device and add two new into device.* and write sum of
    // all current devices into numDevices. After the test, the numDevice must match the total
    // number of writes

    int numWrites = 1000;
    int numThreads = 20;
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
    if (err.get() != null) {
      throw new RuntimeException(err.get());
    }
    verifyNumDevicesMatch(numWrites, numUsers, true);
  }

  private void removeSingleDevice(int numUsers) throws InterruptedException {
    long abortWaitDuration = (long) (random.nextDouble() * 40 + 10);
    do {
      String userId = "user" + random.nextInt(numUsers);
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Response> responses = new ArrayBlockingQueue<>(1);
      List<StreamElement> devices = new ArrayList<>();
      view.scanWildcard(userId, device, devices::add);

      List<KeyAttribute> keyAttributes =
          KeyAttributes.ofWildcardQueryElements(user, userId, device, devices);

      if (devices.isEmpty()) {
        continue;
      }

      long stamp = System.currentTimeMillis();
      client.begin(
          transactionId,
          (id, resp) -> ExceptionUtils.unchecked(() -> responses.put(resp)),
          keyAttributes);

      Response response = responses.take();
      if (response.getFlags() != Flags.OPEN) {
        TimeUnit.MILLISECONDS.sleep(abortWaitDuration);
        abortWaitDuration *= 2;
        continue;
      }
      long sequentialId = response.getSeqId();

      String name =
          device.extractSuffix(devices.get(random.nextInt(devices.size())).getAttribute());
      StreamElement deviceUpdate = device.delete(sequentialId, userId, name, stamp);
      StreamElement numDevicesUpdate =
          numDevices.upsert(sequentialId, userId, "all", stamp, devices.size() - 1);

      client.commit(
          transactionId,
          Arrays.asList(
              KeyAttributes.ofStreamElement(deviceUpdate),
              KeyAttributes.ofStreamElement(numDevicesUpdate)));

      response = responses.take();
      if (response.getFlags() != Flags.COMMITTED) {
        TimeUnit.MILLISECONDS.sleep(abortWaitDuration);
        abortWaitDuration *= 2;
        continue;
      }

      CountDownLatch latch = new CountDownLatch(2);
      CommitCallback callback = (succ, exc) -> latch.countDown();
      view.write(deviceUpdate, callback);
      view.write(numDevicesUpdate, callback);
      latch.await();
      break;

    } while (true);
  }

  private void writeSingleDevice(int numUsers) throws InterruptedException {
    writeSingleDevice(numUsers, false);
  }

  private void writeSingleDevice(int numUsers, boolean intoAll) throws InterruptedException {
    String name = UUID.randomUUID().toString();
    String userId = "user" + random.nextInt(numUsers);
    long abortWaitDuration = (long) (random.nextDouble() * 40 + 10);
    do {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Response> responses = new ArrayBlockingQueue<>(1);
      List<StreamElement> devices = new ArrayList<>();
      view.scanWildcard(userId, device, devices::add);

      List<KeyAttribute> keyAttributes =
          KeyAttributes.ofWildcardQueryElements(user, userId, device, devices);

      long stamp = System.currentTimeMillis();
      client.begin(
          transactionId,
          (id, resp) -> ExceptionUtils.unchecked(() -> responses.put(resp)),
          keyAttributes);

      Response response = responses.take();
      if (response.getFlags() != Flags.OPEN) {
        TimeUnit.MILLISECONDS.sleep(abortWaitDuration);
        abortWaitDuration *= 2;
        continue;
      }
      long sequentialId = response.getSeqId();

      StreamElement deviceUpdate = device.upsert(sequentialId, userId, name, stamp, new byte[] {});
      final StreamElement numDevicesUpdate;
      int count = devices.size() + 1;
      if (intoAll) {
        numDevicesUpdate = numDevices.upsert(sequentialId, userId, "all", stamp, count);
      } else {
        numDevicesUpdate =
            numDevices.upsert(sequentialId, userId, String.valueOf(count), stamp, count);
      }

      client.commit(
          transactionId,
          Arrays.asList(
              KeyAttributes.ofStreamElement(deviceUpdate),
              KeyAttributes.ofStreamElement(numDevicesUpdate)));

      response = responses.take();
      if (response.getFlags() != Flags.COMMITTED) {
        TimeUnit.MILLISECONDS.sleep(abortWaitDuration);
        abortWaitDuration *= 2;
        continue;
      }

      CountDownLatch latch = new CountDownLatch(2);
      CommitCallback callback = (succ, exc) -> latch.countDown();
      view.write(deviceUpdate, callback);
      view.write(numDevicesUpdate, callback);
      latch.await();
      break;

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
        numDeviceAttrs.set(numAllDevices.get().getParsedRequired());
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
    long abortWaitDuration = (long) (random.nextDouble() * 40 + 10);
    do {
      BlockingQueue<Response> responses = new ArrayBlockingQueue<>(1);
      String transactionId = UUID.randomUUID().toString();
      Optional<KeyValue<Double>> firstAmount = view.get(userFirst, amount);
      Optional<KeyValue<Double>> secondAmount = view.get(userSecond, amount);

      client.begin(
          transactionId,
          (id, resp) -> ExceptionUtils.unchecked(() -> responses.put(resp)),
          Arrays.asList(
              KeyAttributes.ofAttributeDescriptor(
                  user, userFirst, amount, firstAmount.map(KeyValue::getSequentialId).orElse(1L)),
              KeyAttributes.ofAttributeDescriptor(
                  user,
                  userSecond,
                  amount,
                  secondAmount.map(KeyValue::getSequentialId).orElse(1L))));

      Response response = responses.take();
      if (response.getFlags() != Flags.OPEN) {
        TimeUnit.MILLISECONDS.sleep(abortWaitDuration);
        abortWaitDuration *= 2;
        continue;
      }
      long sequentialId = response.getSeqId();

      // we are inside transaction

      double firstWillHave = firstAmount.map(KeyValue::getParsedRequired).orElse(0.0) - swap;
      double secondWillHave = secondAmount.map(KeyValue::getParsedRequired).orElse(0.0) + swap;

      client.commit(
          transactionId,
          Arrays.asList(
              KeyAttributes.ofAttributeDescriptor(user, userFirst, amount, sequentialId),
              KeyAttributes.ofAttributeDescriptor(user, userSecond, amount, sequentialId)));

      response = responses.take();
      if (response.getFlags() != Flags.COMMITTED) {
        TimeUnit.MILLISECONDS.sleep(abortWaitDuration);
        abortWaitDuration *= 2;
        continue;
      }

      CountDownLatch latch = new CountDownLatch(2);
      CommitCallback callback = (succ, exc) -> latch.countDown();
      long stamp = System.currentTimeMillis();
      view.write(amount.upsert(sequentialId, userFirst, stamp, firstWillHave), callback);
      view.write(amount.upsert(sequentialId, userSecond, stamp, secondWillHave), callback);
      latch.await();
      break;

    } while (true);
  }
}
