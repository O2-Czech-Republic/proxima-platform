/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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

import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asOnNextContext;
import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Test;

public class ThreadPooledObserverTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf"));
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Wildcard<byte[]> device = Wildcard.of(gateway, gateway.getAttribute("device.*"));

  @Test(timeout = 200_000)
  public void testParallelObserve() throws InterruptedException {
    List<Integer> elements = Collections.synchronizedList(new ArrayList<>());
    Set<Integer> confirmed = Collections.synchronizedSet(new TreeSet<>());
    ExecutorService executor = Executors.newCachedThreadPool();
    int parallelism = 20;
    int numElements = 2_000_000;
    ThreadPooledObserver observer =
        new ThreadPooledObserver(
            executor,
            (elem, context) -> {
              elements.add(Integer.valueOf(elem.getKey().substring(3)));
              context.confirm();
              return true;
            },
            parallelism);
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(parallelism);
    for (int i = 0; i < parallelism; i++) {
      int threadId = i;
      executor.submit(
          () -> {
            CountDownLatch confirmedLatch = new CountDownLatch(numElements / parallelism);
            for (int j = threadId; j < numElements; j += parallelism) {
              int item = j;
              observer.onNext(
                  device.upsert("key" + j, String.valueOf(j), now + j, new byte[] {}),
                  asOnNextContext(
                      (succ, exc) -> {
                        assertTrue(succ);
                        assertNull(exc);
                        confirmed.add(item);
                        confirmedLatch.countDown();
                      },
                      null));
            }
            ExceptionUtils.ignoringInterrupted(confirmedLatch::await);
            latch.countDown();
          });
    }
    latch.await();
    assertEquals(numElements, elements.stream().distinct().count());
    assertEquals(numElements, confirmed.size());
  }

  @Test(timeout = 20_000)
  public void testParallelObserveWithRepartition() throws InterruptedException {
    List<Integer> elements = Collections.synchronizedList(new ArrayList<>());
    Set<Integer> confirmed = Collections.synchronizedSet(new TreeSet<>());
    ExecutorService executor = Executors.newCachedThreadPool();
    int parallelism = 20;
    int numElements = 50_000;
    ThreadPooledObserver observer =
        new ThreadPooledObserver(
            executor,
            (elem, context) -> {
              elements.add(Integer.valueOf(elem.getKey().substring(3)));
              context.confirm();
              return true;
            },
            parallelism);
    long now = System.currentTimeMillis();
    CountDownLatch confirmedLatch = new CountDownLatch(numElements);
    for (int i = 0; i < numElements; i++) {
      int item = i;
      observer.onNext(
          device.upsert("key" + i, String.valueOf(i), now + i, new byte[] {}),
          asOnNextContext(
              (succ, exc) -> {
                assertTrue(succ);
                assertNull(exc);
                confirmed.add(item);
                confirmedLatch.countDown();
              },
              null));
      if (item % 83 == 0) {
        observer.onRepartition(Collections::emptyList);
      }
    }
    confirmedLatch.await();
    observer.onCompleted();
    assertEquals(numElements, elements.stream().distinct().count());
    assertEquals(numElements, confirmed.size());
  }
}
