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
package cz.o2.proxima.direct.core.view;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Test;

/** Test suite for {@link CachedView}. */
public class PartitionedCachedViewTest {

  final Config nonReplicated =
      ConfigFactory.load().withFallback(ConfigFactory.load("test-reference.conf")).resolve();
  final Config replicated =
      ConfigFactory.load()
          .withFallback(ConfigFactory.load("test-replicated.conf"))
          .withFallback(ConfigFactory.load("test-reference.conf"))
          .resolve();

  ConfigRepository repo;
  DirectDataOperator direct;
  EntityDescriptor gateway;

  private void setUp() {
    gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  }

  @After
  public void tearDown() {
    direct.close();
    repo.drop();
  }

  @Test(timeout = 10000)
  public void testCommitLogCaching() throws InterruptedException {
    repo = (ConfigRepository) Repository.ofTest(nonReplicated);
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    setUp();
    testStatusReadWrite();
    testScanWildcardAll();
  }

  @Test(timeout = 10000)
  public void testCommitLogCachingReplicated() throws InterruptedException {
    repo = (ConfigRepository) Repository.ofTest(replicated);
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    setUp();
    testStatusReadWrite();
    testScanWildcardAll();
  }

  private void testStatusReadWrite() throws InterruptedException {
    AttributeDescriptor<Object> status =
        gateway
            .findAttribute("status")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
    DirectAttributeFamilyDescriptor cachedFamily =
        direct.getFamiliesForAttribute(status).stream()
            .filter(af -> af.getDesc().getAccess().canCreateCachedView())
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Status has no cached view"));
    CachedView view = Optionals.get(cachedFamily.getCachedView());
    // read all partitions
    CountDownLatch latch = new CountDownLatch(2);
    view.assign(
        Optionals.get(cachedFamily.getCommitLogReader()).getPartitions(),
        (update, current) -> latch.countDown());
    writer.write(
        StreamElement.upsert(
            gateway,
            status,
            "uuid",
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
    Optional<KeyValue<Object>> kv = view.get("key", status);
    assertTrue(kv.isPresent());
    assertEquals(status, kv.get().getAttributeDescriptor());
  }

  private void testScanWildcardAll() throws InterruptedException {
    AttributeDescriptor<Object> status =
        gateway
            .findAttribute("status")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));
    AttributeDescriptor<Object> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));

    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));

    DirectAttributeFamilyDescriptor cachedFamily =
        direct.getFamiliesForAttribute(status).stream()
            .filter(af -> af.getDesc().getAccess().canCreateCachedView())
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Status has no cached view"));

    CachedView view = Optionals.get(cachedFamily.getCachedView());
    // read all partitions
    CountDownLatch latch = new CountDownLatch(4);
    view.assign(
        Optionals.get(cachedFamily.getCommitLogReader()).getPartitions(),
        (update, current) -> latch.countDown());
    writer.write(
        StreamElement.upsert(
            gateway,
            status,
            "uuid",
            "key",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3}),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            gateway,
            device,
            "uuid2",
            "key",
            device.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {2, 3}),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    List<KeyValue<?>> kvs = new ArrayList<>();
    view.scanWildcardAll("key", kvs::add);
    assertEquals(2, kvs.size());
  }
}
