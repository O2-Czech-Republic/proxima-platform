/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.view;

import static org.junit.Assert.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
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

  final ConfigRepository repo = (ConfigRepository) Repository.of(nonReplicated);
  final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  EntityDescriptor gateway;

  @Before
  public void setUp() {
    repo.reloadConfig(true, nonReplicated);
    gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  }

  @Test(timeout = 10000)
  public void testCommitLogCaching() throws InterruptedException {
    testStatusReadWrite(repo);
    testScanWildcardAll(repo);
  }

  @Test(timeout = 10000)
  public void testCommitLogCachingReplicated() throws InterruptedException {
    repo.reloadConfig(true, replicated);
    testStatusReadWrite(repo);
    testScanWildcardAll(repo);
  }

  private void testStatusReadWrite(final Repository repo) throws InterruptedException {
    AttributeDescriptor<Object> status =
        gateway
            .findAttribute("status")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));
    OnlineAttributeWriter writer =
        direct
            .getWriter(status)
            .orElseThrow(() -> new IllegalStateException("Missing writer for status"));
    DirectAttributeFamilyDescriptor cachedFamily =
        direct
            .getFamiliesForAttribute(status)
            .stream()
            .filter(af -> af.getDesc().getAccess().canCreateCachedView())
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Status has no cached view"));
    CachedView view = cachedFamily.getCachedView().get();
    // read all partitions
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    view.assign(
        cachedFamily.getCommitLogReader().get().getPartitions(),
        (update, current) -> {
          Optional.ofNullable(latch.get()).ifPresent(CountDownLatch::countDown);
        });
    latch.set(new CountDownLatch(2));
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
          latch.get().countDown();
        });
    latch.get().await();
    Optional<KeyValue<Object>> kv = view.get("key", status);
    assertTrue(kv.isPresent());
    assertEquals(status, kv.get().getAttributeDescriptor());
  }

  private void testScanWildcardAll(final Repository repo) throws InterruptedException {
    AttributeDescriptor<Object> status =
        gateway
            .findAttribute("status")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));
    AttributeDescriptor<Object> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute status"));

    OnlineAttributeWriter writer =
        direct
            .getWriter(status)
            .orElseThrow(() -> new IllegalStateException("Missing writer for status"));

    DirectAttributeFamilyDescriptor cachedFamily =
        direct
            .getFamiliesForAttribute(status)
            .stream()
            .filter(af -> af.getDesc().getAccess().canCreateCachedView())
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Status has no cached view"));

    CachedView view = cachedFamily.getCachedView().get();
    // read all partitions
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    view.assign(
        cachedFamily.getCommitLogReader().get().getPartitions(),
        (update, current) -> {
          Optional.ofNullable(latch.get()).ifPresent(CountDownLatch::countDown);
        });
    latch.set(new CountDownLatch(4));
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
          latch.get().countDown();
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
          latch.get().countDown();
        });
    latch.get().await();
    List<KeyValue<?>> kvs = new ArrayList<>();
    view.scanWildcardAll("key", kvs::add);
    assertEquals(2, kvs.size());
  }
}
