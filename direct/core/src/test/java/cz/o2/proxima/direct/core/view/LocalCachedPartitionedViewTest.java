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
package cz.o2.proxima.direct.core.view;

import static org.junit.Assert.*;

import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StorageType;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader.Listing;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.Test;

/** Test suite for {@link LocalCachedPartitionedView}. */
public class LocalCachedPartitionedViewTest {

  Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalStateException("Missing entity 'gateway'"));
  AttributeDescriptor<?> armed =
      gateway
          .findAttribute("armed")
          .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));
  AttributeDescriptor<?> device =
      gateway
          .findAttribute("device.*")
          .orElseThrow(() -> new IllegalStateException("Missing attribute device.*"));
  CommitLogReader reader =
      Sets.intersection(
              direct.getFamiliesForAttribute(armed), direct.getFamiliesForAttribute(device))
          .stream()
          .filter(af -> af.getDesc().getType() == StorageType.PRIMARY)
          .findAny()
          .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
          .orElseThrow(() -> new IllegalStateException("Missing commit log"));
  OnlineAttributeWriter writer =
      direct.getWriter(armed).orElseThrow(() -> new IllegalStateException("Missing writer"));
  Factory<Long> timeProvider = null;

  LocalCachedPartitionedView view =
      new LocalCachedPartitionedView(gateway, reader, writer) {
        @Override
        long getCurrentTimeMillis() {
          return Optional.ofNullable(timeProvider)
              .map(cz.o2.proxima.core.functional.Factory::apply)
              .orElseGet(super::getCurrentTimeMillis);
        }
      };
  long now = System.currentTimeMillis();

  @Test
  public void testWriteSimple() {
    view.assign(singlePartition());
    writer.write(update("key", armed, now), (succ, exc) -> {});
    assertTrue(view.get("key", armed, now).isPresent());
    assertFalse(view.get("key", armed, now - 1).isPresent());
    writer.write(delete("key", armed, now + 1), (succ, exc) -> {});
    assertFalse(view.get("key", armed, now + 1).isPresent());
    assertTrue(view.get("key", armed, now).isPresent());

    assertEquals(reader, view.getUnderlyingReader());
    assertEquals(writer, view.getUnderlyingWriter());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testWriteSimpleWithCallback() {
    AtomicReference<Pair<StreamElement, Pair<Long, byte[]>>> updated = new AtomicReference<>();
    view.assign(singlePartition(), (update, old) -> updated.set(Pair.of(update, (Pair) old)));
    writer.write(update("key", armed, now), (succ, exc) -> {});
    assertEquals("key", updated.get().getFirst().getKey());
    assertNull(updated.get().getSecond());
    assertTrue(view.get("key", armed, now).isPresent());
    assertFalse(view.get("key", armed, now - 1).isPresent());
    writer.write(delete("key", armed, now + 1), (succ, exc) -> {});
    assertEquals("key", updated.get().getFirst().getKey());
    assertEquals(3, updated.get().getSecond().getSecond().length);
    assertEquals(now, (long) updated.get().getSecond().getFirst());
    assertFalse(view.get("key", armed, now + 1).isPresent());
    assertTrue(view.get("key", armed, now).isPresent());
  }

  @Test
  public void testWriteSimpleWithCallbackCalledOnce() throws InterruptedException {
    AtomicInteger updated = new AtomicInteger();
    view.assign(singlePartition(), (update, old) -> updated.incrementAndGet());
    CountDownLatch latch = new CountDownLatch(1);
    view.write(update("key", armed, now), (succ, exc) -> latch.countDown());
    latch.await();
    assertEquals(1, updated.get());
  }

  @Test
  public void testWriteOnCacheError() {
    AtomicInteger errors = new AtomicInteger();
    view.assign(
        singlePartition(),
        (elem, old) -> {
          if (errors.incrementAndGet() < 2) {
            throw new IllegalStateException("Fail");
          }
        });
    writer.write(update("key", armed, now), (succ, exc) -> {});
    assertTrue(view.get("key", armed, now).isPresent());
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteConfirmError() {
    view.assign(singlePartition());
    writer.write(
        update("key", armed, now),
        (succ, exc) -> {
          throw new IllegalStateException("Fail");
        });
    assertFalse(view.get("key", armed, now).isPresent());
  }

  @Test
  public void testWriteWildcard() {
    view.assign(singlePartition());
    writer.write(update("key", "device.1", device, now), (succ, exc) -> {});
    writer.write(update("key", "device.2", device, now + 1), (succ, exc) -> {});
    assertFalse(view.get("key", armed).isPresent());
    assertTrue(view.get("key", "device.1", device, now).isPresent());
    assertFalse(view.get("key", "device.2", device, now).isPresent());
    assertTrue(view.get("key", "device.2", device, now + 1).isPresent());
    writer.write(deleteWildcard("key", device, now + 2), (succ, exc) -> {});
    assertTrue(view.get("key", "device.1", device, now).isPresent());
    assertFalse(view.get("key", "device.1", device, now + 2).isPresent());
    assertFalse(view.get("key", "device.2", device, now).isPresent());
    assertTrue(view.get("key", "device.2", device, now + 1).isPresent());
    assertFalse(view.get("key", "device.2", device, now + 2).isPresent());
  }

  @Test
  public void testScanWildcardWithDelete() {
    view.assign(singlePartition());
    writer.write(update("key", "device.1", device, now), (succ, exc) -> {});
    writer.write(update("key", "device.2", device, now + 1), (succ, exc) -> {});
    List<KeyValue<?>> kvs = new ArrayList<>();
    view.scanWildcard("key", device, now, kvs::add);
    assertEquals(1, kvs.size());
    kvs.clear();
    view.scanWildcard("key", device, now + 1, kvs::add);
    assertEquals(2, kvs.size());
    writer.write(delete("key", "device.2", device, now + 2), (succ, exc) -> {});

    assertTrue(view.get("key", "device.1", device, now).isPresent());
    assertFalse(view.get("key", "device.2", device, now).isPresent());
    assertTrue(view.get("key", "device.2", device, now + 1).isPresent());
    assertFalse(view.get("key", "device.2", device, now + 2).isPresent());
    writer.write(deleteWildcard("key", device, now + 2), (succ, exc) -> {});
    assertTrue(view.get("key", "device.1", device, now).isPresent());
    assertFalse(view.get("key", "device.1", device, now + 2).isPresent());
    assertFalse(view.get("key", "device.2", device, now).isPresent());
    assertTrue(view.get("key", "device.2", device, now + 1).isPresent());
    assertFalse(view.get("key", "device.2", device, now + 2).isPresent());

    kvs.clear();
    view.scanWildcard("key", device, now + 2, kvs::add);
    assertTrue(kvs.isEmpty());

    writer.write(update("key", "device.2", device, now + 3), (succ, exc) -> {});
    kvs.clear();
    view.scanWildcard("key", device, now + 3, kvs::add);
    assertEquals(1, kvs.size());
  }

  @Test
  public void testScanWildcardWithLimit() {
    view.assign(singlePartition());
    writer.write(update("key", "device.1", device, now), (succ, exc) -> {});
    writer.write(update("key", "device.2", device, now + 1), (succ, exc) -> {});
    List<KeyValue<?>> kvs = new ArrayList<>();
    view.scanWildcard("key", device, now, kvs::add);
    assertEquals(1, kvs.size());
    kvs.clear();
    view.scanWildcard(
        "key",
        device,
        view.fetchOffset(Listing.ATTRIBUTE, device.toAttributePrefix()),
        now + 1,
        1,
        kvs::add);
    assertEquals(1, kvs.size());
    view.scanWildcard("key", device, kvs.get(0).getOffset(), now + 1, 1, kvs::add);
    assertEquals(2, kvs.size());
    assertEquals("device.1", kvs.get(0).getAttribute());
    assertEquals("device.2", kvs.get(1).getAttribute());
  }

  @Test
  public void testScanWildcardAllWithDelete() {
    view.assign(singlePartition());
    writer.write(update("key", "armed", armed, now), (succ, exc) -> {});
    writer.write(update("key", "device.1", device, now), (succ, exc) -> {});
    writer.write(update("key", "device.2", device, now + 1), (succ, exc) -> {});
    List<KeyValue<?>> kvs = new ArrayList<>();
    view.scanWildcardAll("key", now, kvs::add);
    assertEquals(2, kvs.size());
    kvs.clear();
    view.scanWildcardAll("key", now + 1, kvs::add);
    assertEquals(3, kvs.size());
    kvs.clear();
    writer.write(deleteWildcard("key", device, now + 2), (succ, exc) -> {});

    kvs.clear();
    view.scanWildcardAll("key", now + 2, kvs::add);
    assertEquals(1, kvs.size());

    writer.write(update("key", "device.2", device, now + 3), (succ, exc) -> {});
    kvs.clear();
    view.scanWildcardAll("key", now + 3, kvs::add);
    assertEquals(2, kvs.size());
  }

  @Test
  public void testGetWithWildcardDelete() {
    view.assign(singlePartition());
    writer.write(update("key", "device.1", device, now - 1000), (succ, exc) -> {});
    writer.write(update("key", "device.2", device, now - 500), (succ, exc) -> {});
    writer.write(deleteWildcard("key", device, now), (succ, exc) -> {});
    writer.write(update("key", "device.1", device, now + 500), (succ, exc) -> {});
    writer.write(update("key", "device.3", device, now - 500), (succ, exc) -> {});
    assertTrue(view.get("key", "device.1", device, now + 500).isPresent());
    assertFalse(view.get("key", "device.2", device, now + 500).isPresent());
    assertFalse(view.get("key", "device.3", device, now + 500).isPresent());
  }

  @Test
  public void testGetWithDeleteAfterReinit() {
    writer.write(update("key", "device.1", device, now - 1000), (succ, exc) -> {});
    writer.write(update("key", "device.2", device, now - 500), (succ, exc) -> {});
    writer.write(delete("key", "device.1", device, now + 500), (succ, exc) -> {});
    writer.write(update("key", "device.1", device, now), (succ, exc) -> {});
    writer.write(update("key", "device.3", device, now - 500), (succ, exc) -> {});
    view.assign(singlePartition());
    assertFalse(view.get("key", "device.1", device, now + 500).isPresent());
    assertTrue(view.get("key", "device.2", device, now + 500).isPresent());
    assertTrue(view.get("key", "device.3", device, now + 500).isPresent());

    Set<KeyValue<?>> elements = new HashSet<>();
    view.scanWildcard("key", device, now + 1000, elements::add);
    assertEquals(2, elements.size());
  }

  @Test
  public void testGetWithWildcardDeleteAfterReinit() {
    writer.write(update("key", "device.1", device, now - 1000), (succ, exc) -> {});
    writer.write(update("key", "device.2", device, now - 500), (succ, exc) -> {});
    writer.write(deleteWildcard("key", device, now), (succ, exc) -> {});
    writer.write(update("key", "device.1", device, now + 500), (succ, exc) -> {});
    writer.write(update("key", "device.3", device, now - 500), (succ, exc) -> {});
    view.assign(singlePartition());
    assertTrue(view.get("key", "device.1", device, now + 500).isPresent());
    assertFalse(view.get("key", "device.2", device, now + 500).isPresent());
    assertFalse(view.get("key", "device.3", device, now + 500).isPresent());

    List<KeyValue<?>> elements = new ArrayList<>();
    view.scanWildcard("key", device, now + 1000, elements::add);
    assertEquals(1, elements.size());
  }

  @Test
  public void testListEntities() {
    writer.write(update("key1", "device.1", device, now - 1000), (succ, exc) -> {});
    writer.write(update("key2", "device.2", device, now - 500), (succ, exc) -> {});
    writer.write(deleteWildcard("key1", device, now), (succ, exc) -> {});
    writer.write(update("key3", "device.1", device, now + 500), (succ, exc) -> {});
    writer.write(update("key4", "device.3", device, now - 500), (succ, exc) -> {});
    view.assign(singlePartition());
    List<Pair<RandomOffset, String>> elements = new ArrayList<>();
    // List the first entity.
    view.listEntities(view.fetchOffset(Listing.ENTITY, ""), 1, elements::add);
    assertEquals(1, elements.size());
    // We should start indexing from zero.
    assertEquals(new LocalCachedPartitionedView.IntOffset(1), elements.get(0).getFirst());
    // List the second entity.
    view.listEntities(
        view.fetchOffset(Listing.ENTITY, elements.get(0).getSecond()), 1, elements::add);
    assertEquals(2, elements.size());
    // List remaining entities.
    view.listEntities(
        view.fetchOffset(Listing.ENTITY, elements.get(1).getSecond()), -1, elements::add);
    assertEquals(
        Sets.newHashSet("key1", "key2", "key3", "key4"),
        elements.stream().map(Pair::getSecond).collect(Collectors.toSet()));
    // List all entities.
    elements.clear();
    view.listEntities(elements::add);
    assertEquals(
        Sets.newHashSet("key1", "key2", "key3", "key4"),
        elements.stream().map(Pair::getSecond).collect(Collectors.toSet()));
  }

  @Test
  public void testOffsetInvariants() {
    writer.write(update("key1", "device.1", device, now - 1000), (succ, exc) -> {});
    view.assign(singlePartition());
    RandomOffset firstOffset = view.fetchOffset(Listing.ENTITY, "");
    AtomicReference<RandomOffset> next = new AtomicReference<>();
    view.listEntities(firstOffset, 1, p -> next.set(p.getFirst()));
    assertNotEquals(firstOffset, next.get());
  }

  @Test
  public void testPreserveSequenceIdOnGet() {
    view.assign(singlePartition());
    writer.write(update("key", armed.getName(), armed, now, 100L), (succ, exc) -> {});
    assertTrue(view.get("key", armed, now).isPresent());
    assertFalse(view.get("key", armed, now - 1).isPresent());
    assertEquals(100L, view.get("key", armed, now).get().getSequentialId());
    writer.write(delete("key", armed, now + 1), (succ, exc) -> {});
    assertFalse(view.get("key", armed, now + 1).isPresent());
    assertTrue(view.get("key", armed, now).isPresent());
  }

  @Test
  public void testPreserveStampOnWrite() {
    view.assign(singlePartition());
    writer.write(update("key", armed.getName(), armed, now, 0L, new byte[] {1}), (succ, exc) -> {});
    writer.write(
        update("key", armed.getName(), armed, now - 1, 0L, new byte[] {2}), (succ, exc) -> {});
    assertTrue(view.get("key", armed, now).isPresent());
    assertTrue(view.get("key", armed, now - 1).isPresent());
    assertFalse(view.get("key", armed, now - 2).isPresent());
    assertEquals(2, view.get("key", armed, now - 1).get().getValue()[0]);
    assertEquals(1, view.get("key", armed, now).get().getValue()[0]);
  }

  @Test
  public void testGetWithTtl() {
    timeProvider = () -> now;
    view.assign(singlePartition(), Duration.ofMinutes(1));
    writer.write(update("key", armed.getName(), armed, now), (succ, exc) -> {});
    assertTrue(view.get("key", armed).isPresent());
    timeProvider = () -> now + 60001;
    writer.write(update("key2", armed.getName(), armed, now + 60000), (succ, exc) -> {});
    assertFalse(view.get("key", armed).isPresent());
  }

  private StreamElement deleteWildcard(String key, AttributeDescriptor<?> desc, long stamp) {

    return StreamElement.deleteWildcard(gateway, desc, UUID.randomUUID().toString(), key, stamp);
  }

  private StreamElement delete(String key, AttributeDescriptor<?> desc, long stamp) {
    return delete(key, desc.getName(), desc, stamp);
  }

  private StreamElement delete(
      String key, String attribute, AttributeDescriptor<?> desc, long stamp) {

    return StreamElement.delete(gateway, desc, UUID.randomUUID().toString(), key, attribute, stamp);
  }

  private StreamElement update(String key, AttributeDescriptor<?> desc, long stamp) {
    return update(key, desc.getName(), desc, stamp);
  }

  private StreamElement update(
      String key, String attribute, AttributeDescriptor<?> desc, long stamp) {
    return update(key, attribute, desc, stamp, 0L);
  }

  private StreamElement update(
      String key, String attribute, AttributeDescriptor<?> desc, long stamp, long seqId) {
    return update(key, attribute, desc, stamp, seqId, new byte[] {1, 2, 3});
  }

  private StreamElement update(
      String key,
      String attribute,
      AttributeDescriptor<?> desc,
      long stamp,
      long seqId,
      byte[] value) {

    if (seqId > 0) {
      return StreamElement.upsert(gateway, desc, seqId, key, attribute, stamp, value);
    }
    return StreamElement.upsert(
        gateway, desc, UUID.randomUUID().toString(), key, attribute, stamp, value);
  }

  private Collection<Partition> singlePartition() {
    return Collections.singletonList(Partition.of(0));
  }
}
