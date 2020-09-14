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

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader.Listing;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.Test;

/** Test suite for {@link LocalCachedPartitionedView}. */
public class LocalCachedPartitionedViewTest {

  Repository repo = Repository.of(ConfigFactory.load("test-reference.conf").resolve());
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
  LocalCachedPartitionedView view = new LocalCachedPartitionedView(gateway, reader, writer);
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
  }

  @Test
  public void testWriteOnCacheError() {
    view.assign(
        singlePartition(),
        (elem, old) -> {
          throw new IllegalStateException("Fail");
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

    return StreamElement.upsert(
        gateway, desc, UUID.randomUUID().toString(), key, attribute, stamp, new byte[] {1, 2, 3});
  }

  private Collection<Partition> singlePartition() {
    return Arrays.asList(() -> 0);
  }
}
