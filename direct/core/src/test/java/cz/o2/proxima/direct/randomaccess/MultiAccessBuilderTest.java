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
package cz.o2.proxima.direct.randomaccess;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link MultiAccessBuilder}. */
public class MultiAccessBuilderTest {

  final Repository repo;
  final DirectDataOperator direct;

  RandomAccessReader reader;
  long now;

  public MultiAccessBuilderTest() {
    this.repo =
        ConfigRepository.Builder.ofTest(
                ConfigFactory.load()
                    .withFallback(ConfigFactory.load("test-reference.conf"))
                    .resolve())
            .build();
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);
  }

  @Before
  public void setUp() {
    now = System.currentTimeMillis();
  }

  @After
  public void tearDown() throws IOException {
    reader.close();
  }

  @Test
  public void testMultiAttributes() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<?> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed in gateway"));
    AttributeDescriptor<?> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute device.* in gateway"));
    RandomAccessReader base =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("gateway-storage-stream"))
            .findAny()
            .map(direct::resolveRequired)
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
            .orElseThrow(() -> new IllegalStateException("Cannot get random access reader"));
    reader = RandomAccessReader.newBuilder(repo, direct).addAttributes(base, armed, device).build();

    // write some data
    direct
        .getWriter(armed)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                armed,
                UUID.randomUUID().toString(),
                "gw",
                armed.getName(),
                now,
                new byte[] {1, 2}),
            (succ, exc) -> {});
    direct
        .getWriter(device)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "1",
                now,
                new byte[] {2, 3}),
            (succ, exc) -> {});
    Optional<? extends KeyValue<?>> kv = reader.get("gw", armed);
    assertTrue(kv.isPresent());
    assertArrayEquals(new byte[] {1, 2}, kv.get().getValue());
    kv = reader.get("gw", device.toAttributePrefix() + "1", device);
    assertTrue(kv.isPresent());
    assertArrayEquals(new byte[] {2, 3}, kv.get().getValue());
    kv = reader.get("gw", device.toAttributePrefix() + "2", device);
    assertFalse(kv.isPresent());
  }

  @Test
  public void testSingleFamily() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<?> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed in gateway"));
    AttributeDescriptor<?> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute device.* in gateway"));
    AttributeFamilyDescriptor family =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("gateway-storage-stream"))
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Cannot get random access reader"));
    reader = RandomAccessReader.newBuilder(repo, direct).addFamily(family).build();

    // write some data
    direct
        .getWriter(armed)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                armed,
                UUID.randomUUID().toString(),
                "gw",
                armed.getName(),
                now,
                new byte[] {1, 2}),
            (succ, exc) -> {});
    direct
        .getWriter(device)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "1",
                now,
                new byte[] {2, 3}),
            (succ, exc) -> {});
    Optional<? extends KeyValue<?>> kv = reader.get("gw", armed);
    assertTrue(kv.isPresent());
    assertArrayEquals(new byte[] {1, 2}, kv.get().getValue());
    kv = reader.get("gw", device.toAttributePrefix() + "1", device);
    assertTrue(kv.isPresent());
    assertArrayEquals(new byte[] {2, 3}, kv.get().getValue());
    kv = reader.get("gw", device.toAttributePrefix() + "2", device);
    assertFalse(kv.isPresent());
  }

  @Test
  public void testMultipleFamiliesAndEntities() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    EntityDescriptor dummy =
        repo.findEntity("dummy")
            .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));
    AttributeDescriptor<?> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed in gateway"));
    AttributeDescriptor<?> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute device.* in gateway"));
    AttributeDescriptor<Object> data =
        dummy
            .findAttribute("data")
            .orElseThrow(() -> new IllegalStateException("Missing attribute data in dummy"));
    AttributeFamilyDescriptor family =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("gateway-storage-stream"))
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Cannot get random access reader"));
    AttributeFamilyDescriptor dummyFamily =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("dummy-storage"))
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Cannot get random reader"));
    reader =
        RandomAccessReader.newBuilder(repo, direct)
            .addFamily(family)
            .addFamily(dummyFamily)
            .build();

    // write some data
    direct
        .getWriter(armed)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                armed,
                UUID.randomUUID().toString(),
                "gw",
                armed.getName(),
                now,
                new byte[] {1, 2}),
            (succ, exc) -> {});
    direct
        .getWriter(device)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "1",
                now,
                new byte[] {2, 3}),
            (succ, exc) -> {});
    direct
        .getWriter(data)
        .get()
        .write(
            StreamElement.upsert(
                dummy,
                data,
                UUID.randomUUID().toString(),
                "dummy",
                data.getName(),
                now,
                new byte[] {3, 4}),
            (succ, exc) -> {});
    Optional<? extends KeyValue<?>> kv = reader.get("gw", armed);
    assertTrue(kv.isPresent());
    assertArrayEquals(new byte[] {1, 2}, kv.get().getValue());
    kv = reader.get("gw", device.toAttributePrefix() + "1", device);
    assertTrue(kv.isPresent());
    assertArrayEquals(new byte[] {2, 3}, kv.get().getValue());
    kv = reader.get("gw", device.toAttributePrefix() + "2", device);
    assertFalse(kv.isPresent());
    kv = reader.get("dummy", data);
    assertTrue(kv.isPresent());
    assertArrayEquals(new byte[] {3, 4}, kv.get().getValue());
  }

  @Test
  public void testSingleFamilyScanWildcard() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<?> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute device.* in gateway"));
    AttributeFamilyDescriptor family =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("gateway-storage-stream"))
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Cannot get random access reader"));
    reader = RandomAccessReader.newBuilder(repo, direct).addFamily(family).build();

    // write some data
    direct
        .getWriter(device)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "1",
                now,
                new byte[] {2, 3}),
            (succ, exc) -> {});
    direct
        .getWriter(device)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "2",
                now,
                new byte[] {2, 3}),
            (succ, exc) -> {});
    List<KeyValue<?>> kvs = new ArrayList<>();
    reader.scanWildcard("gw", device, kvs::add);
    assertEquals(2, kvs.size());
    assertEquals(device.toAttributePrefix() + "1", kvs.get(0).getAttribute());
    assertEquals(device.toAttributePrefix() + "2", kvs.get(1).getAttribute());
  }

  @Test
  public void testMultiAttributesScanAll() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<?> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed in gateway"));
    AttributeDescriptor<?> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute device.* in gateway"));
    RandomAccessReader base =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("gateway-storage-stream"))
            .findAny()
            .map(direct::resolveRequired)
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
            .orElseThrow(() -> new IllegalStateException("Cannot get random access reader"));
    reader = RandomAccessReader.newBuilder(repo, direct).addAttributes(base, armed, device).build();

    // write some data
    direct
        .getWriter(armed)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                armed,
                UUID.randomUUID().toString(),
                "gw",
                armed.getName(),
                now,
                new byte[] {1, 2}),
            (succ, exc) -> {});
    direct
        .getWriter(device)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "1",
                now,
                new byte[] {2, 3}),
            (succ, exc) -> {});
    Set<KeyValue<?>> kvs =
        new TreeSet<>((k1, k2) -> k1.getAttribute().compareTo(k2.getAttribute()));
    reader.scanWildcardAll("gw", kvs::add);
    assertEquals(2, kvs.size());
    List<KeyValue<?>> ordered = new ArrayList<>(kvs);
    assertEquals(armed.getName(), ordered.get(0).getAttribute());
    assertEquals(device.toAttributePrefix() + "1", ordered.get(1).getAttribute());
  }

  @Test
  public void testMultiAttributesScanAllOffset() {
    EntityDescriptor gateway =
        repo.findEntity("gateway")
            .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    AttributeDescriptor<?> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing attribute armed in gateway"));
    AttributeDescriptor<?> device =
        gateway
            .findAttribute("device.*")
            .orElseThrow(() -> new IllegalStateException("Missing attribute device.* in gateway"));
    RandomAccessReader base =
        repo.getAllFamilies()
            .filter(af -> af.getName().equals("gateway-storage-stream"))
            .findAny()
            .map(direct::resolveRequired)
            .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
            .orElseThrow(() -> new IllegalStateException("Cannot get random access reader"));
    reader = RandomAccessReader.newBuilder(repo, direct).addAttributes(base, armed, device).build();

    // write some data
    direct
        .getWriter(armed)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                armed,
                UUID.randomUUID().toString(),
                "gw",
                armed.getName(),
                now,
                new byte[] {1, 2}),
            (succ, exc) -> {});
    direct
        .getWriter(device)
        .get()
        .write(
            StreamElement.upsert(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "1",
                now,
                new byte[] {2, 3}),
            (succ, exc) -> {});

    RandomOffset off = reader.fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, armed.getName());
    List<KeyValue<?>> kvs = new ArrayList<>();
    reader.scanWildcardAll("gw", off, -1, kvs::add);
    assertEquals(1, kvs.size());
    List<KeyValue<?>> ordered = new ArrayList<>(kvs);
    assertEquals(device.toAttributePrefix() + "1", ordered.get(0).getAttribute());
  }
}
