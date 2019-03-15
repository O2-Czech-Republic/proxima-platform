/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.gcloud.storage;

import com.google.cloud.storage.Blob;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import com.google.common.collect.Iterables;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;

import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.mockito.Mockito.*;

/**
 * Test suite for {@link BulkGCloudStorageWriter}.
 */
@RunWith(Parameterized.class)
public class BulkGCloudStorageWriterTest {
  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  final Repository repo = Repository.of(ConfigFactory.load().resolve());
  final DirectDataOperator direct = repo.asDataOperator(DirectDataOperator.class);
  final AttributeDescriptor<?> attr;
  final AttributeDescriptor<?> wildcard;
  final EntityDescriptor entity;

  @Parameterized.Parameters
  public static Collection<Boolean> parameters() {
    return Arrays.asList(true, false);
  }

  @Parameterized.Parameter
  public boolean gzip;

  BulkGCloudStorageWriter writer;
  NavigableSet<String> blobs;
  Map<Long, List<StreamElement>> written;
  AtomicReference<CountDownLatch> latch = new AtomicReference<>();
  AtomicReference<Throwable> onFlushToBlob = new AtomicReference<>();

  public BulkGCloudStorageWriterTest() throws URISyntaxException {
    this.wildcard = AttributeDescriptor.newBuilder(repo)
        .setEntity("dummy")
        .setSchemeUri(new URI("bytes:///"))
        .setName("wildcard.*")
        .build();
    this.attr = AttributeDescriptor.newBuilder(repo)
        .setEntity("dummy")
        .setSchemeUri(new URI("bytes:///"))
        .setName("attr")
        .build();
    this.entity = EntityDescriptor.newBuilder()
        .setName("dummy")
        .addAttribute((AttributeDescriptorBase<?>) attr)
        .addAttribute((AttributeDescriptorBase<?>) wildcard)
        .build();
  }

  @Before
  public void setUp() throws URISyntaxException, IOException {
    blobs = Collections.synchronizedNavigableSet(new TreeSet<>());
    onFlushToBlob.set(null);
    written = Collections.synchronizedMap(new HashMap<>());
    writer = new BulkGCloudStorageWriter(
        entity, new URI("gcloud-storage://project:bucket/path"),
        cfg(), direct.getContext()) {

      @Override
      Blob createBlob(String name) {
        blobs.add(name);
        return mock(Blob.class);
      }

      @Override
      String uuid() {
        return "uuid";
      }

      @Override
      void flushToBlob(long bucketEndStamp, File file, Blob blob) throws IOException {
        try {
          if (onFlushToBlob.get() != null) {
            throw new RuntimeException(onFlushToBlob.get());
          }
          List<StreamElement> blobWritten = new ArrayList<>();
          written.put(bucketEndStamp, blobWritten);
          try (BinaryBlob.Reader reader = new BinaryBlob(file).reader(entity)) {
            reader.iterator().forEachRemaining(blobWritten::add);
          }
        } finally {
          if (latch.get() != null) {
            latch.get().countDown();
          }
        }
      }

    };
  }

  @Test(timeout = 10000)
  public synchronized void testWrite() throws Exception {
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement first = StreamElement.update(entity, attr,
        UUID.randomUUID().toString(),
        "key", "attr", now, new byte[] { 1, 2 });
    StreamElement second = StreamElement.update(entity, wildcard,
        UUID.randomUUID().toString(),
        "key", "wildcard.1", now + 200, new byte[] { 3 });
    write(first, (succ, exc) -> {
      // this one will not get committed
    });
    write(second, (succ, exc) -> {
      assertTrue("Exception " + exc, succ);
      assertNull(exc);
      latch.get().countDown();
    });
    writer.flush();
    latch.get().await();
    assertEquals(1, written.size());
    validate(written.get(1500000001000L), first, second);
    assertEquals(1, blobs.size());
    assertEquals(
        writer.toBlobName(now, now + 1000),
        Iterables.getOnlyElement(blobs));
    assertTrue(Iterables.getOnlyElement(blobs).startsWith("2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testWriteAutoFlush() throws Exception {
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement first = StreamElement.update(entity, attr,
        UUID.randomUUID().toString(),
        "key", "attr", now, new byte[] { 1, 2 });
    StreamElement second = StreamElement.update(entity, wildcard,
        UUID.randomUUID().toString(),
        "key", "wildcard.1", now + 2000, new byte[] { 3 });
    write(first, (succ, exc) -> {
      assertTrue("Exception " + exc, succ);
      assertNull(exc);
      latch.get().countDown();
    });
    write(second, (succ, exc) -> {
      assertTrue("Exception " + exc, succ);
      assertNull(exc);
    });
    latch.get().await();
    assertEquals(1, written.size());
    validate(written.get(1500000001000L), first);
    assertEquals(1, blobs.size());
    assertEquals(
        writer.toBlobName(now, now + 1000),
        Iterables.getOnlyElement(blobs));
    assertTrue(Iterables.getOnlyElement(blobs).startsWith("2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testWriteOutOfOrder() throws Exception {
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement[] elements = {
      StreamElement.update(entity, attr,
          UUID.randomUUID().toString(),
          "key", "attr", now + 200, new byte[] { 1 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now, new byte[] { 1, 2 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now + 999, new byte[] { 1, 2, 3 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now + 500, new byte[] { 1, 2, 3, 4 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now + 2000, new byte[] { 1, 2, 3, 4, 5 })
    };
    Arrays.stream(elements).forEach(e -> write(e, (succ, exc) -> {
      assertTrue("Exception " + exc, succ);
      assertNull(exc);
      assertEquals(now + 500, e.getStamp());
      latch.get().countDown();
    }));
    latch.get().await();
    assertEquals(1, written.size());
    validate(
        written.get(1500000001000L),
        elements[0], elements[1], elements[2], elements[3]);
    assertEquals(1, blobs.size());
    assertEquals(
        writer.toBlobName(now, now + 1000),
        Iterables.getOnlyElement(blobs));
    assertTrue(Iterables.getOnlyElement(blobs).startsWith("2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testFlushingOnOutOfOrder() throws Exception {
    latch.set(new CountDownLatch(3));
    long now = 1500000000000L;
    StreamElement[] elements = {
      StreamElement.update(entity, attr,
          UUID.randomUUID().toString(),
          "key", "attr", now + 200, new byte[] { 1 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now, new byte[] { 1, 2 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now + 1000, new byte[] { 1, 2, 3 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now + 500, new byte[] { 1, 2, 3, 4 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now + 3000, new byte[] { 1, 2, 3, 4, 5 })
    };
    List<Long> watermarks = new ArrayList<>(
        Arrays.asList(now, now, now + 500, now + 500, now + 2500));
    Arrays.stream(elements).forEach(e -> write(e, watermarks.remove(0), (succ, exc) -> {
      assertTrue("Exception " + exc, succ);
      assertNull(exc);
      assertEquals(now + 500, e.getStamp());
      latch.get().countDown();
    }));
    latch.get().await();
    assertEquals(2, written.size());
    validate(written.get(1500000001000L), elements[0], elements[1], elements[3]);
    validate(written.get(1500000002000L), elements[2]);
    assertEquals(2, blobs.size());
    assertEquals(
        writer.toBlobName(now, now + 1000),
        Iterables.get(blobs, 0));
    assertTrue(Iterables.get(blobs, 0).startsWith("2017/07/"));
    assertEquals(
        writer.toBlobName(now + 1000, now + 2000),
        Iterables.get(blobs, 1));
    assertTrue(Iterables.get(blobs, 1).startsWith("2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testFailWrite() throws Exception {
    onFlushToBlob.set(new RuntimeException("Fail"));
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement[] elements = {
      StreamElement.update(entity, attr,
          UUID.randomUUID().toString(),
          "key", "attr", now + 200, new byte[] { 1 }),
      StreamElement.update(entity, wildcard,
          UUID.randomUUID().toString(),
          "key", "wildcard.1", now, new byte[] { 1, 2 }),
    };
    Arrays.stream(elements).forEach(e -> write(e, (succ, exc) -> {
      assertFalse(succ);
      assertNotNull(exc);
      latch.get().countDown();
    }));
    writer.flush();
    latch.get().await();
    assertTrue(written.isEmpty());
  }

  private void validate(List<StreamElement> written, StreamElement... elements)
      throws IOException {

    Iterator<StreamElement> iterator = written.iterator();
    assertEquals(
        "Expected " + Arrays.toString(elements) + " got " + written,
        elements.length, written.size());
    for (StreamElement el : elements) {
      StreamElement next = iterator.next();
      assertEquals(next, el);
    }
  }

  private Map<String, Object> cfg() throws IOException {
    Map<String, Object> ret = new HashMap<>();
    ret.put("log-roll-interval", "1000");
    ret.put("allowed-lateness-ms", 500);
    ret.put("flush-delay-ms", 50);
    ret.put("tmp.dir", tempFolder.newFolder().getAbsolutePath());
    if (gzip) {
      ret.put("gzip", "true");
    }
    return ret;
  }

  private void write(StreamElement elem, CommitCallback commit) {
    write(elem, elem.getStamp(), commit);
  }

  private void write(StreamElement elem, long watermark, CommitCallback commit) {
    writer.write(elem, watermark, commit);
  }



}
