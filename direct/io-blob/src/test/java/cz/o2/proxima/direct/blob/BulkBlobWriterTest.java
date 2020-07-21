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
package cz.o2.proxima.direct.blob;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.blob.TestBlobStorageAccessor.BlobWriter;
import cz.o2.proxima.direct.bulk.DefaultNamingConvention;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.core.BulkAttributeWriter.Factory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test suite for {@link BulkBlobWriter}. */
@RunWith(Parameterized.class)
public class BulkBlobWriterTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Rule public final transient TemporaryFolder tempFolder = new TemporaryFolder();

  final Repository repo = Repository.of(ConfigFactory.load().resolve());
  final AttributeDescriptor<?> attr;
  final AttributeDescriptor<?> wildcard;
  final EntityDescriptor entity;
  final transient DirectDataOperator direct;

  @Parameterized.Parameters
  public static Collection<Boolean> parameters() {
    return Arrays.asList(true, false);
  }

  @Parameterized.Parameter public boolean gzip;

  TestBlobStorageAccessor accessor;
  BlobWriter writer;
  AtomicReference<CountDownLatch> latch = new AtomicReference<>();
  AtomicReference<TestBlobStorageAccessor.Runnable> onFlushToBlob = new AtomicReference<>();

  public BulkBlobWriterTest() {
    this.wildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(URI.create("bytes:///"))
            .setName("wildcard.*")
            .build();
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(URI.create("bytes:///"))
            .setName("attr")
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("dummy")
            .addAttribute(attr)
            .addAttribute(wildcard)
            .build();
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
  }

  @Before
  public void setUp() throws IOException {
    onFlushToBlob.set(null);
    initWriter(cfg());
  }

  @After
  public void tearDown() {
    Optional.ofNullable(writer).ifPresent(BulkBlobWriter::close);
  }

  void initWriter(Map<String, Object> cfg) {
    accessor =
        new TestBlobStorageAccessor(
            entity,
            URI.create("blob-test://bucket/path"),
            cfg,
            () -> Optional.ofNullable(latch.get()).ifPresent(CountDownLatch::countDown),
            onFlushToBlob) {
          @Override
          public NamingConvention getNamingConvention() {
            return new DefaultNamingConvention(
                Duration.ofMillis(getRollPeriod()), "prefix", "suffix", () -> "uuid");
          }
        };
    writer = accessor.new BlobWriter(direct.getContext());
  }

  @Test(timeout = 10000)
  public synchronized void testWrite() throws Exception {
    testWriteWithWriter();
  }

  @Test(timeout = 10000)
  public synchronized void testWriteToJson() throws Exception {
    initWriter(cfg("json"));
    testWriteWithWriter();
  }

  void testWriteWithWriter() throws InterruptedException, IOException {
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement first =
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "key", "attr", now, new byte[] {1, 2});
    StreamElement second =
        StreamElement.upsert(
            entity,
            wildcard,
            UUID.randomUUID().toString(),
            "key",
            "wildcard.1",
            now + 200,
            new byte[] {3});
    write(
        first,
        (succ, exc) -> {
          // this one will not get committed
        });
    write(
        second,
        (succ, exc) -> {
          assertTrue("Exception " + exc, succ);
          assertNull(exc);
          latch.get().countDown();
        });
    writer.flush();
    latch.get().await();
    assertEquals(1, accessor.getWrittenBlobs());
    validate(accessor.getWrittenFor(now + 999L), first, second);
  }

  @Test(timeout = 10000)
  public synchronized void testWriteAutoFlush() throws Exception {
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement first =
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "key", "attr", now, new byte[] {1, 2});
    StreamElement second =
        StreamElement.upsert(
            entity,
            wildcard,
            UUID.randomUUID().toString(),
            "key",
            "wildcard.1",
            now + 2000,
            new byte[] {3});
    write(
        first,
        (succ, exc) -> {
          assertTrue("Exception " + exc, succ);
          assertNull(exc);
          latch.get().countDown();
        });
    write(
        second,
        (succ, exc) -> {
          assertTrue("Exception " + exc, succ);
          assertNull(exc);
        });
    latch.get().await();
    assertEquals(1, accessor.getWrittenBlobs());
    validate(accessor.getWrittenFor(now + 999L), first);
  }

  @Test(timeout = 10000)
  public synchronized void testWriteOutOfOrder() throws Exception {
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement[] elements = {
      StreamElement.upsert(
          entity, attr, UUID.randomUUID().toString(), "key", "attr", now + 200, new byte[] {1}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now,
          new byte[] {1, 2}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 999,
          new byte[] {1, 2, 3}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 500,
          new byte[] {1, 2, 3, 4}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 2000,
          new byte[] {1, 2, 3, 4, 5})
    };
    Arrays.stream(elements)
        .forEach(
            e ->
                write(
                    e,
                    (succ, exc) -> {
                      assertTrue("Exception " + exc, succ);
                      assertNull(exc);
                      assertEquals(now + 500, e.getStamp());
                      latch.get().countDown();
                    }));
    latch.get().await();
    assertEquals(1, accessor.getWrittenBlobs());
    validate(
        accessor.getWrittenFor(now + 999L), elements[0], elements[1], elements[2], elements[3]);
  }

  @Test(timeout = 10000)
  public synchronized void testFlushingOnOutOfOrder() throws Exception {
    latch.set(new CountDownLatch(3));
    long now = 1500000000000L;
    StreamElement[] elements = {
      StreamElement.upsert(
          entity, attr, UUID.randomUUID().toString(), "key", "attr", now + 200, new byte[] {1}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now,
          new byte[] {1, 2}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 1000,
          new byte[] {1, 2, 3}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 500,
          new byte[] {1, 2, 3, 4}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 3000,
          new byte[] {1, 2, 3, 4, 5})
    };
    List<Long> watermarks =
        new ArrayList<>(Arrays.asList(now, now, now + 500, now + 500, now + 2500));
    Arrays.stream(elements)
        .forEach(
            e ->
                write(
                    e,
                    watermarks.remove(0),
                    (succ, exc) -> {
                      assertTrue("Exception " + exc, succ);
                      assertNull(exc);
                      assertEquals(now + 500, e.getStamp());
                      latch.get().countDown();
                    }));
    latch.get().await();
    assertEquals(2, accessor.getWrittenBlobs());
    validate(accessor.getWrittenFor(now + 999L), elements[0], elements[1], elements[3]);
    validate(accessor.getWrittenFor(now + 1999L), elements[2]);
  }

  @Test(timeout = 10000)
  public synchronized void testFailWrite() throws Exception {
    onFlushToBlob.set(
        () -> {
          throw new RuntimeException("Fail");
        });
    latch.set(new CountDownLatch(1));
    long now = 1500000000000L;
    StreamElement[] elements = {
      StreamElement.upsert(
          entity, attr, UUID.randomUUID().toString(), "key", "attr", now + 200, new byte[] {1}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now,
          new byte[] {1, 2}),
    };
    Arrays.stream(elements)
        .forEach(
            e ->
                write(
                    e,
                    (succ, exc) -> {
                      assertFalse(succ);
                      assertNotNull(exc);
                      latch.get().countDown();
                    }));
    writer.flush();
    latch.get().await();
    assertEquals(0, accessor.getWrittenBlobs());
  }

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    accessor =
        new TestBlobStorageAccessor(
            entity,
            URI.create("blob-test://bucket/path"),
            Collections.emptyMap(),
            () -> {},
            new AtomicReference<>());
    BlobWriter writer = accessor.new BlobWriter(direct.getContext());
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(
        writer.getAccessor().getUri(), ((BlobWriter) factory.apply(repo)).getAccessor().getUri());
  }

  private void validate(List<StreamElement> written, StreamElement... elements) {

    Iterator<StreamElement> iterator = written.iterator();
    assertEquals(
        "Expected " + Arrays.toString(elements) + " got " + written,
        elements.length,
        written.size());
    for (StreamElement el : elements) {
      StreamElement next = iterator.next();
      assertEquals(next, el);
    }
  }

  private Map<String, Object> cfg() throws IOException {
    return cfg(null);
  }

  private Map<String, Object> cfg(@Nullable String format) throws IOException {
    Map<String, Object> ret = new HashMap<>();
    ret.put("log-roll-interval", "1000");
    ret.put("allowed-lateness-ms", 500);
    ret.put("tmp.dir", tempFolder.newFolder().getAbsolutePath());
    if (gzip) {
      ret.put("gzip", "true");
    }
    if (format != null) {
      ret.put("format", format);
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
