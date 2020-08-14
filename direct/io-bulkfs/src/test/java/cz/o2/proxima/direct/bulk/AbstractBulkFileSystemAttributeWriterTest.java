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
package cz.o2.proxima.direct.bulk;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test {@link java.util.AbstractCollection}. */
@RunWith(Parameterized.class)
@Slf4j
public class AbstractBulkFileSystemAttributeWriterTest implements Serializable {

  public static class InstantiableJsonFormat extends JsonFormat {

    private static final long serialVersionUID = 1L;

    public InstantiableJsonFormat() {
      super(false);
    }
  }

  @Value
  private static class Params {

    long rollPeriod;
    String format;
    boolean gzip;
    long allowedLateness;

    FileSystem getFs(TemporaryFolder tempFolder, String dir) throws IOException {
      return FileSystem.local(new File(tempFolder.newFolder(), dir), getNamingConvention());
    }

    FileFormat getFileFormat() {
      return FileFormatUtils.getFileFormatFromName(format, gzip);
    }

    NamingConvention getNamingConvention() {
      return NamingConvention.defaultConvention(
          Duration.ofMillis(rollPeriod), "prefix", getFileFormat().fileSuffix());
    }
  }

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  final URI uri = URI.create("abstract-bulk:///");
  final Repository repo = Repository.ofTest(ConfigFactory.load().resolve());
  final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  final AttributeDescriptor<?> attr;
  final AttributeDescriptor<?> wildcard;
  final EntityDescriptor entity;

  @Parameterized.Parameters
  public static Collection<Params> parameters() {
    return Arrays.asList(
        new Params(1000, "binary", false, 0),
        new Params(1000, "binary", false, 500),
        new Params(1000, "binary", true, 0),
        new Params(1000, "binary", true, 500),
        new Params(1000, "json", false, 0),
        new Params(1000, "json", false, 500),
        new Params(1000, "json", true, 0),
        new Params(1000, "json", true, 500),
        new Params(2000, "binary", false, 0),
        new Params(2000, "binary", false, 500),
        new Params(2000, "binary", true, 0),
        new Params(2000, "binary", true, 500),
        new Params(2000, "json", false, 0),
        new Params(2000, "json", false, 500),
        new Params(2000, "json", true, 0),
        new Params(2000, "json", true, 500),
        new Params(1000, InstantiableJsonFormat.class.getCanonicalName(), false, 0),
        new Params(1000, InstantiableJsonFormat.class.getCanonicalName(), false, 500),
        new Params(2000, InstantiableJsonFormat.class.getCanonicalName(), true, 0),
        new Params(2000, InstantiableJsonFormat.class.getCanonicalName(), true, 500));
  }

  @Parameterized.Parameter public Params params;

  AbstractBulkFileSystemAttributeWriter writer;
  Set<String> flushedPaths;
  Map<Long, List<StreamElement>> written;
  AtomicReference<Throwable> onFlush = new AtomicReference<>();

  public AbstractBulkFileSystemAttributeWriterTest() throws URISyntaxException {
    this.wildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(new URI("bytes:///"))
            .setName("wildcard.*")
            .build();
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(new URI("bytes:///"))
            .setName("attr")
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("dummy")
            .addAttribute(attr)
            .addAttribute(wildcard)
            .build();
  }

  @Before
  public void setUp() throws IOException {
    flushedPaths = Collections.synchronizedSet(new TreeSet<>());
    onFlush.set(null);
    written = Collections.synchronizedMap(new HashMap<>());
    writer = initWriter();
  }

  @After
  public void tearDown() {
    writer.close();
  }

  AbstractBulkFileSystemAttributeWriter initWriter() throws IOException {

    return new AbstractBulkFileSystemAttributeWriter(
        entity,
        uri,
        params.getFs(tempFolder, "/path/"),
        params.getNamingConvention(),
        params.getFileFormat(),
        direct.getContext(),
        params.getRollPeriod(),
        params.getAllowedLateness()) {

      @Override
      public BulkAttributeWriter.Factory<?> asFactory() {
        return repo -> ExceptionUtils.uncheckedFactory(() -> initWriter());
      }

      @Override
      protected void flush(Bulk v) {
        Throwable exc = onFlush.getAndSet(null);
        if (exc != null) {
          throw new RuntimeException(exc);
        }
        flushedPaths.add(v.getPath().toString());
        try {
          List<StreamElement> elements =
              Lists.newArrayList(params.getFileFormat().openReader(v.getPath(), entity));
          log.debug("Putting elements {} to stamp {}", elements, v.getMaxTs());
          written.put(v.getMaxTs(), elements);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  @Test(timeout = 10000)
  public synchronized void testWrite() throws Exception {
    AtomicInteger commits = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);
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
          fail("This should not have been committed!");
        });
    write(
        second,
        (succ, exc) -> {
          assertTrue("Exception " + exc, succ);
          assertNull(exc);
          commits.incrementAndGet();
          latch.countDown();
        });
    writer.updateWatermark(Long.MAX_VALUE);
    latch.await();
    assertEquals(1, commits.get());
    assertEquals(1, written.size());
    validate(written.get(1500000000000L + params.getRollPeriod()), first, second);
    assertEquals(1, flushedPaths.size());
    assertTrue(
        params
            .getNamingConvention()
            .isInRange(Iterables.getOnlyElement(flushedPaths), now, now + 1));
    assertTrue(Iterables.getOnlyElement(flushedPaths).contains("/path/2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testWriteAutoFlush() throws Exception {
    long now = 1500000000000L;
    CountDownLatch latch = new CountDownLatch(1);
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
            now + 2 * params.getRollPeriod(),
            new byte[] {3});
    write(
        first,
        (succ, exc) -> {
          assertTrue("Exception " + exc, succ);
          assertNull(exc);
          latch.countDown();
        });
    write(
        second,
        (succ, exc) -> {
          // not going to be committed
          fail("Should not be committed");
        });
    latch.await();
    assertEquals(1, written.size());
    validate(written.get(1500000000000L + params.getRollPeriod()), first);
    assertEquals(1, flushedPaths.size());
    assertTrue(
        params
            .getNamingConvention()
            .isInRange(Iterables.getOnlyElement(flushedPaths).toString(), now, now + 1));
    assertTrue(Iterables.getOnlyElement(flushedPaths).toString().contains("/path/2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testWriteOutOfOrder() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
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
          now + params.getRollPeriod() - 1,
          new byte[] {1, 2, 3}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + params.getRollPeriod() / 2,
          new byte[] {1, 2, 3, 4}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 2 * params.getRollPeriod(),
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
                      assertEquals(now + params.getRollPeriod() / 2, e.getStamp());
                      latch.countDown();
                    }));
    latch.await();
    assertEquals(1, written.size());
    validate(
        written.get(1500000000000L + params.getRollPeriod()),
        elements[0],
        elements[1],
        elements[2],
        elements[3]);
    assertEquals(1, flushedPaths.size());
    assertTrue(
        params
            .getNamingConvention()
            .isInRange(Iterables.getOnlyElement(flushedPaths).toString(), now, now + 1));
    assertTrue(Iterables.getOnlyElement(flushedPaths).toString().contains("/path/2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testFlushingOnOutOfOrder() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
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
          now + params.getRollPeriod(),
          new byte[] {1, 2, 3}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + params.getRollPeriod() / 2,
          new byte[] {1, 2, 3, 4}),
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key",
          "wildcard.1",
          now + 3 * params.getRollPeriod(),
          new byte[] {1, 2, 3, 4, 5})
    };
    List<Long> watermarks =
        new ArrayList<>(
            Arrays.asList(
                now,
                now,
                now + params.getRollPeriod() / 2,
                now + params.getRollPeriod() / 2,
                now + 3 * params.getRollPeriod() - params.getRollPeriod() / 2));

    Arrays.stream(elements)
        .forEach(
            e ->
                write(
                    e,
                    watermarks.remove(0),
                    (succ, exc) -> {
                      assertTrue("Exception " + exc, succ);
                      assertNull(exc);
                      assertEquals(now + params.getRollPeriod() / 2, e.getStamp());
                      latch.countDown();
                    }));
    latch.await();
    assertEquals(2, written.size());
    validate(written.get(now + params.getRollPeriod()), elements[0], elements[1], elements[3]);
    validate(written.get(now + 2 * params.getRollPeriod()), elements[2]);
    assertEquals("Expected two paths, got " + flushedPaths, 2, flushedPaths.size());
    assertTrue(
        "Invalid range for " + Iterables.get(flushedPaths, 0).toString(),
        params
            .getNamingConvention()
            .isInRange(Iterables.get(flushedPaths, 0).toString(), now, now + 1));
    assertTrue(Iterables.get(flushedPaths, 0).contains("/path/2017/07/"));
    assertTrue(
        params
            .getNamingConvention()
            .isInRange(
                Iterables.get(flushedPaths, 1).toString(),
                now + params.getRollPeriod(),
                now + params.getRollPeriod() + 1));
    assertTrue(Iterables.get(flushedPaths, 1).contains("/path/2017/07/"));
  }

  @Test(timeout = 10000)
  public synchronized void testFailWrite() throws InterruptedException {
    onFlush.set(new RuntimeException("Fail"));
    CountDownLatch latch = new CountDownLatch(1);
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
                      latch.countDown();
                    }));
    writer.updateWatermark(Long.MAX_VALUE);
    latch.await();
    assertTrue(written.isEmpty());
  }

  @Test(timeout = 10000)
  public synchronized void testWriteNoWatermark() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    long now = 1500000000000L;
    StreamElement el =
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "key", "attr", now, new byte[] {1, 2});
    write(
        el,
        (succ, exc) -> {
          latch.countDown();
        });
    writer.updateWatermark(Long.MIN_VALUE);
    assertFalse(latch.await(500, TimeUnit.MILLISECONDS));
  }

  @Test(timeout = 10000)
  public synchronized void testWriteLate() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    long now = 1500000000000L;
    StreamElement[] elements = {
      // not late
      StreamElement.upsert(
          entity, attr, UUID.randomUUID().toString(), "key1", "attr", now + 200, new byte[] {1}),
      // not late
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key2",
          "wildcard.1",
          now,
          new byte[] {1, 2}),
      // not late, flushes previous two
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key3",
          "wildcard.1",
          now + params.getRollPeriod() + 1,
          new byte[] {1, 2, 3}),
      // late, not flushed
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key4",
          "wildcard.1",
          now + params.getRollPeriod() / 2 + 1,
          new byte[] {1, 2, 3, 4}),
      // not late, flush
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key5",
          "wildcard.1",
          now + 3 * params.getRollPeriod(),
          new byte[] {1, 2, 3, 4, 5})
    };
    List<Long> watermarks =
        new ArrayList<>(
            Arrays.asList(
                now,
                now,
                now + 2 * params.getRollPeriod() + params.getAllowedLateness() + 1,
                now + 2 * params.getRollPeriod() + params.getAllowedLateness() + 1,
                now + 3 * params.getRollPeriod() + params.getAllowedLateness()));

    Set<Long> committed = Collections.synchronizedSet(new HashSet<>());
    Arrays.stream(elements)
        .forEach(
            e ->
                write(
                    e,
                    watermarks.remove(0),
                    (succ, exc) -> {
                      assertTrue("Exception " + exc, succ);
                      assertNull(exc);
                      committed.add(e.getStamp());
                      latch.countDown();
                    }));
    long watermark = now + 4 * params.getRollPeriod() + 1 + params.getAllowedLateness();
    while (true) {
      writer.bulk().updateWatermark(watermark);
      if (latch.await(50, TimeUnit.MILLISECONDS)) {
        break;
      }
    }
    assertEquals("Written: " + written.keySet(), 3, written.size());
    validate(written.get(now + params.getRollPeriod()), elements[0], elements[1]);
    validate(written.get(now + params.getRollPeriod() + 1), elements[2], elements[3]);
    validate(written.get(now + 4 * params.getRollPeriod()), elements[4]);
    assertEquals("Expected three paths, got " + flushedPaths, 3, flushedPaths.size());
    assertEquals(2, committed.size());
    assertEquals(Sets.newHashSet(now, now + 3 * params.getRollPeriod()), committed);
  }

  @Test(timeout = 10000)
  public synchronized void testWriteLateWithLargeDelay() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    long now = 1500000000000L;
    StreamElement[] elements = {
      // very late, not flushed
      StreamElement.upsert(
          entity, attr, UUID.randomUUID().toString(), "key1", "attr", 0L, new byte[] {1}),
      // late, flushed
      StreamElement.upsert(
          entity,
          wildcard,
          UUID.randomUUID().toString(),
          "key3",
          "wildcard.1",
          now,
          new byte[] {1, 2, 3}),
      // not late
      StreamElement.upsert(
          entity,
          attr,
          UUID.randomUUID().toString(),
          "key1",
          "attr",
          now + 2 * params.getRollPeriod(),
          new byte[] {1})
    };
    List<Long> watermarks =
        new ArrayList<>(
            Arrays.asList(
                now,
                now + 2 * params.getRollPeriod() + params.getAllowedLateness(),
                now + 2 * params.getRollPeriod() + params.getAllowedLateness() + 1));

    Set<Long> committed = Collections.synchronizedSet(new HashSet<>());
    Arrays.stream(elements)
        .forEach(
            e ->
                write(
                    e,
                    watermarks.remove(0),
                    (succ, exc) -> {
                      assertTrue("Exception " + exc, succ);
                      assertNull(exc);
                      committed.add(e.getStamp());
                      latch.countDown();
                    }));
    writer
        .bulk()
        .updateWatermark(now + 4 * params.getRollPeriod() + 1 + params.getAllowedLateness());
    latch.await();
    assertEquals("Written: " + written.keySet(), 3, written.size());
    // on time
    validate(written.get(now + 3 * params.getRollPeriod()), elements[2]);
    // late
    validate(written.get(now), elements[1]);
    // very late
    validate(written.get(0L), elements[0]);
    assertEquals("Expected three paths, got " + flushedPaths, 3, flushedPaths.size());
    assertEquals(1, committed.size());
    assertEquals(Sets.newHashSet(now + 2 * params.getRollPeriod()), committed);
  }

  @Test
  public void testUpdateWatermarkPrioToWrite() {
    // this must not throw exception
    writer.updateWatermark(System.currentTimeMillis());
    // sonar :-)
    assertTrue(true);
  }

  private void validate(List<StreamElement> written, StreamElement... elements) throws IOException {
    assertNotNull(written);
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

  private void write(StreamElement elem, CommitCallback commit) {
    write(elem, elem.getStamp(), commit);
  }

  private void write(StreamElement elem, long watermark, CommitCallback commit) {
    writer.write(elem, watermark, commit);
  }
}
