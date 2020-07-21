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

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class ComplexWriteTest implements Serializable {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  final URI uri = URI.create("abstract-bulk:///");
  final Repository repo = Repository.ofTest(ConfigFactory.load().resolve());
  final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  final AttributeDescriptor<?> attr;
  final AttributeDescriptor<?> wildcard;
  final EntityDescriptor entity;
  final long rollPeriod = 1000;
  final long allowedLateness = 100;
  final AtomicInteger flushed = new AtomicInteger();

  AbstractBulkFileSystemAttributeWriter writer;

  public ComplexWriteTest() throws URISyntaxException {
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
    flushed.set(0);
    writer = initWriter();
  }

  AbstractBulkFileSystemAttributeWriter initWriter() throws IOException {

    FileFormat format = FileFormatUtils.getFileFormatFromName("binary", false);
    NamingConvention naming =
        new DefaultNamingConvention(Duration.ofMillis(rollPeriod), "prefix", "suffix");
    FileSystem fs =
        FileSystem.local(new File(tempFolder.newFolder(), UUID.randomUUID().toString()), naming);
    return new AbstractBulkFileSystemAttributeWriter(
        entity, uri, fs, naming, format, direct.getContext(), rollPeriod, allowedLateness) {

      @Override
      public BulkAttributeWriter.Factory<?> asFactory() {
        return repo -> ExceptionUtils.uncheckedFactory(() -> initWriter());
      }

      @Override
      protected void flush(Bulk v) {
        try {
          List<StreamElement> elements = Lists.newArrayList(format.openReader(v.getPath(), entity));
          flushed.addAndGet(elements.size());
          log.info("Written {} elements to stamp {}", elements.size(), v.getMaxTs());
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  @Test
  public synchronized void testWriteLate() throws Exception {
    long now = 1500000000000L;
    int numElements = 100000;
    Random random = new Random(0);
    List<StreamElement> elements =
        IntStream.range(0, numElements)
            .mapToObj(
                i -> {
                  long stamp =
                      i < numElements - 1
                          ? (long)
                              (now + random.nextGaussian() * 10 * (rollPeriod + allowedLateness))
                          : Long.MAX_VALUE - allowedLateness;
                  return StreamElement.upsert(
                      entity,
                      attr,
                      "key" + i,
                      UUID.randomUUID().toString(),
                      attr.getName(),
                      stamp,
                      new byte[] {1});
                })
            .collect(Collectors.toList());
    AtomicLong committed = new AtomicLong();
    AtomicLong watermark = new AtomicLong(Long.MIN_VALUE);
    int pos = 0;
    List<String> failures = Collections.synchronizedList(new ArrayList<>());
    for (StreamElement el : elements) {
      long currentPos = ++pos;
      writer.write(
          el,
          watermark.get(),
          (succ, exc) -> {
            int flushed = this.flushed.get();
            if (!succ) {
              log.error("Exception while committing offset {}", currentPos, exc);
              failures.add(
                  String.format(
                      "Exception while committing offset %d: %s", currentPos, exc.getMessage()));
            } else if (currentPos > flushed) {
              failures.add(
                  String.format("Committed offset %d while written only %d", currentPos, flushed));
            }
            committed.getAndAccumulate(currentPos, Math::max);
          });
      watermark.accumulateAndGet(el.getStamp(), Math::max);
    }
    writer.updateWatermark(Long.MAX_VALUE);
    while (committed.get() != numElements) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
    assertEquals(numElements, flushed.get());
    assertTrue("Expected empty failures, got " + failures, failures.isEmpty());
  }
}
