/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.direct.io;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.InMemStorage;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.config.ConfigUtils;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test {@link BeamCommitLogReader}. */
public class BeamCommitLogReaderTest {

  private final Repository repo =
      Repository.ofTest(
          ConfigUtils.withStorageReplacement(
              ConfigFactory.load("test-reference.conf").resolve(),
              af -> true,
              af -> URI.create(String.format("inmem://%s", af))));
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final BeamDataOperator beam = repo.getOrCreateOperator(BeamDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  private AtomicLong watermark;
  private AtomicInteger elementsWritten;

  @Before
  public void setUp() {
    watermark = new AtomicLong();
    elementsWritten = new AtomicInteger();
    repo.getAllFamilies()
        .filter(af -> af.getStorageUri().getScheme().equals("inmem"))
        .forEach(
            af ->
                InMemStorage.setWatermarkEstimatorFactory(
                    af.getStorageUri(),
                    (stamp, name, offset) -> asWatermarkEstimator(elementsWritten, watermark)));
  }

  @After
  public void tearDown() {
    beam.close();
    direct.close();
  }

  @Test
  public void testReadingFromCommitLogEventTime() throws InterruptedException {
    testReadingFromCommitLog(true, false);
  }

  @Test
  public void testReadingFromCommitLogEventTimeBounded() throws InterruptedException {
    testReadingFromCommitLog(true, true);
  }

  @Test
  public void testReadingFromCommitLogProcesingTimeBounded() throws InterruptedException {
    testReadingFromCommitLog(false, true);
  }

  private void testReadingFromCommitLog(boolean eventTime, boolean stopAtCurrent) {
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> stream =
        beam.getStream(p, Position.OLDEST, stopAtCurrent, eventTime, status);
    PCollection<Long> result =
        stream
            .apply(
                Window.<StreamElement>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes())
            .apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(2L);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    write("key1");
    write("key2");
    Future<Boolean> future =
        direct
            .getContext()
            .getExecutorService()
            .submit(
                () -> {
                  try {
                    assertNotNull(p.run());
                    return true;
                  } catch (Throwable ex) {
                    caught.set(ex);
                    return false;
                  }
                });
    if (!stopAtCurrent) {
      watermark.set(Watermarks.MAX_WATERMARK);
    }
    if (!ExceptionUtils.uncheckedFactory(future::get)) {
      throw new AssertionError(caught.get());
    }
  }

  private void write(String key) {

    StreamElement element =
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            key,
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {1});

    elementsWritten.incrementAndGet();
    direct
        .getWriter(status)
        .orElseThrow(() -> new IllegalStateException("Missing writer for " + status))
        .write(element, (succ, exc) -> {});
  }

  private static WatermarkEstimator asWatermarkEstimator(
      AtomicInteger elementsWritten, AtomicLong watermark) {
    return new WatermarkEstimator() {

      Set<StreamElement> updated = new HashSet<>();

      @Override
      public void update(StreamElement element) {
        updated.add(element);
      }

      @Override
      public long getWatermark() {
        return updated.size() >= elementsWritten.get() ? watermark.get() : Watermarks.MIN_WATERMARK;
      }

      @Override
      public void setMinWatermark(long minWatermark) {}
    };
  }
}
