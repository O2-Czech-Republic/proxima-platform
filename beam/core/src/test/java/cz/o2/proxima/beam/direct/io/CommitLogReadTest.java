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

import static cz.o2.proxima.util.TestUtils.createTestFamily;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.kafka.KafkaAccessor;
import cz.o2.proxima.direct.kafka.LocalKafkaCommitLogDescriptor;
import cz.o2.proxima.direct.storage.ListCommitLog;
import cz.o2.proxima.direct.time.WatermarkConfiguration;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Optionals;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Test {@link CommitLogRead}. */
@RunWith(Parameterized.class)
public class CommitLogReadTest {

  @Parameters
  public static List<Class<? extends PipelineRunner<?>>> runners() {
    return Arrays.asList(FlinkRunner.class, DirectRunner.class);
  }

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor event = repo.getEntity("event");
  private final AttributeDescriptor<byte[]> data = event.getAttribute("data");

  @Parameter public Class<? extends PipelineRunner<?>> runner;

  @Test(timeout = 30000)
  public void testReadingFromCommitLog() {
    List<StreamElement> data = createInput(1);
    ListCommitLog commitLog = ListCommitLog.of(data, direct.getContext());
    testReadingFromCommitLog(commitLog);
  }

  @Test(timeout = 30000)
  public void testReadingFromCommitLogBounded() {
    // FIXME: need https://github.com/O2-Czech-Republic/proxima-platform/issues/191
    // to be able to support Flink runner
    if (runner.getSimpleName().equals("DirectRunner")) {
      List<StreamElement> data = createInput(1);
      ListCommitLog commitLog = ListCommitLog.of(data, direct.getContext());
      CommitLogRead read =
          CommitLogRead.ofBounded("name", Long.MAX_VALUE, repo.asFactory(), commitLog);
      testReadingFromCommitLog(commitLog, read);
    }
  }

  @Test(timeout = 30000)
  public void testReadingFromCommitLogNonExternalizable() {
    if (isDirect()) {
      List<StreamElement> data = createInput(1);
      ListCommitLog commitLog = ListCommitLog.ofNonExternalizable(data, direct.getContext());
      testReadingFromCommitLog(commitLog);
    }
  }

  @Test(timeout = 60000)
  public void testReadingFromCommitLogMany() {
    int numElements = 1000;
    List<StreamElement> input = createInput(numElements);
    ListCommitLog commitLog = ListCommitLog.of(input, direct.getContext());
    testReadingFromCommitLogMany(numElements, commitLog);
  }

  @Test(timeout = 60000)
  public void testReadingFromCommitLogManyNonExternalizable() {
    if (isDirect()) {
      int numElements = 1000;
      List<StreamElement> input = createInput(numElements);
      ListCommitLog commitLog = ListCommitLog.ofNonExternalizable(input, direct.getContext());
      testReadingFromCommitLogMany(numElements, commitLog);
    }
  }

  @Test(timeout = 60000)
  public void testWatermarkEstimator() {
    int numElements = 1000;
    WatermarkEstimator estimator = new TestWatermarkEstimator(numElements);
    List<StreamElement> input = createInput(numElements);
    ListCommitLog commitLog = ListCommitLog.of(input, estimator, direct.getContext());
    testReadingFromCommitLogMany(numElements, commitLog);
  }

  @Test(timeout = 60000)
  public void testCommitLogReadWithLimit() {
    int numElements = 1000;
    List<StreamElement> input = createInput(numElements);
    ListCommitLog commitLog = ListCommitLog.of(input, direct.getContext());
    testReadingFromCommitLogMany(
        50, CommitLogRead.of("name", Position.CURRENT, 50, repo, commitLog));
  }

  @Test(timeout = 60000)
  public void testWithMultiplePartitions() throws InterruptedException {
    int numPartitions = 3;
    int numElements = 10;
    // this test has undebuggable issues on Flink, skip it for now
    if (runner.getSimpleName().equals("DirectRunner")) {
      LocalKafkaCommitLogDescriptor kafka = new LocalKafkaCommitLogDescriptor();
      KafkaAccessor accessor =
          kafka.createAccessor(
              direct,
              createTestFamily(
                  event,
                  URI.create("kafka-test://brokers/topic-" + UUID.randomUUID().toString()),
                  ImmutableMap.of(
                      LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS,
                      numPartitions,
                      WatermarkConfiguration.prefixedKey(
                          WatermarkConfiguration.CFG_ESTIMATOR_FACTORY),
                      FiniteElementsWatermarkEstimatorFactory.class.getName(),
                      WatermarkConfiguration.prefixedKey("numElements"),
                      numElements,
                      WatermarkConfiguration.prefixedKey("name"),
                      UUID.randomUUID().toString())));

      writeElementsToKafka(numElements, accessor);

      CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));

      testReadingFromCommitLogMany(
          numElements, CommitLogRead.of("name", Position.OLDEST, Long.MAX_VALUE, repo, reader));
    }
  }

  @Test(timeout = 120000)
  public void testWithMultiplePartitionsMany() throws InterruptedException {
    int numPartitions = 3;
    int numElements = 1000;
    LocalKafkaCommitLogDescriptor kafka = new LocalKafkaCommitLogDescriptor();
    KafkaAccessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                event,
                URI.create("kafka-test://brokers/topic-" + UUID.randomUUID().toString()),
                ImmutableMap.of(
                    LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS,
                    numPartitions,
                    WatermarkConfiguration.prefixedKey(
                        WatermarkConfiguration.CFG_ESTIMATOR_FACTORY),
                    FiniteElementsWatermarkEstimatorFactory.class.getName(),
                    WatermarkConfiguration.prefixedKey("numElements"),
                    numElements,
                    WatermarkConfiguration.prefixedKey("name"),
                    UUID.randomUUID().toString())));

    writeElementsToKafka(numElements, accessor);

    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));

    testReadingFromCommitLogMany(
        numElements, CommitLogRead.of("name", Position.OLDEST, Long.MAX_VALUE, repo, reader));
  }

  private void writeElementsToKafka(int numElements, KafkaAccessor accessor)
      throws InterruptedException {
    OnlineAttributeWriter writer = Optionals.get(accessor.getWriter(direct.getContext())).online();
    CountDownLatch latch = new CountDownLatch(numElements);
    createInput(numElements)
        .forEach(
            e ->
                writer.write(
                    e,
                    (succ, exc) -> {
                      assertTrue(succ);
                      latch.countDown();
                    }));
    latch.await();
  }

  private void testReadingFromCommitLogMany(int numElements, ListCommitLog commitLog) {
    testReadingFromCommitLogMany(numElements, getCommitLogReadTransform(commitLog, repo));
  }

  private void testReadingFromCommitLogMany(
      int numElements, PTransform<PBegin, PCollection<StreamElement>> readTransform) {

    Pipeline p = createPipeline();
    PCollection<Integer> count =
        p.apply(readTransform)
            .apply(
                Window.<StreamElement>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                    .discardingFiredPanes())
            .apply(
                MapElements.into(TypeDescriptors.integers())
                    .via(el -> ByteBuffer.wrap(el.getValue()).getInt()))
            .apply(Sum.integersGlobally());
    PAssert.that(count).containsInAnyOrder(numElements * (numElements - 1) / 2);
    try {
      assertNotNull(p.run().waitUntilFinish());
    } catch (Throwable err) {
      err.printStackTrace(System.err);
      throw err;
    }
  }

  private void testReadingFromCommitLog(ListCommitLog commitLog) {
    CommitLogRead read = getCommitLogReadTransform(commitLog, repo);
    testReadingFromCommitLog(commitLog, read);
  }

  private void testReadingFromCommitLog(
      ListCommitLog commitLog, PTransform<PBegin, PCollection<StreamElement>> read) {
    Pipeline p = createPipeline();
    PCollection<Long> count =
        p.apply(read)
            .apply(
                Window.<StreamElement>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                    .discardingFiredPanes())
            .apply(Count.globally());
    PAssert.that(count).containsInAnyOrder(1L);
    try {
      assertNotNull(p.run().waitUntilFinish());
    } catch (Throwable err) {
      err.printStackTrace(System.err);
      throw err;
    }
  }

  private boolean isDirect() {
    return runner.getSimpleName().equals("DirectRunner");
  }

  private Pipeline createPipeline() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(runner);
    if (!isDirect()) {
      FlinkPipelineOptions flinkOpts = opts.as(FlinkPipelineOptions.class);
      flinkOpts.setParallelism(4);
    }
    return Pipeline.create(opts);
  }

  private List<StreamElement> createInput(int num) {
    List<StreamElement> ret = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      ret.add(
          StreamElement.upsert(
              event,
              data,
              UUID.randomUUID().toString(),
              "key",
              data.getName(),
              Instant.now().toEpochMilli(),
              ByteBuffer.allocate(4).putInt(i).array()));
    }
    return ret;
  }

  private static CommitLogRead getCommitLogReadTransform(ListCommitLog commitLog, Repository repo) {
    return CommitLogRead.of("name", Position.CURRENT, Long.MAX_VALUE, repo.asFactory(), commitLog);
  }

  private static class TestWatermarkEstimator implements WatermarkEstimator {

    private final int numElements;
    private long watermark = Watermarks.MIN_WATERMARK;

    public TestWatermarkEstimator(int numElements) {
      this.numElements = numElements;
    }

    @Override
    public long getWatermark() {
      return watermark;
    }

    @Override
    public void setMinWatermark(long minWatermark) {
      // nop
    }

    @Override
    public void update(StreamElement element) {
      if (ByteBuffer.wrap(element.getValue()).getInt() == numElements - 1) {
        watermark = Watermarks.MAX_WATERMARK;
      }
    }
  }

  public static class FiniteElementsWatermarkEstimatorFactory implements WatermarkEstimatorFactory {

    private static final Map<String, Map<Integer, Boolean>> CONSUMED_ELEMENTS =
        new ConcurrentHashMap<>();

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {

      return new WatermarkEstimator() {

        final String name = cfg.get(WatermarkConfiguration.prefixedKey("name")).toString();
        final int numElements =
            Integer.parseInt(cfg.get(WatermarkConfiguration.prefixedKey("numElements")).toString());
        long watermark = Watermarks.MIN_WATERMARK;
        final Map<Integer, Boolean> selfElements =
            CONSUMED_ELEMENTS.computeIfAbsent(name, k -> new ConcurrentHashMap<>());
        final AtomicInteger numIdles = new AtomicInteger();
        long lastUpdateStamp = System.currentTimeMillis();

        @Override
        public long getWatermark() {
          return watermark;
        }

        @Override
        public void setMinWatermark(long minWatermark) {
          // nop
        }

        @Override
        public void update(StreamElement element) {
          int elementId = ByteBuffer.wrap(element.getValue()).getInt();
          selfElements.put(elementId, true);
          numIdles.set(0);
          lastUpdateStamp = System.currentTimeMillis();
        }

        @Override
        public void idle() {
          if (System.currentTimeMillis() > lastUpdateStamp + 2_000) {
            watermark = Watermarks.MAX_WATERMARK;
          }
          if (numIdles.get() >= 10 && selfElements.size() == numElements) {
            watermark = Watermarks.MAX_WATERMARK;
          }
        }
      };
    }
  }
}
