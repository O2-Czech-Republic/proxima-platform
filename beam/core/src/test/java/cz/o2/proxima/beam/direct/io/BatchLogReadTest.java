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

import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.ListBatchReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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
public class BatchLogReadTest {

  @Parameters
  public static List<Class<? extends PipelineRunner<?>>> runners() {
    return Arrays.asList(FlinkRunner.class, DirectRunner.class);
  }

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final Context context = direct.getContext();
  private final EntityDescriptor event = repo.getEntity("event");
  private final AttributeDescriptor<byte[]> data = event.getAttribute("data");

  @Parameter public Class<? extends PipelineRunner<?>> runner;

  @Test(timeout = 30000)
  public void testReadingFromBatchLog() {
    List<StreamElement> data = createInput(1);
    ListBatchReader reader = ListBatchReader.of(context, data);
    testReadingFromBatchLog(Collections.singletonList(this.data), reader);
  }

  @Test(timeout = 60000)
  public void testReadingFromBatchLogMany() {
    int numElements = 1000;
    List<StreamElement> data = createInput(numElements);
    ListBatchReader reader = ListBatchReader.of(context, data);
    testReadingFromBatchLogMany(Collections.singletonList(this.data), numElements, reader);
  }

  @Test(timeout = 60000)
  public void testBatchLogReadWithLimit() {
    int numElements = 1000;
    List<StreamElement> input = createInput(numElements);
    ListBatchReader reader = ListBatchReader.of(direct.getContext(), input);
    testReadingFromBatchLogMany(
        50, BatchLogRead.of(Collections.singletonList(this.data), 50, repo, reader));
  }

  @Test(timeout = 60000)
  public void testWithMultiplePartitions() {
    // this fails randomly on Flink
    if (runner.getSimpleName().equals("DirectRunner")) {
      int numElements = 10;
      BatchLogReader reader =
          ListBatchReader.ofPartitioned(
              context,
              createInput(numElements),
              createInput(numElements, 2 * numElements),
              createInput(2 * numElements, 3 * numElements));
      testReadingFromBatchLogMany(
          3 * numElements,
          BatchLogRead.of(Collections.singletonList(this.data), Long.MAX_VALUE, repo, reader));
    }
  }

  @Test(timeout = 60000)
  public void testWithMultiplePartitionsMany() {
    int numElements = 1000;
    BatchLogReader reader =
        ListBatchReader.ofPartitioned(
            context,
            createInput(numElements),
            createInput(numElements, 2 * numElements),
            createInput(2 * numElements, 3 * numElements));
    testReadingFromBatchLogMany(
        3 * numElements,
        BatchLogRead.of(Collections.singletonList(this.data), Long.MAX_VALUE, repo, reader));
  }

  private void testReadingFromBatchLogMany(
      List<AttributeDescriptor<?>> attrs, int numElements, ListBatchReader reader) {

    testReadingFromBatchLogMany(numElements, BatchLogRead.of(attrs, Long.MAX_VALUE, repo, reader));
  }

  private void testReadingFromBatchLogMany(
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

  private void testReadingFromBatchLog(List<AttributeDescriptor<?>> attrs, BatchLogReader reader) {
    Pipeline p = createPipeline();
    PCollection<Long> count =
        p.apply(BatchLogRead.of(attrs, Long.MAX_VALUE, repo, reader))
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
    return createInput(0, num);
  }

  private List<StreamElement> createInput(int start, int end) {
    List<StreamElement> ret = new ArrayList<>();
    for (int i = start; i < end; i++) {
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
}
