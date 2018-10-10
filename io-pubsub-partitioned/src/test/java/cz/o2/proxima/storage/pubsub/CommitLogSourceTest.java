/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.pubsub;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.ListCommitLog;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.pubsub.io.CommitLogSource;
import cz.seznam.euphoria.beam.BeamFlow;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.util.Sums;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Test {@link CommitLogSource}.
 */
@Slf4j
public class CommitLogSourceTest {

  private final transient Repository repo = Repository.of(ConfigFactory.empty());
  private final AttributeDescriptorBase<byte[]> attr = AttributeDescriptor
      .newBuilder(repo)
      .setEntity("entity")
      .setName("attr")
      .setSchemeUri(new URI("bytes:///"))
      .build();

  private final EntityDescriptor entity = EntityDescriptor.newBuilder()
      .setName("entity")
      .addAttribute(attr)
      .build();

  public CommitLogSourceTest() throws Exception {

  }

  @Test
  public void testReading() throws URISyntaxException, InterruptedException {
    CommitLogReader reader = ListCommitLog.of(
        Arrays.asList(
            StreamElement.update(
                entity, attr, UUID.randomUUID().toString(),
                "key1", attr.getName(), System.currentTimeMillis(), new byte[] { 1 }),
            StreamElement.update(
                entity, attr, UUID.randomUUID().toString(),
                "key2", attr.getName(), System.currentTimeMillis(), new byte[] { 2 }),
            StreamElement.update(
                entity, attr, UUID.randomUUID().toString(),
                "key3", attr.getName(), System.currentTimeMillis(), new byte[] { 3 })),
        context());

    CommitLogSource source = CommitLogSource.of(reader, "name", 100);
    Pipeline pipeline = Pipeline.create();
    BeamFlow flow = BeamFlow.create(pipeline);
    PCollection<AttributeData> input = flow
        .getPipeline()
        .apply(Read.from(source).withMaxNumRecords(3));
    Dataset<Integer> output = ReduceWindow.of(flow.wrapped(input))
        .valueBy(e -> (int) e.getValue()[0])
        .combineBy(Sums.ofInts())
        .windowBy(Time.of(Duration.ofHours(1)))
        .output();
    PAssert.that(flow.unwrapped(output))
        .containsInAnyOrder(6);

    pipeline.run().waitUntilFinish();
  }

  private static Context context() {
    return new Context(() -> Executors.newCachedThreadPool()) {
    };
  }

}
