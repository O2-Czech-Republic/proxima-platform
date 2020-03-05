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
package cz.o2.proxima.beam.direct.io;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Test;

public class DirectBatchUnboundedSourceTest {

  @Test
  public void testCheckpointCoder() throws CoderException {
    DirectBatchUnboundedSource.CheckpointCoder coder;
    coder = new DirectBatchUnboundedSource.CheckpointCoder();
    coder.verifyDeterministic();
    DirectBatchUnboundedSource.Checkpoint checkpoint;
    checkpoint = new DirectBatchUnboundedSource.Checkpoint(Arrays.asList(() -> 0), 1);
    DirectBatchUnboundedSource.Checkpoint cloned = CoderUtils.clone(coder, checkpoint);
    assertEquals(1, cloned.getPartitions().size());
    assertEquals(1L, cloned.getSkipFromFirst());
  }

  @Test
  public void testPartitionsSorted() {
    List<Partition> partitions =
        Arrays.asList(partition(0, 4, 5), partition(1, 3, 4), partition(2, 1, 2));
    partitions.sort(DirectBatchUnboundedSource.partitionsComparator());
    assertEquals(
        Arrays.asList(partition(2, 1, 2), partition(1, 3, 4), partition(0, 4, 5)), partitions);
  }

  @Test
  public void testDirectBatchUnboundedSource() {
    testBatchUnboundedSourceWithCount(2);
  }

  @Test(timeout = 20000)
  public void testDirectBatchUnboundedSourceWithMany() {
    testBatchUnboundedSourceWithCount(1000);
  }

  void testBatchUnboundedSourceWithCount(int count) {
    Pipeline pipeline = Pipeline.create();
    Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<Object> armed = gateway.getAttribute("armed");
    DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
    BatchLogObservable observable =
        direct
            .getFamiliesForAttribute(armed)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadBatchSnapshot())
            .findFirst()
            .flatMap(DirectAttributeFamilyDescriptor::getBatchObservable)
            .orElseThrow(() -> new IllegalArgumentException("Missing batch snapshot for armed"));
    PCollection<StreamElement> input =
        pipeline.apply(
            Read.from(
                DirectBatchUnboundedSource.of(
                    repo.asFactory(),
                    observable,
                    Arrays.asList(armed),
                    Long.MIN_VALUE,
                    Long.MAX_VALUE)));
    PCollection<Long> res =
        input
            .apply(
                Window.<StreamElement>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(100))))
                    .accumulatingFiredPanes())
            .apply(Count.globally());
    PAssert.that(res).containsInAnyOrder((long) count);
    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));
    long now = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      writer.write(
          StreamElement.upsert(
              gateway,
              armed,
              UUID.randomUUID().toString(),
              "key" + i,
              armed.getName(),
              now + i,
              new byte[] {1, 2}),
          (succ, exc) -> {});
    }
    pipeline.run();
    direct.close();
  }

  static Partition partition(int id, long minStamp, long maxStamp) {
    return new Partition() {

      @Override
      public int getId() {
        return id;
      }

      @Override
      public long getMinTimestamp() {
        return minStamp;
      }

      @Override
      public long getMaxTimestamp() {
        return maxStamp;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof Partition)) {
          return false;
        }
        return ((Partition) obj).getId() == getId();
      }

      @Override
      public int hashCode() {
        return id;
      }
    };
  }
}
