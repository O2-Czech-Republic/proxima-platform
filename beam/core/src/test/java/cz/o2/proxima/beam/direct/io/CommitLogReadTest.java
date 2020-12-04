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

import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.ListCommitLog;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

/** Test {@link CommitLogRead}. */
public class CommitLogReadTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor event = repo.getEntity("event");
  private final AttributeDescriptor<byte[]> data = event.getAttribute("data");

  @Test(timeout = 30000)
  public void testReadingFromCommitLog() {
    List<StreamElement> data = createInput(1);
    ListCommitLog commitLog = ListCommitLog.of(data, direct.getContext());
    testReadingFromCommitLog(commitLog);
  }

  @Test(timeout = 30000)
  public void testReadingFromCommitLogNonExternalizable() {
    List<StreamElement> data = createInput(1);
    ListCommitLog commitLog = ListCommitLog.ofNonExternalizable(data, direct.getContext());
    testReadingFromCommitLog(commitLog);
  }

  @Test(timeout = 30000)
  public void testReadingFromCommitLogMany() {
    int numElements = 1000;
    List<StreamElement> input = createInput(numElements);
    ListCommitLog commitLog = ListCommitLog.of(input, direct.getContext());
    testReadingFromCommitLogMany(numElements, commitLog);
  }

  @Test(timeout = 10000)
  public void testReadingFromCommitLogManyNonExternalizable() {
    int numElements = 1000;
    List<StreamElement> input = createInput(numElements);
    ListCommitLog commitLog = ListCommitLog.ofNonExternalizable(input, direct.getContext());
    testReadingFromCommitLogMany(numElements, commitLog);
  }

  private void testReadingFromCommitLogMany(int numElements, ListCommitLog commitLog) {
    Pipeline p = Pipeline.create();
    PCollection<Integer> count =
        p.apply(CommitLogRead.of("name", Position.CURRENT, Long.MAX_VALUE, repo, commitLog))
            .apply(
                Window.<StreamElement>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                    .discardingFiredPanes())
            .apply(
                MapElements.into(TypeDescriptors.integers())
                    .via(el -> ByteBuffer.wrap(el.getValue()).getInt()))
            .apply(Sum.integersGlobally());
    PAssert.that(count).containsInAnyOrder(numElements * (numElements - 1) / 2);
    assertNotNull(p.run().waitUntilFinish());
  }

  private void testReadingFromCommitLog(ListCommitLog commitLog) {
    Pipeline p = Pipeline.create();
    PCollection<Long> count =
        p.apply(CommitLogRead.of("name", Position.CURRENT, Long.MAX_VALUE, repo, commitLog))
            .apply(
                Window.<StreamElement>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                    .discardingFiredPanes())
            .apply(Count.globally());
    PAssert.that(count).containsInAnyOrder(1L);
    assertNotNull(p.run().waitUntilFinish());
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
}
