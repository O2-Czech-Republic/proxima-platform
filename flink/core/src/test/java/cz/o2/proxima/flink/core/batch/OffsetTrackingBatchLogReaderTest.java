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
package cz.o2.proxima.flink.core.batch;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.direct.batch.Offset;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.storage.ListBatchReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OffsetTrackingBatchLogReaderTest {

  private static final String MODEL =
      "{\n"
          + "  entities: {\n"
          + "    test {\n"
          + "      attributes {\n"
          + "        data: { scheme: \"string\" }\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "  attributeFamilies: {}\n"
          + "}\n";

  private static StreamElement newData(
      Repository repository, String key, Instant timestamp, String value) {
    final EntityDescriptor entity = repository.getEntity("test");
    final AttributeDescriptor<String> attribute = entity.getAttribute("data");
    return StreamElement.upsert(
        entity,
        attribute,
        UUID.randomUUID().toString(),
        key,
        attribute.getName(),
        timestamp.toEpochMilli(),
        attribute.getValueSerializer().serialize(value));
  }

  @Test
  void test() throws InterruptedException {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    final DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class);
    final List<StreamElement> firstPartition =
        Arrays.asList(
            newData(repository, "first", Instant.now(), "value"),
            newData(repository, "first", Instant.now(), "value2"),
            newData(repository, "first", Instant.now(), "value3"));
    final List<StreamElement> secondPartition =
        Arrays.asList(
            newData(repository, "second", Instant.now(), "value"),
            newData(repository, "second", Instant.now(), "value2"),
            newData(repository, "second", Instant.now(), "value3"));
    final BatchLogReader reader =
        OffsetTrackingBatchLogReader.of(
            ListBatchReader.ofPartitioned(direct.getContext(), firstPartition, secondPartition));

    final CountDownLatch latch = new CountDownLatch(1);
    final ObserveHandle handle =
        reader.observe(
            reader.getPartitions(),
            Collections.singletonList(repository.getEntity("test").getAttribute("data")),
            new BatchLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                final OffsetTrackingBatchLogReader.OffsetCommitter committer =
                    (OffsetTrackingBatchLogReader.OffsetCommitter) context;
                committer.markOffsetAsConsumed();
                return !context.getOffset().isLast();
              }

              @Override
              public void onCancelled() {
                latch.countDown();
              }

              @Override
              public void onCompleted() {
                latch.countDown();
              }
            });
    latch.await();

    final OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle otHandle =
        (OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle) handle;

    Assertions.assertEquals(
        Arrays.asList(
            Offset.of(Partition.of(0), firstPartition.size() - 1, true),
            Offset.of(Partition.of(1), -1, false)),
        otHandle.getCurrentOffsets());
  }
}
