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
package cz.o2.proxima.direct.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValueFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Optionals;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/** Integration test for {@link KafkaLogReader}. */
public class KafkaLogReaderIT {

  private static final long AWAIT_TIMEOUT_MS = 5_000;

  private static final String CONFIG =
      "entities {\n"
          + "  entity {\n"
          + "    attributes {\n"
          + "      foo: { scheme: bytes }\n"
          + "    }\n"
          + "  }\n"
          + "}\n"
          + "attributeFamilies {\n"
          + "  scalar-primary {\n"
          + "    entity: entity\n"
          + "    attributes: [\"foo\"]\n"
          + "    storage: \"kafka://\"${broker}\"/foo\"\n"
          + "    type: primary\n"
          + "    access: commit-log\n"
          + "    watermark {\n"
          + "      idle-policy-factory: cz.o2.proxima.direct.time.NotProgressingWatermarkIdlePolicy.Factory\n"
          + "    }\n"
          + "    assignment-timeout-ms: 1000\n"
          + "  }\n"
          + "}\n";

  private static void await(CountDownLatch latch) throws InterruptedException {
    Assert.assertTrue(latch.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
  }

  private static class TestLogObserver implements LogObserver {

    private final AtomicInteger numReceivedElements = new AtomicInteger();
    private final CountDownLatch completed = new CountDownLatch(1);

    @Override
    public void onCompleted() {
      completed.countDown();
    }

    @Override
    public boolean onError(Throwable error) {
      return false;
    }

    @Override
    public boolean onNext(StreamElement ingest, OnNextContext context) {
      if (!ingest.getKey().startsWith("poisoned-pill")) {
        numReceivedElements.incrementAndGet();
      }
      context.confirm();
      return true;
    }

    CountDownLatch getCompleted() {
      return completed;
    }

    int getNumReceivedElements() {
      return numReceivedElements.get();
    }
  }

  @Rule public final EmbeddedKafkaRule rule = new EmbeddedKafkaRule(1);

  private DirectDataOperator operator;
  private EntityDescriptor entity;
  private AttributeDescriptor<byte[]> fooDescriptor;

  @Before
  public void setup() {
    final Repository repository = Repository.ofTest(createConfig());
    entity = repository.getEntity("entity");
    fooDescriptor = entity.getAttribute("foo");
    operator = repository.getOrCreateOperator(DirectDataOperator.class);
  }

  @Test(timeout = 30_000L)
  @Ignore(value = "https://github.com/O2-Czech-Republic/proxima-platform/issues/183")
  public void testReadFromCurrent() throws InterruptedException {
    final EmbeddedKafkaBroker embeddedKafka = rule.getEmbeddedKafka();
    final int numPartitions = 3;
    embeddedKafka.addTopics(new NewTopic("foo", numPartitions, (short) 1));
    final CommitLogReader commitLogReader =
        Optionals.get(operator.getCommitLogReader(fooDescriptor));
    final TestLogObserver observer = new TestLogObserver();
    final ObserveHandle handle = commitLogReader.observe("test-reader", Position.CURRENT, observer);
    handle.waitUntilReady();
    final int numElements = 100;
    writeElements(numElements);
    writePoisonedPills(numPartitions);
    await(observer.getCompleted());
    handle.close();
    Assert.assertEquals(numElements, observer.getNumReceivedElements());
    Assert.assertEquals(numElements + numPartitions, numCommittedElements(handle));
  }

  @Test(timeout = 30_000L)
  public void testReadFromOldest() throws InterruptedException {
    final EmbeddedKafkaBroker embeddedKafka = rule.getEmbeddedKafka();
    final int numPartitions = 3;
    embeddedKafka.addTopics(new NewTopic("foo", numPartitions, (short) 1));
    final CommitLogReader commitLogReader =
        Optionals.get(operator.getCommitLogReader(fooDescriptor));
    // Write everything up front, so we can be sure that we really seek all the way to beginning.
    final int numElements = 100;
    await(writeElements(numElements));
    await(writePoisonedPills(numPartitions));
    final TestLogObserver firstObserver = new TestLogObserver();
    final ObserveHandle firstHandle =
        commitLogReader.observe("test-reader", Position.OLDEST, firstObserver);
    // First observer should successfully complete and commit offsets.
    await(firstObserver.getCompleted());
    Assert.assertEquals(numElements, firstObserver.getNumReceivedElements());
    Assert.assertEquals(numElements + numPartitions, numCommittedElements(firstHandle));
    firstHandle.close();
    // Second observer share the same name and should start from committed offsets.
    await(writePoisonedPills(3));
    final TestLogObserver secondObserver = new TestLogObserver();
    final ObserveHandle secondHandle =
        commitLogReader.observe("test-reader", Position.OLDEST, secondObserver);
    await(secondObserver.getCompleted());
    Assert.assertEquals(0, secondObserver.getNumReceivedElements());
    Assert.assertEquals(numElements + 2 * numPartitions, numCommittedElements(secondHandle));
  }

  // --------------------------------------------------------------------------
  // HELPER METHODS
  // --------------------------------------------------------------------------

  private long numCommittedElements(ObserveHandle handle) {
    return handle
        .getCommittedOffsets()
        .stream()
        .mapToLong(offset -> ((TopicOffset) offset).getOffset())
        .sum();
  }

  private CountDownLatch writeElements(int numElements) {
    final OnlineAttributeWriter writer = Optionals.get(operator.getWriter(fooDescriptor));
    final CountDownLatch done = new CountDownLatch(numElements);
    for (int i = 0; i < numElements; i++) {
      final StreamElement element =
          StreamElement.upsert(
              entity,
              fooDescriptor,
              UUID.randomUUID().toString(),
              String.format("element-%d", i),
              fooDescriptor.getName(),
              i,
              "value".getBytes(StandardCharsets.UTF_8));
      writer.write(
          element,
          ((success, error) -> {
            if (success) {
              done.countDown();
            }
          }));
    }
    return done;
  }

  /**
   * Write poisoned pills (element with timestamp = {@link Watermarks#MAX_WATERMARK}) to all
   * partitions.
   *
   * @param numPartitions Number of partitions in topic.
   * @return Completion latch.
   */
  private CountDownLatch writePoisonedPills(int numPartitions) {
    final OnlineAttributeWriter writer = Optionals.get(operator.getWriter(fooDescriptor));
    // We assume test uses default partitioner.
    final KeyPartitioner keyPartitioner = new KeyPartitioner();
    final Set<Integer> poisonedPartitions = new HashSet<>();
    final CountDownLatch done = new CountDownLatch(numPartitions);
    for (int i = 0; poisonedPartitions.size() < numPartitions; i++) {
      final StreamElement poisonedPill =
          StreamElement.upsert(
              entity,
              fooDescriptor,
              UUID.randomUUID().toString(),
              String.format("poisoned-pill-%d", i),
              fooDescriptor.getName(),
              Watermarks.MAX_WATERMARK,
              "value".getBytes(StandardCharsets.UTF_8));
      final int partition =
          (keyPartitioner.getPartitionId(poisonedPill) & Integer.MAX_VALUE) % numPartitions;
      if (poisonedPartitions.add(partition)) {
        writer.write(
            poisonedPill,
            ((success, error) -> {
              if (success) {
                done.countDown();
              }
            }));
      }
    }
    return done;
  }

  private Config createConfig() {
    final EmbeddedKafkaBroker embeddedKafka = rule.getEmbeddedKafka();
    final String connectionString =
        Arrays.stream(embeddedKafka.getBrokerAddresses())
            .map(ba -> ba.getHost() + ":" + ba.getPort())
            .collect(Collectors.joining(","));
    return ConfigFactory.parseString(CONFIG)
        .resolveWith(
            ConfigFactory.empty()
                .withValue("broker", ConfigValueFactory.fromAnyRef(connectionString)),
            ConfigResolveOptions.noSystem());
  }
}
