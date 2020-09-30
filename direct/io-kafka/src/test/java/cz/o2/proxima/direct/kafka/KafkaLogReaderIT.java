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
package cz.o2.proxima.direct.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValueFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.Optionals;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/** Integration test for {@link KafkaLogReader}. */
public class KafkaLogReaderIT {

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
          + "    assignment-timeout-ms: 100\n"
          + "  }\n"
          + "}\n";
  @Rule public final EmbeddedKafkaRule rule = new EmbeddedKafkaRule(1);

  @Test(timeout = 30_000L)
  public void testWriteRead() throws InterruptedException {
    final EmbeddedKafkaBroker embeddedKafka = rule.getEmbeddedKafka();
    embeddedKafka.addTopics(new NewTopic("foo", 3, (short) 1));
    final String connectionString =
        Arrays.stream(embeddedKafka.getBrokerAddresses())
            .map(ba -> ba.getHost() + ":" + ba.getPort())
            .collect(Collectors.joining(","));
    final Config config =
        ConfigFactory.parseString(CONFIG)
            .resolveWith(
                ConfigFactory.empty()
                    .withValue("broker", ConfigValueFactory.fromAnyRef(connectionString)),
                ConfigResolveOptions.noSystem());
    final Repository repository = Repository.ofTest(config);
    final EntityDescriptor entity = repository.getEntity("entity");
    final AttributeDescriptor<byte[]> fooDescriptor = entity.getAttribute("foo");
    final CommitLogReader commitLogReader =
        Optionals.get(
            repository
                .getOrCreateOperator(DirectDataOperator.class)
                .getCommitLogReader(fooDescriptor));
    final CountDownLatch finishedReading = new CountDownLatch(1);
    final AtomicInteger numElements = new AtomicInteger(0);
    final ObserveHandle handle =
        commitLogReader.observe(
            "test-reader",
            Position.OLDEST,
            new LogObserver() {

              @Override
              public void onCompleted() {
                finishedReading.countDown();
              }

              @Override
              public boolean onError(Throwable error) {
                return false;
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                System.out.println("WATERMARK " + context.getWatermark());
                numElements.incrementAndGet();
                return Long.MAX_VALUE != context.getWatermark();
              }
            });
    handle.waitUntilReady();
    final OnlineAttributeWriter writer =
        Optionals.get(
            repository.getOrCreateOperator(DirectDataOperator.class).getWriter(fooDescriptor));
    for (int i = 0; i < 100; i++) {
      writer.write(
          StreamElement.upsert(
              entity,
              fooDescriptor,
              UUID.randomUUID().toString(),
              Integer.toString(i),
              fooDescriptor.getName(),
              Long.MAX_VALUE,
              "value".getBytes(StandardCharsets.UTF_8)),
          CommitCallback.noop());
    }
    finishedReading.await();
    Assert.assertTrue(numElements.get() > 3);
  }
}
