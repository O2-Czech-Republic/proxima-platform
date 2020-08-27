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
package cz.o2.proxima.beam.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link BeamDataOperator}. */
public class BeamDataOperatorTest {

  final Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf"));
  final EntityDescriptor gateway = repo.getEntity("gateway");
  final AttributeDescriptor<?> armed = gateway.getAttribute("armed");
  final EntityDescriptor proxied = repo.getEntity("proxied");
  final AttributeDescriptor<?> event = proxied.getAttribute("event.*");

  BeamDataOperator beam;
  DirectDataOperator direct;
  Pipeline pipeline;
  long now;

  @Before
  public synchronized void setUp() {
    beam = repo.getOrCreateOperator(BeamDataOperator.class);
    direct = beam.getDirect();
    pipeline = Pipeline.create();
    now = System.currentTimeMillis();
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumption() {
    direct
        .getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer for armed"))
        .write(
            StreamElement.upsert(
                gateway, armed, "uuid", "key", armed.getName(), now, new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);
    PCollection<KV<String, Long>> counted =
        CountByKey.of(stream).keyBy(e -> "", TypeDescriptors.strings()).output();
    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L));
    runPipeline(pipeline);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumptionWithWindow() {
    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key1", armed.getName(), now - 5000, new byte[] {1, 2, 3}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key2", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);

    PCollection<KV<String, Long>> counted =
        CountByKey.of(stream)
            .keyBy(e -> "", TypeDescriptors.strings())
            .windowBy(FixedWindows.of(Duration.millis(1000)))
            .triggeredBy(
                AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.ZERO)
            .output();

    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L), KV.of("", 1L));
    runPipeline(pipeline);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testUnboundedCommitLogConsumptionWithWindow() {
    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key1", armed.getName(), now - 5000, new byte[] {1, 2, 3}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key2", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<StreamElement> stream =
        beam.getStream("", pipeline, Position.OLDEST, false, true, 2, armed);

    PCollection<KV<String, Long>> counted =
        CountByKey.of(stream)
            .keyBy(e -> "", TypeDescriptors.strings())
            .windowBy(FixedWindows.of(Duration.millis(1000)))
            .triggeredBy(
                AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(10000))
            .output();

    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L), KV.of("", 1L));
    runPipeline(pipeline);
  }

  @Test(timeout = 30000)
  public synchronized void testUnboundedCommitLogConsumptionWithWindowMany() {
    for (int round = 0; round < 10; round++) {
      setUp();
      final long elements = 99L;
      now += round * elements;
      validatePCollectionWindowedRead(
          () -> beam.getStream("", pipeline, Position.OLDEST, false, true, elements + 1, armed),
          elements);
    }
  }

  @Test(timeout = 180000)
  public synchronized void testUnboundedCommitLogConsumptionWithWindowManyMany() {
    final long elements = 1000L;
    validatePCollectionWindowedRead(
        () -> beam.getStream("", pipeline, Position.OLDEST, false, true, elements + 1, armed),
        elements);
  }

  @Test /* (timeout = 5000) */
  public synchronized void testBatchUpdatesConsumptionWithWindowMany() {
    validatePCollectionWindowedRead(() -> beam.getBatchUpdates(pipeline, armed), 99L);
  }

  @Test(timeout = 5000)
  public synchronized void testBatchSnapshotConsumptionWithWindowMany() {
    validatePCollectionWindowedRead(() -> beam.getBatchSnapshot(pipeline, armed), 99L);
  }

  @Test(timeout = 5000)
  public void testReadingFromSpecificFamily() {
    validatePCollectionWindowedRead(
        () ->
            beam.getAccessorFor(
                    beam.getRepository()
                        .getFamiliesForAttribute(armed)
                        .stream()
                        .filter(af -> af.getAccess().canReadBatchSnapshot())
                        .findFirst()
                        .orElseThrow(
                            () -> new IllegalStateException("Missing batch snapshot for " + armed)))
                .createBatch(
                    pipeline, Collections.singletonList(armed), Long.MIN_VALUE, Long.MAX_VALUE),
        99L);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumptionFromProxy() {
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway,
                event,
                "uuid",
                "key",
                event.toAttributePrefix() + "1",
                now,
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, event);
    stream =
        Filter.of(stream)
            .by(e -> e.getAttributeDescriptor().toAttributePrefix().startsWith("event"))
            .output();
    PCollection<KV<String, Long>> counted =
        CountByKey.of(stream).keyBy(e -> "", TypeDescriptors.strings()).output();
    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L));
    runPipeline(pipeline);
  }

  @Test
  public void testTwoPipelines() {
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway,
                event,
                "uuid",
                "key",
                event.toAttributePrefix() + "1",
                now,
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});

    PCollection<Long> result =
        beam.getStream(pipeline, Position.OLDEST, true, true, event).apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    runPipeline(pipeline);

    pipeline = Pipeline.create();
    result = beam.getStream(pipeline, Position.OLDEST, true, true, event).apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    runPipeline(pipeline);
  }

  @Test
  public void testUnion() {
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway,
                event,
                "uuid",
                "key",
                event.toAttributePrefix() + "1",
                now,
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    direct
        .getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway, armed, "uuid", "key2", armed.getName(), now + 1, new byte[] {1, 2, 3, 4}),
            (succ, exc) -> {});

    PCollection<StreamElement> events =
        beam.getStream(pipeline, Position.OLDEST, true, true, event);
    PCollection<StreamElement> armedStream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);
    PCollection<Long> result = Union.of(events, armedStream).output().apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(2L);
    runPipeline(pipeline);
  }

  @Test
  public void testStreamFromOldestWithKafkaTest() {
    Config config =
        ConfigFactory.parseMap(
                Collections.singletonMap(
                    "attributeFamilies.event-storage-stream.storage", "kafka-test://dummy/events"))
            .withFallback(ConfigFactory.load("test-reference.conf"));
    Repository repo = Repository.ofTest(config);
    EntityDescriptor event = repo.getEntity("event");
    AttributeDescriptor<?> data = event.getAttribute("data");
    int numElements = 10000;
    long now = System.currentTimeMillis();
    try (DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
        BeamDataOperator operator = repo.getOrCreateOperator(BeamDataOperator.class)) {

      for (int i = 0; i < numElements; i++) {
        direct
            .getWriter(data)
            .orElseThrow(() -> new IllegalStateException("Missing writer for data"))
            .write(
                StreamElement.upsert(
                    event,
                    data,
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    data.getName(),
                    now + i,
                    new byte[] {}),
                (succ, exc) -> {});
      }
      Pipeline p = Pipeline.create();
      PCollection<StreamElement> input = operator.getStream(p, Position.OLDEST, true, true, data);
      PCollection<Long> count = input.apply(Count.globally());
      PAssert.that(count).containsInAnyOrder(Collections.singletonList((long) numElements));
      assertNotNull(p.run());
    }
  }

  @Test
  public void testMultipleConsumptionsFromSingleAttribute() {
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> first = beam.getStream(p, Position.OLDEST, true, true, armed);
    PCollection<StreamElement> second = beam.getStream(p, Position.OLDEST, true, true, armed);
    // validate that we have used cached PCollection
    terminatePipeline(first, second);
    checkHasSingleInput(p);

    p = Pipeline.create();
    first = beam.getBatchUpdates(p, armed);
    second = beam.getBatchUpdates(p, armed);
    terminatePipeline(first, second);
    checkHasSingleInput(p);

    p = Pipeline.create();
    first = beam.getBatchSnapshot(p, armed);
    second = beam.getBatchSnapshot(p, armed);
    terminatePipeline(first, second);
    checkHasSingleInput(p);
  }

  @Test
  public void testReadFromProxy() {
    EntityDescriptor entity = repo.getEntity("proxied");
    AttributeDescriptor<byte[]> event = entity.getAttribute("event.*");
    direct
        .getWriter(event)
        .get()
        .write(
            StreamElement.upsert(
                entity,
                event,
                UUID.randomUUID().toString(),
                "key",
                event.toAttributePrefix() + 1,
                System.currentTimeMillis(),
                new byte[] {1}),
            (succ, exc) -> {});
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> input = beam.getStream(p, Position.OLDEST, true, true, event);
    PAssert.that(input.apply(Count.globally())).containsInAnyOrder(1L);
    assertNotNull(p.run());
  }

  private void terminatePipeline(
      PCollection<StreamElement> first, PCollection<StreamElement> second) {
    PCollection<StreamElement> union = Union.of(first, second).output();
    union.apply(done());
  }

  private static <T> PTransform<PCollection<T>, PDone> done() {
    return new PTransform<PCollection<T>, PDone>() {
      @Override
      public PDone expand(PCollection<T> input) {
        return PDone.in(input.getPipeline());
      }
    };
  }

  private void checkHasSingleInput(Pipeline p) {
    AtomicBoolean terminated = new AtomicBoolean();
    p.traverseTopologically(checkHasSingleInputVisitor(terminated));
    assertTrue(terminated.get());
  }

  private PipelineVisitor checkHasSingleInputVisitor(AtomicBoolean terminated) {
    Set<Node> roots = new HashSet<>();
    return new PipelineVisitor() {

      @Override
      public void enterPipeline(Pipeline p) {}

      @Override
      public CompositeBehavior enterCompositeTransform(Node node) {
        visitNode(node);
        return CompositeBehavior.ENTER_TRANSFORM;
      }

      @Override
      public void leaveCompositeTransform(Node node) {}

      @Override
      public void visitPrimitiveTransform(Node node) {
        visitNode(node);
      }

      @Override
      public void visitValue(PValue value, Node producer) {}

      @Override
      public void leavePipeline(Pipeline pipeline) {
        assertTrue(roots.size() == 1);
        terminated.set(true);
      }

      void visitNode(Node node) {
        if (node.getTransform() != null && node.getInputs().isEmpty()) {
          roots.add(node);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private synchronized void validatePCollectionWindowedRead(
      Factory<PCollection<StreamElement>> input, long elements) {

    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    for (int i = 0; i < elements; i++) {
      writer.write(
          StreamElement.upsert(
              gateway,
              armed,
              "uuid",
              "key" + i,
              armed.getName(),
              now - 2000 - i,
              new byte[] {1, 2, 3}),
          (succ, exc) -> {});
    }

    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key-last", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<KV<String, Long>> counted =
        CountByKey.of(input.apply())
            .keyBy(e -> "", TypeDescriptors.strings())
            .windowBy(Sessions.withGapDuration(Duration.millis(1000)))
            .triggeredBy(
                AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(10000))
            .output();

    PAssert.that(counted).containsInAnyOrder(KV.of("", elements), KV.of("", 1L));
    runPipeline(pipeline);
  }

  private void runPipeline(Pipeline pipeline) {
    try {
      assertNotNull(pipeline.run());
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }
  }
}
