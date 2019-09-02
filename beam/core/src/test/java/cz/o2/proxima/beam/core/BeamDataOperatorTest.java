/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link BeamDataOperator}. */
public class BeamDataOperatorTest {

  final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf"));
  final EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  final AttributeDescriptor<?> armed =
      gateway
          .findAttribute("armed")
          .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));
  final EntityDescriptor proxied =
      repo.findEntity("proxied")
          .orElseThrow(() -> new IllegalStateException("Missing entity proxied"));
  final AttributeDescriptor<?> event =
      proxied
          .findAttribute("event.*")
          .orElseThrow(() -> new IllegalStateException("Missing attribute event.*"));

  BeamDataOperator beam;
  DirectDataOperator direct;
  Pipeline pipeline;
  long now;

  @Before
  public synchronized void setUp() {
    beam = repo.asDataOperator(BeamDataOperator.class);
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
            StreamElement.update(
                gateway, armed, "uuid", "key", armed.getName(), now, new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);
    PCollection<KV<String, Long>> counted =
        CountByKey.of(stream).keyBy(e -> "", TypeDescriptors.strings()).output();
    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L));
    assertNotNull(pipeline.run());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumptionWithWindow() {
    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    writer.write(
        StreamElement.update(
            gateway, armed, "uuid", "key1", armed.getName(), now - 5000, new byte[] {1, 2, 3}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.update(
            gateway, armed, "uuid", "key2", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);

    PCollection<KV<String, Long>> counted =
        CountByKey.of(stream)
            .keyBy(e -> "", TypeDescriptors.strings())
            .windowBy(FixedWindows.of(Duration.millis(1000)))
            .triggeredBy(AfterWatermark.pastEndOfWindow())
            .discardingFiredPanes()
            .withAllowedLateness(Duration.ZERO)
            .output();

    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L), KV.of("", 1L));
    assertNotNull(pipeline.run());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testUnboundedCommitLogConsumptionWithWindow() {
    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    writer.write(
        StreamElement.update(
            gateway, armed, "uuid", "key1", armed.getName(), now - 5000, new byte[] {1, 2, 3}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.update(
            gateway, armed, "uuid", "key2", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<StreamElement> stream =
        beam.getStream("", pipeline, Position.OLDEST, false, true, 2, armed);

    PCollection<KV<String, Long>> counted =
        CountByKey.of(stream)
            .keyBy(e -> "", TypeDescriptors.strings())
            .windowBy(FixedWindows.of(Duration.millis(1000)))
            .triggeredBy(AfterWatermark.pastEndOfWindow())
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(10000))
            .output();

    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L), KV.of("", 1L));
    assertNotNull(pipeline.run());
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

  @Test(timeout = 5000)
  public synchronized void testBatchUpdatesConsumptionWithWindowMany() {
    validatePCollectionWindowedRead(() -> beam.getBatchUpdates(pipeline, armed), 99L);
  }

  @Test(timeout = 5000)
  public synchronized void testBatchSnapshotConsumptionWithWindowMany() {
    validatePCollectionWindowedRead(() -> beam.getBatchSnapshot(pipeline, armed), 99L);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumptionFromProxy() {
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.update(
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
    assertNotNull(pipeline.run());
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
          StreamElement.update(
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
        StreamElement.update(
            gateway, armed, "uuid", "key-last", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<KV<String, Long>> counted =
        CountByKey.of(input.apply())
            .keyBy(e -> "", TypeDescriptors.strings())
            .windowBy(Sessions.withGapDuration(Duration.millis(1000)))
            .triggeredBy(AfterWatermark.pastEndOfWindow())
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(10000))
            .output();

    PAssert.that(counted).containsInAnyOrder(KV.of("", elements), KV.of("", 1L));
    assertNotNull(pipeline.run());
  }
}
