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
package cz.o2.proxima.beam.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class StreamElementFilterTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void filterUntilTimestampTest() {
    Instant now = Instant.now();
    PCollection<StreamElement> output =
        pipeline
            .apply(
                Create.of(
                    Arrays.asList(
                        write("first", now.minusMillis(5000).toEpochMilli()),
                        write("second", now.minusMillis(1).toEpochMilli()),
                        write("now", now.toEpochMilli()),
                        write("third", now.plusMillis(1000).toEpochMilli()))))
            .setCoder(StreamElementCoder.of(repo))
            .apply(StreamElementFilter.untilTimestamp(now));

    PAssert.thatSingleton(output.apply(Count.globally())).isEqualTo(2L);
    PAssert.that(
            output.apply(MapElements.into(TypeDescriptors.strings()).via(StreamElement::getKey)))
        .containsInAnyOrder(Arrays.asList("first", "second"));
    PipelineResult result = pipeline.run();
    assertNotNull(result);
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("filter-timestamp", "until"))
                    .build());
    long filtered =
        StreamSupport.stream(metrics.getCounters().spliterator(), false)
            .map(MetricResult::getCommitted)
            .reduce(0L, Long::sum);
    assertEquals(2, filtered);
  }

  @Test
  public void filterFromTimestampTest() {
    Instant now = Instant.now();
    PCollection<StreamElement> output =
        pipeline
            .apply(
                Create.of(
                    Arrays.asList(
                        write("first", now.minusMillis(5000).toEpochMilli()),
                        write("second", now.minusMillis(1).toEpochMilli()),
                        write("now", now.toEpochMilli()),
                        write("third", now.plusMillis(1000).toEpochMilli()))))
            .setCoder(StreamElementCoder.of(repo))
            .apply(StreamElementFilter.fromTimestamp(now));

    PAssert.thatSingleton(output.apply(Count.globally())).isEqualTo(2L);
    PAssert.that(
            output.apply(MapElements.into(TypeDescriptors.strings()).via(StreamElement::getKey)))
        .containsInAnyOrder(Arrays.asList("now", "third"));
    PipelineResult result = pipeline.run();
    assertNotNull(result);

    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("filter-timestamp", "from"))
                    .build());
    long filtered =
        StreamSupport.stream(metrics.getCounters().spliterator(), false)
            .map(MetricResult::getCommitted)
            .reduce(0L, Long::sum);
    assertEquals(2, filtered);
  }

  private StreamElement write(String key, long timestamp) {
    return StreamElement.upsert(
        gateway,
        status,
        UUID.randomUUID().toString(),
        key,
        status.getName(),
        timestamp,
        new byte[] {1});
  }
}
