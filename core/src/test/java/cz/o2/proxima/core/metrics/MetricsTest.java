/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.metrics;

import static org.junit.Assert.*;

import org.junit.Test;

/** Test various simple metrics. */
public class MetricsTest {

  @Test
  public void testAbsoluteMetric() {
    ScalarMetric metric = AbsoluteMetric.of("group", "name");
    metric.increment();
    assertEquals(1.0, metric.getValue(), 0.0001);
    metric.increment();
    assertEquals(2.0, metric.getValue(), 0.0001);
    metric.increment(2.5);
    assertEquals(4.5, metric.getValue(), 0.0001);
    metric.reset();
    assertEquals(0.0, metric.getValue(), 0.0001);
    metric.increment();
    assertEquals(1.0, metric.getValue(), 0.0001);
    metric.decrement();
    assertEquals(0.0, metric.getValue(), 0.0001);

    assertEquals("group", metric.getGroup());
    assertEquals("name", metric.getName());
  }

  @Test
  public void testGaugeMetric() {
    ScalarMetric metric = GaugeMetric.of("group", "name");
    metric.increment(100.0);
    assertEquals(100.0, metric.getValue(), 0.0001);
    metric.increment(50.0);
    assertEquals(50.0, metric.getValue(), 0.0001);
    metric.increment();
    assertEquals(1.0, metric.getValue(), 0.0001);
    metric.reset();
    assertEquals(0.0, metric.getValue(), 0.0001);

    assertEquals("group", metric.getGroup());
    assertEquals("name", metric.getName());
  }
}
