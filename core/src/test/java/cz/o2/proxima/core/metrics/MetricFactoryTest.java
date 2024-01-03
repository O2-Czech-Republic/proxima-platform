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

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class MetricFactoryTest {

  private final MetricFactory factory = new MetricFactory();

  @Test
  public void testMetricCreation() {
    GaugeMetric gauge = factory.gauge("group", "name1");
    assertNotNull(gauge);
    ApproxPercentileMetric percentile = factory.percentile("group", "name2", 30_000, 1_000);
    assertNotNull(percentile);
    TimeAveragingMetric timeAveraging = factory.timeAveraging("group", "name3", 1_000);
    assertNotNull(timeAveraging);
    AbsoluteMetric absolute = factory.absolute("group", "name4");
    assertNotNull(absolute);
  }
}
