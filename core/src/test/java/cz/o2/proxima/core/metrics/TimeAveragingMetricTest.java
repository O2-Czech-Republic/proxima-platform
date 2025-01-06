/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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

import org.junit.Before;
import org.junit.Test;

/** Test {@code TimeAveragingMetric}. */
public class TimeAveragingMetricTest {

  private TimeAveragingMetric metric;

  @Before
  public void setUp() {
    metric = new TimeAveragingMetric("test", "test", 1_000, 1_000, 1_000);
  }

  @Test
  public void testMetric() {
    long now = System.currentTimeMillis();

    // increment 2 times a second for 4 seconds
    for (int i = 0; i < 8; i++) {
      while (System.currentTimeMillis() - now < 495 * (i + 1)) {
        // no-op
      }
      if (i < 1) {
        assertEquals("Error on round " + i, 0.0, metric.getValue(), 0.0001);
      }
      metric.increment();
    }

    // check this just roughly
    assertEquals(2.0, metric.getValue(), 0.5);
    now = System.currentTimeMillis();

    for (int i = 0; i < 4; i++) {
      while (System.currentTimeMillis() - now < 495 * (i + 1)) {
        // no-op
      }
      if (i > 2) {
        // after one second, we should have zero again
        assertEquals(0.0, metric.getValue(), 1);
      }
    }
  }
}
