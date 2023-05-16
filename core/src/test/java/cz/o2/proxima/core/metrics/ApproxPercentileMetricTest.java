/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Test for approximate percentile metric test. */
public class ApproxPercentileMetricTest {

  private final Random random = new Random();

  @Test
  public void testMetric() {
    ApproxPercentileMetric m = new ApproxPercentileMetric("test", "test", 60000, 60000);
    // put in some uniformly distributed values in range 0..999
    int count = 100000;
    for (int i = 0; i < count; i++) {
      int r = random.nextInt(1000);
      m.increment(r);
    }
    double[] result = m.getValue().getRaw();
    assertTrue("Invalid value, got " + result[0], result[0] < 30);
    assertTrue("Invalid value, got " + result[1], 50 < result[1] && result[1] < 150);
    assertTrue("Invalid value, got " + result[2], 249 < result[2] && result[2] < 351);
    assertTrue("Invalid value, got " + result[3], 450 < result[3] && result[3] < 550);
    assertTrue("Invalid value, got " + result[4], 650 < result[4] && result[4] < 750);
    assertTrue("Invalid value, got " + result[5], 850 < result[5] && result[5] < 950);
    assertTrue("Invalid value, got " + result[6], 950 < result[6]);
  }

  @Test
  public void testMetricWhenDistributionUpdates() {
    ApproxPercentileMetric m = new ApproxPercentileMetric("test", "test", 60000, 60000);
    // put in some uniformly distributed values in range 0..999
    final int k = 2000;
    for (int i = 0; i < k; i++) {
      m.increment(random.nextInt(1000));
    }
    // change the distribution and put in values distributed in 0..99
    for (int i = 0; i < 1000 * k; i++) {
      m.increment(random.nextInt(100));
    }
    double[] result = m.getValue().getRaw();
    assertTrue("Invalid value, got " + result[0], result[0] < 3);
    assertTrue("Invalid value, got " + result[1], 5 < result[1] && result[1] < 15);
    assertTrue("Invalid value, got " + result[2], 25 < result[2] && result[2] < 35);
    assertTrue("Invalid value, got " + result[3], 45 < result[3] && result[3] < 55);
    assertTrue("Invalid value, got " + result[4], 64 < result[4] && result[4] < 76);
    assertTrue("Invalid value, got " + result[5], 85 < result[5] && result[5] < 95);
    assertTrue("Invalid value, got " + result[6], 95 < result[6]);
  }

  @Test
  public void testWindowing() throws InterruptedException {
    ApproxPercentileMetric m = new ApproxPercentileMetric("test", "test", 1000, 100);
    // put 20-times 10 values and wait for 100 ms
    for (int i = 0; i < 20; i++) {
      for (int j = 0; j < 10; j++) {
        m.increment(10 * i + j);
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    assertEquals(150.0, m.getValue().get50(), 1.0);
  }

  @Test
  public void testSingleValue() {
    ApproxPercentileMetric m = new ApproxPercentileMetric("test", "test", 1000, 100);
    m.increment(1.0);
    assertEquals(1.0, m.getValue().get50(), 0.001);
  }
}
