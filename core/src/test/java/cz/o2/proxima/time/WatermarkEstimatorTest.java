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
package cz.o2.proxima.time;

import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link WatermarkEstimator}.
 */
public class WatermarkEstimatorTest {

  AtomicLong stamp;

  @Before
  public void setUp() {
    stamp = new AtomicLong(System.currentTimeMillis());
  }

  @Test
  public void testUninitialized() {
    WatermarkEstimator est = new WatermarkEstimator(1000, 250, stamp::get);
    est.add(1);
    assertEquals(Long.MIN_VALUE, est.getWatermark());
  }

  @Test
  public void testInitializedSameStamp() {
    WatermarkEstimator est = new WatermarkEstimator(1000, 250, stamp::get);
    for (int i = 0; i < 5; i++) {
      assertEquals(Long.MIN_VALUE, est.getWatermark());
      stamp.accumulateAndGet(i * 250, (a, b) -> a + b);
      est.add(1);
    }
    assertEquals(1, est.getWatermark());
  }

  @Test
  public void testInitializedIncreasingStamp() {
    WatermarkEstimator est = new WatermarkEstimator(1000, 250, stamp::get);
    for (int i = 0; i < 10; i++) {
      stamp.accumulateAndGet(i * 250, (a, b) -> a + b);
      est.add(i);
    }
    assertEquals(5, est.getWatermark());
  }


}
