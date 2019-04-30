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
    stamp = new AtomicLong(0L);
  }

  @Test
  public void testUninitialized() {
    WatermarkEstimator est = createEstimator();
    est.add(1);
    assertEquals(WatermarkEstimator.MIN_WATERMARK, est.getWatermark());
  }

  @Test
  public void testInitializedSameStamp() {
    WatermarkEstimator est = createEstimator();
    for (int i = 0; i < 3; i++) {
      assertEquals(WatermarkEstimator.MIN_WATERMARK, est.getWatermark());
      stamp.accumulateAndGet(250, (a, b) -> a + b);
      est.add(stamp.get());
    }
    assertEquals(550, est.getWatermark());
  }

  @Test
  public void testInitializedIncreasingStamp() {
    WatermarkEstimator est = createEstimator();
    testTimestampIncreaseInitializes(est);
  }

  @Test
  public void testCreateBacklog() {
    WatermarkEstimator est = createEstimator();
    testTimestampIncreaseInitializes(est);
    stamp.accumulateAndGet(250, (a, b) -> a + b);
    est.add(stamp.get() - 1000);
    assertEquals(stamp.get() - 450, est.getWatermark());
    testTimestampIncreaseInitializes(est);
  }

  private WatermarkEstimator testTimestampIncreaseInitializes(WatermarkEstimator est) {
    for (int i = 0; i < 10; i++) {
      stamp.accumulateAndGet(250, (a, b) -> a + b);
      est.add(stamp.get() - i * 5);
    }
    assertEquals(stamp.get() - 200, est.getWatermark());
    return est;
  }

  @Test
  public void testSingleUpdateIncresesStamp() {
    WatermarkEstimator est = createEstimator(1, 1);
    est.add(1);
    stamp.incrementAndGet();
    assertEquals(1 - 200, est.getWatermark());
  }

  @Test
  public void testSingleUpdateIncresesStamp2() {
    WatermarkEstimator est = createEstimator(1, 1, 100);
    est.add(1);
    stamp.incrementAndGet();
    assertEquals(1 - 100, est.getWatermark());
  }


  @Test
  public void testLargeUpdate() {
    WatermarkEstimator est = createEstimator(1, 1);
    stamp.set(10000);
    est.add(9999);
    stamp.set(20000L);
    est.add(20100L);
    est.add(20200L);
    assertEquals(19800L, est.getWatermark());
  }

  private WatermarkEstimator createEstimator() {
    return createEstimator(1000, 250);
  }

  private WatermarkEstimator createEstimator(long durationMs, long stepMs) {
    return createEstimator(durationMs, stepMs, 200);
  }

  private WatermarkEstimator createEstimator(long durationMs, long stepMs, long skew) {
    return WatermarkEstimator.newBuilder()
        .withDurationMs(durationMs)
        .withStepMs(stepMs)
        .withAllowedTimestampSkew(skew)
        .withTimestampSupplier(stamp::get)
        .build();
  }

}
