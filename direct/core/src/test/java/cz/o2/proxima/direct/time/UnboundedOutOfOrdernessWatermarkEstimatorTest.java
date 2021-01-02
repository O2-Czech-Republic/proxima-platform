/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.time;

import static cz.o2.proxima.direct.time.UnboundedOutOfOrdernessWatermarkEstimator.ALLOWED_TIMESTAMP_SKEW;
import static cz.o2.proxima.direct.time.UnboundedOutOfOrdernessWatermarkEstimator.ESTIMATE_DURATION_MS;
import static cz.o2.proxima.direct.time.UnboundedOutOfOrdernessWatermarkEstimator.STEP_MS;
import static cz.o2.proxima.direct.time.WatermarkConfiguration.prefixedKey;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

/** Test {@link UnboundedOutOfOrdernessWatermarkEstimator}. */
public class UnboundedOutOfOrdernessWatermarkEstimatorTest {

  AtomicLong stamp;

  @Before
  public void setUp() {
    stamp = new AtomicLong(0L);
  }

  @Test
  public void testUninitialized() {
    UnboundedOutOfOrdernessWatermarkEstimator est = createEstimator();
    est.add(1);
    assertEquals(
        UnboundedOutOfOrdernessWatermarkEstimator.DEFAULT_MIN_WATERMARK, est.getWatermark());
  }

  @Test
  public void testInitializedSameStamp() {
    UnboundedOutOfOrdernessWatermarkEstimator est = createEstimator();
    for (int i = 0; i < 3; i++) {
      assertEquals(
          UnboundedOutOfOrdernessWatermarkEstimator.DEFAULT_MIN_WATERMARK, est.getWatermark());
      stamp.accumulateAndGet(250, (a, b) -> a + b);
      est.add(stamp.get());
    }
    assertEquals(550, est.getWatermark());
  }

  @Test
  public void testInitializedIncreasingStamp() {
    UnboundedOutOfOrdernessWatermarkEstimator est = createEstimator();
    testTimestampIncreaseInitializes(est);
  }

  @Test
  public void testCreateBacklog() {
    UnboundedOutOfOrdernessWatermarkEstimator est = createEstimator();
    testTimestampIncreaseInitializes(est);
    stamp.accumulateAndGet(250, (a, b) -> a + b);
    est.add(stamp.get() - 1000);
    assertEquals(stamp.get() - 450, est.getWatermark());
    testTimestampIncreaseInitializes(est);
  }

  private UnboundedOutOfOrdernessWatermarkEstimator testTimestampIncreaseInitializes(
      UnboundedOutOfOrdernessWatermarkEstimator est) {
    for (int i = 0; i < 10; i++) {
      stamp.accumulateAndGet(250, (a, b) -> a + b);
      est.add(stamp.get() - i * 5);
    }
    assertEquals(stamp.get() - 200, est.getWatermark());
    return est;
  }

  @Test
  public void testSingleUpdateIncresesStamp() {
    UnboundedOutOfOrdernessWatermarkEstimator est = createEstimator(1, 1);
    est.add(1);
    stamp.incrementAndGet();
    assertEquals(1 - 200, est.getWatermark());
  }

  @Test
  public void testSingleUpdateIncresesStamp2() {
    UnboundedOutOfOrdernessWatermarkEstimator est = createEstimator(1, 1, 100);
    est.add(1);
    stamp.incrementAndGet();
    assertEquals(1 - 100, est.getWatermark());
  }

  @Test
  public void testLargeUpdate() {
    UnboundedOutOfOrdernessWatermarkEstimator est = createEstimator(1, 1);
    stamp.set(10000);
    est.add(9999);
    stamp.set(20000L);
    est.add(20100L);
    est.add(20200L);
    assertEquals(19800L, est.getWatermark());
  }

  @Test
  public void testFactory() {
    long estimateDurationMs = 100L;
    long stepMs = 10L;
    long allowedTimestampSkew = 200L;

    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put(prefixedKey(ESTIMATE_DURATION_MS), estimateDurationMs);
            put(prefixedKey(STEP_MS), stepMs);
            put(prefixedKey(ALLOWED_TIMESTAMP_SKEW), allowedTimestampSkew);
          }
        };

    WatermarkIdlePolicyFactory idlePolicyFactory = mock(WatermarkIdlePolicyFactory.class);
    when(idlePolicyFactory.create(cfg)).thenReturn(mock(WatermarkIdlePolicy.class));

    WatermarkEstimatorFactory factory = new UnboundedOutOfOrdernessWatermarkEstimator.Factory();
    UnboundedOutOfOrdernessWatermarkEstimator watermarkEstimator =
        (UnboundedOutOfOrdernessWatermarkEstimator) factory.create(cfg, idlePolicyFactory);

    assertEquals(estimateDurationMs, watermarkEstimator.getEstimateDurationMs());
    assertEquals(stepMs, watermarkEstimator.getStepMs());
    assertEquals(allowedTimestampSkew, watermarkEstimator.getAllowedTimestampSkew());
    verify(idlePolicyFactory, times(1)).create(cfg);
  }

  private UnboundedOutOfOrdernessWatermarkEstimator createEstimator() {
    return createEstimator(1000, 250);
  }

  private UnboundedOutOfOrdernessWatermarkEstimator createEstimator(long durationMs, long stepMs) {
    return createEstimator(durationMs, stepMs, 200);
  }

  private UnboundedOutOfOrdernessWatermarkEstimator createEstimator(
      long durationMs, long stepMs, long skew) {
    return UnboundedOutOfOrdernessWatermarkEstimator.newBuilder()
        .withDurationMs(durationMs)
        .withStepMs(stepMs)
        .withAllowedTimestampSkew(skew)
        .withTimestampSupplier(stamp::get)
        .build();
  }
}
