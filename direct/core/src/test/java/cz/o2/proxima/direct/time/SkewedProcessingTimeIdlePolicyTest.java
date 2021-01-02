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

import static cz.o2.proxima.direct.time.SkewedProcessingTimeIdlePolicy.DEFAULT_TIMESTAMP_SKEW;
import static cz.o2.proxima.direct.time.WatermarkConfiguration.prefixedKey;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;

public class SkewedProcessingTimeIdlePolicyTest {

  private SkewedProcessingTimeIdlePolicy policy;
  private long now;
  private final long TIMESTAMP_SKEW = 1000L;
  private TimestampSupplier timestampSupplier;

  @Before
  public void setup() {
    now = Instant.now().toEpochMilli();
    timestampSupplier = mock(TimestampSupplier.class);
    policy = new SkewedProcessingTimeIdlePolicy(TIMESTAMP_SKEW, timestampSupplier);
  }

  @Test
  public void testGetIdleWatermark() {
    when(timestampSupplier.get()).thenReturn(10_000L);
    policy.idle(1000L);
    assertEquals(9_000L, policy.getIdleWatermark());

    when(timestampSupplier.get()).thenReturn(11_000L);
    policy.idle(2000L);
    assertEquals(10_000L, policy.getIdleWatermark());

    verify(timestampSupplier, times(2)).get();
  }

  @Test
  public void testGetIdleWatermarkMonotonicity() {
    final Random random = new Random(now);
    final SkewedProcessingTimeIdlePolicy policy =
        new SkewedProcessingTimeIdlePolicy(TIMESTAMP_SKEW, System::currentTimeMillis);

    for (int i = 0; i < 100; i++) {
      long previousWatermark = policy.getIdleWatermark();
      policy.idle(random.nextLong());

      assertTrue(previousWatermark <= policy.getIdleWatermark());
    }
  }

  @Test
  public void testFactory() {
    WatermarkIdlePolicyFactory factory = new SkewedProcessingTimeIdlePolicy.Factory();
    SkewedProcessingTimeIdlePolicy policy =
        (SkewedProcessingTimeIdlePolicy) factory.create(Collections.emptyMap());
    assertNotNull(policy);
    assertEquals(DEFAULT_TIMESTAMP_SKEW, policy.getTimestampSkew());
  }

  @Test
  public void testFactoryConfig() {
    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put(prefixedKey(SkewedProcessingTimeIdlePolicy.TIMESTAMP_SKEW), TIMESTAMP_SKEW);
          }
        };

    WatermarkIdlePolicyFactory factory = new SkewedProcessingTimeIdlePolicy.Factory();
    SkewedProcessingTimeIdlePolicy policy = (SkewedProcessingTimeIdlePolicy) factory.create(cfg);
    assertEquals(TIMESTAMP_SKEW, policy.getTimestampSkew());
  }

  @Test
  public void testFactoryLegacyConfig() {
    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put(SkewedProcessingTimeIdlePolicy.TIMESTAMP_SKEW, TIMESTAMP_SKEW);
          }
        };

    WatermarkIdlePolicyFactory factory = new SkewedProcessingTimeIdlePolicy.Factory();
    SkewedProcessingTimeIdlePolicy policy = (SkewedProcessingTimeIdlePolicy) factory.create(cfg);
    assertEquals(TIMESTAMP_SKEW, policy.getTimestampSkew());
  }

  @Test
  public void testFactoryLegacyAndNewConfig() {
    long newTimestampSkew = 20L;
    long legacyTimestampSkew = 10L;

    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put(prefixedKey(SkewedProcessingTimeIdlePolicy.TIMESTAMP_SKEW), newTimestampSkew);
            put(SkewedProcessingTimeIdlePolicy.TIMESTAMP_SKEW, legacyTimestampSkew);
          }
        };

    WatermarkIdlePolicyFactory factory = new SkewedProcessingTimeIdlePolicy.Factory();
    SkewedProcessingTimeIdlePolicy policy = (SkewedProcessingTimeIdlePolicy) factory.create(cfg);
    // Legacy config has higher priority
    assertEquals(legacyTimestampSkew, policy.getTimestampSkew());
  }
}
