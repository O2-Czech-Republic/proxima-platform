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
package cz.o2.proxima.direct.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.time.Instant;
import java.util.Collections;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;

public class NotProgressingWatermarkIdlePolicyTest {

  private NotProgressingWatermarkIdlePolicy policy;
  private long now;

  @Before
  public void setup() {
    now = Instant.now().toEpochMilli();
    policy = new NotProgressingWatermarkIdlePolicy();
  }

  @Test
  public void testGetIdleWatermark() {
    policy.idle(now);
    assertEquals(now, policy.getIdleWatermark());

    policy.idle(now + 1000L);
    assertEquals(now + 1000L, policy.getIdleWatermark());
  }

  @Test
  public void testGetIdleWatermarkMonotonicity() {
    final Random random = new Random(now);

    for (int i = 0; i < 100; i++) {
      long previousWatermark = policy.getIdleWatermark();
      policy.idle(random.nextLong());

      assertTrue(previousWatermark <= policy.getIdleWatermark());
    }
  }

  @Test
  public void testFactory() {
    WatermarkIdlePolicyFactory factory = new NotProgressingWatermarkIdlePolicy.Factory();
    WatermarkIdlePolicy policy = factory.create(Collections.emptyMap());
    assertNotNull(policy);
    assertEquals(NotProgressingWatermarkIdlePolicy.class, policy.getClass());
  }
}
