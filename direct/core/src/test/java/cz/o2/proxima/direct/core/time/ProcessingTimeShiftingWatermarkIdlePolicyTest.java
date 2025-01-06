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
package cz.o2.proxima.direct.core.time;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.time.WatermarkIdlePolicy;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.direct.core.time.ProcessingTimeShiftingWatermarkIdlePolicy.Factory;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class ProcessingTimeShiftingWatermarkIdlePolicyTest {

  public static class TimeFactory implements cz.o2.proxima.core.functional.Factory<Long> {

    private static final AtomicLong stamp = new AtomicLong(0);

    @Override
    public Long apply() {
      return stamp.get();
    }

    void add(long increment) {
      stamp.addAndGet(increment);
    }
  }

  @Test
  public void testIdlePolicy() {
    TimeFactory timeFactory = new TimeFactory();
    Factory factory = new Factory();
    factory.setup(
        ImmutableMap.of("watermark.processing-time-factory", TimeFactory.class.getName()));
    WatermarkIdlePolicy policy = factory.create();
    assertEquals(Watermarks.MIN_WATERMARK, policy.getIdleWatermark());
    policy.idle(1L);
    assertEquals(1L, policy.getIdleWatermark());
    timeFactory.add(100L);
    policy.update(/* ignored */ null);
    assertEquals(1L, policy.getIdleWatermark());
    policy.idle(2L);
    assertEquals(2L, policy.getIdleWatermark());
    timeFactory.add(200L);
    policy.idle(2L);
    assertEquals(202L, policy.getIdleWatermark());
  }
}
