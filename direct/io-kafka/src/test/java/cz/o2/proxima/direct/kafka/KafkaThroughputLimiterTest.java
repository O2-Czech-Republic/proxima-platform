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
package cz.o2.proxima.direct.kafka;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KafkaThroughputLimiterTest {
  final KafkaThroughputLimiter limiter = new KafkaThroughputLimiter(1_000_000L);

  @Test
  public void testGetSleepTimeWhenNothingPolled() {
    assertEquals(0, limiter.getSleepTime(0, 0L));
  }

  @Test
  public void testGetSleepTimeWhenPolledBytesBellowLimit() {
    assertEquals(0, limiter.getSleepTime(1_000L, 100L));
  }

  @Test
  public void testGetSleepTimeWhenPolledBytesMatchLimit() {
    assertEquals(0, limiter.getSleepTime(100_000L, 100L));
  }

  @Test
  public void testGetSleepTimeWhenPolledBytesExceedLimit() {
    assertEquals(100L, limiter.getSleepTime(200_000L, 100L));
  }

  @Test
  public void testGetSleepTimeWhenFastPoll() {
    assertEquals(0L, limiter.getSleepTime(1_000L, 0L));
  }

  @Test
  public void testGetSleepTimeWhenSlowPoll() {
    assertEquals(0L, limiter.getSleepTime(1_000_000L, 10_000L));
    assertEquals(0L, limiter.getSleepTime(10_000_000L, 10_000L));
    assertEquals(10_000L, limiter.getSleepTime(20_000_000L, 10_000L));
  }
}
