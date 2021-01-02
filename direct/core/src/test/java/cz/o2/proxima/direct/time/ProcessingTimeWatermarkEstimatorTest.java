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

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

public class ProcessingTimeWatermarkEstimatorTest {
  AtomicLong stamp;

  @Before
  public void setUp() {
    stamp = new AtomicLong(0L);
  }

  @Test
  public void testGetWatermark() {
    ProcessingTimeWatermarkEstimator est = createEstimator();
    stamp.incrementAndGet();
    assertEquals(1, est.getWatermark());
    stamp.incrementAndGet();
    assertEquals(2, est.getWatermark());
  }

  private ProcessingTimeWatermarkEstimator createEstimator() {
    return ProcessingTimeWatermarkEstimator.newBuilder().withTimestampSupplier(stamp::get).build();
  }
}
