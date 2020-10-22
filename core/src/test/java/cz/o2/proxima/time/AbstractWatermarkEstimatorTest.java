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
package cz.o2.proxima.time;

import cz.o2.proxima.storage.StreamElement;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class AbstractWatermarkEstimatorTest {

  @Test
  public void test() {
    final StreamElement element = Mockito.mock(StreamElement.class);
    final AtomicLong idleWatermark = new AtomicLong(0L);
    final WatermarkEstimator watermarkEstimator =
        new AbstractWatermarkEstimator(idleWatermark::get) {

          private long lastElementTimestamp = Long.MIN_VALUE;

          @Override
          public void setMinWatermark(long minWatermark) {
            throw new UnsupportedOperationException("Not implemented.");
          }

          @Override
          protected long estimateWatermark() {
            return lastElementTimestamp;
          }

          @Override
          protected void updateWatermark(StreamElement element) {
            lastElementTimestamp = element.getStamp();
          }
        };

    // Verify that we check, whether source is idle.
    Assert.assertEquals(Long.MIN_VALUE, watermarkEstimator.getWatermark());
    watermarkEstimator.idle();
    Assert.assertEquals(0L, watermarkEstimator.getWatermark());

    // Progress timestamp to 1.
    Mockito.when(element.getStamp()).thenReturn(1L);
    watermarkEstimator.update(element);
    Assert.assertEquals(1L, watermarkEstimator.getWatermark());

    // Progress timestamp to 2.
    Mockito.when(element.getStamp()).thenReturn(2L);
    watermarkEstimator.update(element);
    Assert.assertEquals(2L, watermarkEstimator.getWatermark());

    // Switch to idle state. Make sure we don't shift watermark back in time.
    watermarkEstimator.idle();
    Assert.assertEquals(2L, watermarkEstimator.getWatermark());

    // Progress idle policy to 3.
    idleWatermark.set(3L);
    Assert.assertEquals(3L, watermarkEstimator.getWatermark());

    // Back to running state. Make sure we don't shift watermark back in time.
    watermarkEstimator.update(element);
    Assert.assertEquals(3L, watermarkEstimator.getWatermark());

    // Progress watermark to 10.
    Mockito.when(element.getStamp()).thenReturn(10L);
    watermarkEstimator.update(element);
    Assert.assertEquals(10L, watermarkEstimator.getWatermark());

    // Now let's assume bug in estimator's implementation and have estimateWatermark to return 5.
    // Make sure we don't shift watermark back in time.
    Mockito.when(element.getStamp()).thenReturn(5L);
    watermarkEstimator.update(element);
    Assert.assertEquals(10L, watermarkEstimator.getWatermark());
  }
}
