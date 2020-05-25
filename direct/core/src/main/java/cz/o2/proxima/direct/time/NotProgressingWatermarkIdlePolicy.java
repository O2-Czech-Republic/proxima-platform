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

import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import java.util.Map;

/** Idle policy doesn't progress watermark on idle. */
public class NotProgressingWatermarkIdlePolicy implements WatermarkIdlePolicy {
  private static final long serialVersionUID = 1L;
  private long watermark = Watermarks.MIN_WATERMARK;

  public static class Factory implements WatermarkIdlePolicyFactory {

    @Override
    public WatermarkIdlePolicy create(Map<String, Object> cfg) {
      return new NotProgressingWatermarkIdlePolicy();
    }
  }

  @Override
  public void idle(long currentWatermark) {
    this.watermark = Math.max(currentWatermark, watermark);
  }

  @Override
  public long getIdleWatermark() {
    return watermark;
  }
}
