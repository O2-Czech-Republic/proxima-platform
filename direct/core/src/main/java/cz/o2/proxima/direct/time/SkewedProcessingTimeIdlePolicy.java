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

import static cz.o2.proxima.direct.time.WatermarkConfiguration.prefixedKey;

import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Policy returns skewed (delayed) processing time when source is idle. */
@Slf4j
public class SkewedProcessingTimeIdlePolicy implements WatermarkIdlePolicy {
  private static final long serialVersionUID = 1L;
  public static final String TIMESTAMP_SKEW = "timestamp-skew";
  public static final long DEFAULT_TIMESTAMP_SKEW = 100L;

  @Getter private final long timestampSkew;
  private final TimestampSupplier timestampSupplier;
  private long currentWatermark = Watermarks.MIN_WATERMARK;

  SkewedProcessingTimeIdlePolicy(long timestampSkew) {
    this(timestampSkew, System::currentTimeMillis);
  }

  SkewedProcessingTimeIdlePolicy(long timestampSkew, TimestampSupplier timestampSupplier) {
    this.timestampSkew = timestampSkew;
    this.timestampSupplier = timestampSupplier;
  }

  public static class Factory implements WatermarkIdlePolicyFactory {

    @Override
    public WatermarkIdlePolicy create(Map<String, Object> cfg) {
      long timestampSkew;

      // Check for legacy configuration outside watermark config
      if (cfg.containsKey(TIMESTAMP_SKEW)) {
        log.warn(
            String.format(
                "Legacy configuration being used '%s' prefer to use configuration '%s'",
                TIMESTAMP_SKEW, prefixedKey(TIMESTAMP_SKEW)));
        timestampSkew =
            Optional.ofNullable(cfg.get(TIMESTAMP_SKEW))
                .map(v -> Long.valueOf(v.toString()))
                .orElse(DEFAULT_TIMESTAMP_SKEW);

      } else {
        timestampSkew =
            Optional.ofNullable(cfg.get(prefixedKey(TIMESTAMP_SKEW)))
                .map(v -> Long.valueOf(v.toString()))
                .orElse(DEFAULT_TIMESTAMP_SKEW);
      }

      return new SkewedProcessingTimeIdlePolicy(timestampSkew);
    }
  }

  @Override
  public long getIdleWatermark() {
    return currentWatermark;
  }

  @Override
  public void idle(long currentWatermark) {
    this.currentWatermark = timestampSupplier.get() - timestampSkew;
  }
}
