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
package cz.o2.proxima.beam.direct.io;

import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/** Various utilities for code sharing between SDF implementations. */
class SDFUtils {

  static Manual rangeCheckedManualEstimator(Instant initialWatermark) {
    return new Manual(ensureInBounds(initialWatermark)) {
      @Override
      public void setWatermark(Instant watermark) {
        super.setWatermark(ensureInBounds(watermark));
      }
    };
  }

  private static Instant ensureInBounds(Instant instant) {
    if (instant.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
    if (instant.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return instant;
  }

  private SDFUtils() {
    // nop
  }
}
