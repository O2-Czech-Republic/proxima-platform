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

import com.google.common.base.Preconditions;
import cz.o2.proxima.storage.StreamElement;

/** The watermark estimator base class. */
public abstract class AbstractWatermarkEstimator implements WatermarkEstimator {
  /** Used idle policy */
  private final WatermarkIdlePolicy idlePolicy;

  /** If estimator is in the idle state */
  private boolean isIdle = false;

  private long lastWatermark = Watermarks.MIN_WATERMARK;

  protected AbstractWatermarkEstimator(WatermarkIdlePolicy idlePolicy) {
    Preconditions.checkNotNull(idlePolicy, "Idle policy must be provided");
    this.idlePolicy = idlePolicy;
  }

  /**
   * Estimates current watermark.
   *
   * @return the estimated watermark.
   */
  protected abstract long estimateWatermark();

  /**
   * Updates the watermark estimate according to given stream element.
   *
   * @param element a stream element.
   */
  protected abstract void updateWatermark(StreamElement element);

  /** Signals that streaming source is idle. */
  @Override
  public void idle() {
    isIdle = true;
    idlePolicy.idle(getWatermark());
  }

  /**
   * Updates the watermark estimate according to the given stream element.
   *
   * @param element a stream element.
   */
  @Override
  public final void update(StreamElement element) {
    isIdle = false;
    idlePolicy.update(element);
    updateWatermark(element);
  }

  /**
   * Returns monotonically increasing estimated watermark.
   *
   * @return the watermark estimate.
   */
  @Override
  public long getWatermark() {
    final long watermark =
        isIdle ? Math.max(estimateWatermark(), idlePolicy.getIdleWatermark()) : estimateWatermark();
    if (watermark < lastWatermark) {
      return lastWatermark;
    }
    lastWatermark = watermark;
    return watermark;
  }
}
