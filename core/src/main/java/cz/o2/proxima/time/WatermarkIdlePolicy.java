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
package cz.o2.proxima.time;

import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;

/** Policy defines behaviour how watermark should behave when streaming source is idle */
public interface WatermarkIdlePolicy extends Serializable {

  /**
   * Returns watermark for idle source.
   *
   * @return the watermark.
   */
  long getIdleWatermark();

  /**
   * Updates policy state when a new element is received from streaming source.
   *
   * @param element a stream element.
   */
  default void update(StreamElement element) {}

  /**
   * Signals that a source is idle.
   *
   * @param currentWatermark the most recent watermark estimated by watermark estimator {@link
   *     WatermarkEstimator}.
   */
  default void idle(long currentWatermark) {}
}
