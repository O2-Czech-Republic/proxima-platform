/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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

import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.time.WatermarkIdlePolicy;
import cz.o2.proxima.core.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.Classpath;
import java.util.Map;
import java.util.Optional;

/** Idle policy that shifts watermark by the amount of processing time elapsed in the idle state. */
public class ProcessingTimeShiftingWatermarkIdlePolicy implements WatermarkIdlePolicy {

  public static class Factory implements WatermarkIdlePolicyFactory {

    private cz.o2.proxima.core.functional.Factory<Long> timeFactory = System::currentTimeMillis;

    @Override
    public void setup(Map<String, Object> cfg) {
      @SuppressWarnings("unchecked")
      Optional<cz.o2.proxima.core.functional.Factory<Long>> provided =
          (Optional<cz.o2.proxima.core.functional.Factory<Long>>)
              Optional.ofNullable(cfg.get("watermark.processing-time-factory"))
                  .map(String.class::cast)
                  .flatMap(name -> Optional.ofNullable(Classpath.findClass(name, Object.class)))
                  .map(Classpath::newInstance);
      this.timeFactory = provided.orElse(timeFactory);
    }

    @Override
    public WatermarkIdlePolicy create() {
      return new ProcessingTimeShiftingWatermarkIdlePolicy(timeFactory);
    }
  }

  private final cz.o2.proxima.core.functional.Factory<Long> timeFactory;
  private long lastIdleProcessingTime = Watermarks.MIN_WATERMARK;
  private long lastIdleWatermark = Watermarks.MIN_WATERMARK;

  public ProcessingTimeShiftingWatermarkIdlePolicy() {
    this(System::currentTimeMillis);
  }

  protected ProcessingTimeShiftingWatermarkIdlePolicy(
      cz.o2.proxima.core.functional.Factory<Long> timeFactory) {

    this.timeFactory = timeFactory;
  }

  @Override
  public long getIdleWatermark() {
    return lastIdleWatermark;
  }

  @Override
  public void update(StreamElement element) {
    // disable idle watermark advance
    lastIdleProcessingTime = Watermarks.MIN_WATERMARK;
  }

  @Override
  public void idle(long currentWatermark) {
    long now = timeFactory.apply();
    if (lastIdleProcessingTime > 0) {
      long delta = now - lastIdleProcessingTime;
      lastIdleWatermark += delta;
    } else {
      lastIdleWatermark = currentWatermark;
    }
    lastIdleProcessingTime = now;
  }
}
