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
package cz.o2.proxima.storage.watermark;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.storage.ThroughputLimiter;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * A {@link ThroughputLimiter} that synchronizes progress of global watermark among distributed
 * consumers.
 */
public class GlobalWatermarkThroughputLimiter implements ThroughputLimiter {

  private static final long serialVersionUID = 1L;

  static final String TRACKER_CFG_PREFIX = "tracker.";
  static final String KW_CLASS = "class";
  static final String MAX_AHEAD_TIME_MS_CFG = "max-watermark-ahead-ms";
  static final String GLOBAL_WATERMARK_UPDATE_MS_CFG = "global-watermark-update-ms";
  static final String DEFAULT_SLEEP_TIME_CFG = "default-sleep-time-ms";

  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  GlobalWatermarkTracker tracker;

  private long maxAheadTimeFromGlobalMs = Long.MAX_VALUE;
  private long globalWatermarkUpdatePeriodMs = Duration.ofMinutes(1).toMillis();
  private transient String processName = UUID.randomUUID().toString();

  private long lastGlobalWatermarkUpdate = Long.MIN_VALUE;
  private long globalWatermark = Long.MIN_VALUE;
  private long defaultSleepTimeMs = globalWatermarkUpdatePeriodMs;

  @Override
  public void setup(Map<String, Object> cfg) {
    this.tracker =
        Optional.ofNullable(cfg.get(TRACKER_CFG_PREFIX + KW_CLASS))
            .map(Object::toString)
            .map(
                cls ->
                    ExceptionUtils.uncheckedFactory(
                        () -> Classpath.newInstance(cls, GlobalWatermarkTracker.class)))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Missing %s%s specifying GlobalWatermarkTracker implementation",
                            TRACKER_CFG_PREFIX, KW_CLASS)));
    this.tracker.setup(
        cfg.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith(TRACKER_CFG_PREFIX))
            .map(e -> Pair.of(e.getKey().substring(TRACKER_CFG_PREFIX.length()), e.getValue()))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
    this.maxAheadTimeFromGlobalMs = getLong(cfg, MAX_AHEAD_TIME_MS_CFG, maxAheadTimeFromGlobalMs);
    this.globalWatermarkUpdatePeriodMs =
        getLong(cfg, GLOBAL_WATERMARK_UPDATE_MS_CFG, globalWatermarkUpdatePeriodMs);
    this.defaultSleepTimeMs = getLong(cfg, DEFAULT_SLEEP_TIME_CFG, defaultSleepTimeMs);
  }

  private Long getLong(Map<String, Object> cfg, String key, long defVal) {
    return Optional.ofNullable(cfg.get(key))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(defVal);
  }

  @Override
  public Duration getPauseTime(Context context) {
    updateGlobalWatermarkIfNeeded(context);
    if (globalWatermark + maxAheadTimeFromGlobalMs < context.getMinWatermark()) {
      return Duration.ofMillis(defaultSleepTimeMs);
    }
    return Duration.ZERO;
  }

  private void updateGlobalWatermarkIfNeeded(Context context) {
    long now = System.currentTimeMillis();
    if (now - globalWatermarkUpdatePeriodMs > lastGlobalWatermarkUpdate) {
      tracker.update(processName, Instant.ofEpochMilli(context.getMinWatermark()));
      globalWatermark = tracker.getWatermark();
      lastGlobalWatermarkUpdate = now;
    }
  }

  @VisibleForTesting
  void forceUpdateGlobalWatermark() {
    lastGlobalWatermarkUpdate = 0;
  }

  protected Object readResolve() {
    this.processName = UUID.randomUUID().toString();
    return this;
  }
}
