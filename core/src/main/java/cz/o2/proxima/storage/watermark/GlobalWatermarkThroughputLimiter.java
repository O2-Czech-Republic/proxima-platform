/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
import com.google.common.base.MoreObjects;
import cz.o2.proxima.storage.ThroughputLimiter;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link ThroughputLimiter} that synchronizes progress of global watermark among distributed
 * consumers.
 */
@Slf4j
public class GlobalWatermarkThroughputLimiter implements ThroughputLimiter {

  private static final long serialVersionUID = 1L;

  private static volatile GlobalWatermarkTracker singletonTracker;

  static final String TRACKER_CFG_PREFIX = "tracker.";
  static final String KW_CLASS = "class";

  /**
   * Configuration key for maximum amount of time that a reader can be ahead of a global watermark
   * in milliseconds.
   */
  public static final String MAX_AHEAD_TIME_MS_CFG = "max-watermark-ahead-ms";

  /**
   * Configuration key for number of milliseconds to pass between two updates of global watermark.
   */
  public static final String GLOBAL_WATERMARK_UPDATE_MS_CFG = "global-watermark-update-ms";

  /**
   * Configuration key for the amount of time to sleep when reader is too far ahead global watermark
   * in milliseconds.
   */
  public static final String DEFAULT_SLEEP_TIME_CFG = "default-sleep-time-ms";

  private long maxAheadTimeFromGlobalMs = Long.MAX_VALUE;
  private long globalWatermarkUpdatePeriodMs = Duration.ofMinutes(1).toMillis();

  @VisibleForTesting @Getter private GlobalWatermarkTracker tracker;

  private boolean closed = false;

  private transient String processName = UUID.randomUUID().toString();

  private long lastGlobalWatermarkUpdate = Long.MIN_VALUE;
  private long sleepTimeMs = globalWatermarkUpdatePeriodMs;

  @Override
  public void setup(Map<String, Object> cfg) {
    initializeTracker(cfg);
    this.tracker = singletonTracker;
    this.maxAheadTimeFromGlobalMs = getLong(cfg, MAX_AHEAD_TIME_MS_CFG, maxAheadTimeFromGlobalMs);
    this.globalWatermarkUpdatePeriodMs =
        getLong(cfg, GLOBAL_WATERMARK_UPDATE_MS_CFG, globalWatermarkUpdatePeriodMs);
    this.sleepTimeMs = getLong(cfg, DEFAULT_SLEEP_TIME_CFG, sleepTimeMs);
  }

  private void initializeTracker(Map<String, Object> cfg) {
    if (singletonTracker == null) {
      synchronized (GlobalWatermarkThroughputLimiter.class) {
        if (singletonTracker == null) {
          singletonTracker =
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
        }
        singletonTracker.setup(
            cfg.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(TRACKER_CFG_PREFIX))
                .map(e -> Pair.of(e.getKey().substring(TRACKER_CFG_PREFIX.length()), e.getValue()))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
      }
    }
  }

  private static Long getLong(Map<String, Object> cfg, String key, long defVal) {
    return Optional.ofNullable(cfg.get(key))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(defVal);
  }

  @Override
  public synchronized Duration getPauseTime(Context context) {
    if (!closed) {
      updateGlobalWatermarkIfNeeded(context);
      long globalWatermark = tracker.getGlobalWatermark(processName, context.getMinWatermark());
      if (globalWatermark < Watermarks.MAX_WATERMARK
          && globalWatermark + maxAheadTimeFromGlobalMs < context.getMinWatermark()) {
        log.info(
            "ThroughputLimiter {} pausing processing for {} ms on global watermark {} and context.minWatermark {}",
            this,
            sleepTimeMs,
            globalWatermark,
            context.getMinWatermark());
        return Duration.ofMillis(sleepTimeMs);
      }
    }
    return Duration.ZERO;
  }

  @Override
  public synchronized void close() {
    tracker.finished(processName);
    closed = true;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tracker", tracker)
        .add("maxAheadTimeFromGlobalMs", maxAheadTimeFromGlobalMs)
        .add("globalWatermarkUpdatePeriodMs", globalWatermarkUpdatePeriodMs)
        .add("processName", processName)
        .add("sleepTimeMs", sleepTimeMs)
        .toString();
  }

  private void updateGlobalWatermarkIfNeeded(Context context) {
    long now = System.currentTimeMillis();
    if (now - globalWatermarkUpdatePeriodMs > lastGlobalWatermarkUpdate) {
      CompletableFuture<Void> update = tracker.update(processName, context.getMinWatermark());
      if (lastGlobalWatermarkUpdate == Long.MIN_VALUE) {
        // when there was no update yet, wait for the update to happen
        ExceptionUtils.ignoringInterrupted(update::get);
      }
      lastGlobalWatermarkUpdate = now;
    }
  }

  @VisibleForTesting
  void forceUpdateGlobalWatermark() {
    lastGlobalWatermarkUpdate = Long.MIN_VALUE;
  }

  protected Object readResolve() {
    this.processName = UUID.randomUUID().toString();
    if (singletonTracker == null) {
      synchronized (GlobalWatermarkThroughputLimiter.class) {
        if (singletonTracker == null) {
          singletonTracker = this.tracker;
        }
      }
    } else {
      // enforce using singleton tracker
      this.tracker = singletonTracker;
    }
    return this;
  }
}
