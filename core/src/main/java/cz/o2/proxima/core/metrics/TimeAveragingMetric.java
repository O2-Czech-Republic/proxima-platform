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
package cz.o2.proxima.core.metrics;

import cz.o2.proxima.core.annotations.Stable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Metric calculating average per time window. */
@Stable
public class TimeAveragingMetric extends ScalarMetric {

  private static final long serialVersionUID = 1L;

  public static TimeAveragingMetric of(
      String group, String name, long windowLengthMs, long checkpointMs, long purgeMs) {
    return new TimeAveragingMetric(group, name, windowLengthMs, checkpointMs, purgeMs);
  }

  public static TimeAveragingMetric of(String group, String name, long windowLengthMs) {
    // by default, checkpoint every window length and purge after thirty windows
    return of(group, name, windowLengthMs, windowLengthMs, 30 * windowLengthMs);
  }

  private final long windowLengthNanos;
  private final long checkpointNanos;
  private final long purgeNanos;

  // a cache that will purge too old values to
  // get consistent results after some reasonable time
  // store just increments in this map
  private final NavigableMap<Long, Double> checkpoints = new TreeMap<>();
  private double sumCheckpoints = 0.0;
  private long startNanos = System.nanoTime();
  private long lastCheckpoint = startNanos;
  private double sum = 0.0;

  TimeAveragingMetric(
      String group, String name, long windowLengthMs, long checkpointMs, long purgeMs) {

    super(group, name);
    this.windowLengthNanos = windowLengthMs * 1_000_000L;
    this.checkpointNanos = checkpointMs * 1_000_000L;
    this.purgeNanos = purgeMs * 1_000_000L;
  }

  @Override
  public synchronized void increment(double d) {
    storeCheckpoints(System.nanoTime());
    sum += d;
  }

  @Override
  public synchronized Double getValue() {
    long now = System.nanoTime();
    if (now - startNanos < windowLengthNanos) {
      return 0.0;
    }
    storeCheckpoints(now);
    applyCheckpoints(now);
    if (checkpoints.isEmpty()) {
      return 0.0;
    }
    return sumCheckpoints * windowLengthNanos / (double) (lastCheckpoint - startNanos);
  }

  @Override
  public synchronized void reset() {
    checkpoints.clear();
    sumCheckpoints = 0.0;
    startNanos = System.nanoTime();
    lastCheckpoint = startNanos;
    sum = 0.0;
  }

  private void applyCheckpoints(long now) {
    long toRemove = now - purgeNanos;
    if (toRemove > startNanos) {
      NavigableMap<Long, Double> headMap = checkpoints.headMap(toRemove, true);
      if (!headMap.isEmpty()) {
        Map.Entry<Long, Double> lastEntry = headMap.lastEntry();
        headMap.entrySet().stream()
            .collect(Collectors.toList())
            .forEach(
                e -> {
                  sum -= e.getValue();
                  checkpoints.remove(e.getKey());
                  sumCheckpoints -= e.getValue();
                });
        startNanos = lastEntry.getKey();
      }
    }
  }

  private void storeCheckpoints(long now) {
    while (checkpointNanos + lastCheckpoint < now) {
      double checkpointValue = sum - sumCheckpoints;
      lastCheckpoint += checkpointNanos;
      checkpoints.put(lastCheckpoint, checkpointValue);
      sumCheckpoints += checkpointValue;
    }
  }
}
