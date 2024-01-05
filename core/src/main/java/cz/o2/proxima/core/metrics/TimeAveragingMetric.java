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
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Metric calculating average per time window. */
@Stable
public class TimeAveragingMetric extends ScalarMetric {

  private static final long serialVersionUID = 1L;

  private final long windowLengthMs;
  private final long checkpointMs;
  private final long purgeMs;

  // a cache that will purge too old values to
  // get consistent results after some reasonable time
  // store just increments in this map
  private final NavigableMap<Long, Double> checkpoints = new TreeMap<>();
  private double sumCheckpoints = 0.0;
  private long startMs = System.currentTimeMillis();
  private long lastCheckpoint = startMs;
  private double sum = 0.0;

  TimeAveragingMetric(
      String group, String name, long windowLengthMs, long checkpointMs, long purgeMs) {

    super(group, name);
    this.windowLengthMs = windowLengthMs;
    this.checkpointMs = checkpointMs;
    this.purgeMs = purgeMs;
  }

  @Override
  public synchronized void increment(double d) {
    storeCheckpoints(System.currentTimeMillis());
    sum += d;
  }

  @Override
  public synchronized Double getValue() {
    long now = System.currentTimeMillis();
    if (now - startMs < windowLengthMs) {
      return 0.0;
    }
    storeCheckpoints(now);
    applyCheckpoints(now);
    if (checkpoints.isEmpty()) {
      return 0.0;
    }
    return sumCheckpoints * windowLengthMs / (double) (lastCheckpoint - startMs);
  }

  @Override
  public synchronized void reset() {
    checkpoints.clear();
    sumCheckpoints = 0.0;
    startMs = System.currentTimeMillis();
    lastCheckpoint = startMs;
    sum = 0.0;
  }

  private void applyCheckpoints(long now) {
    long toRemove = now - purgeMs;
    if (toRemove > startMs) {
      NavigableMap<Long, Double> headMap = checkpoints.headMap(toRemove, true);
      if (!headMap.isEmpty()) {
        Map.Entry<Long, Double> lastEntry = headMap.lastEntry();
        new ArrayList<>(headMap.entrySet())
            .forEach(
                e -> {
                  sum -= e.getValue();
                  checkpoints.remove(e.getKey());
                  sumCheckpoints -= e.getValue();
                });
        startMs = lastEntry.getKey();
      }
    }
  }

  private void storeCheckpoints(long now) {
    while (checkpointMs + lastCheckpoint < now) {
      double checkpointValue = sum - sumCheckpoints;
      lastCheckpoint += checkpointMs;
      checkpoints.put(lastCheckpoint, checkpointValue);
      sumCheckpoints += checkpointValue;
    }
  }
}
