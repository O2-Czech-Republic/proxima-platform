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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.junit.Test;

/** Test invariants of {@link GlobalWatermarkTracker}. */
public class GlobalWatermarkTrackerTest {

  private static class Tracker implements GlobalWatermarkTracker {

    Map<String, Long> updated = new HashMap<>();

    @Override
    public String getName() {
      return "name";
    }

    @Override
    public void setup(Map<String, Object> cfg) {}

    @Override
    public void initWatermarks(Map<String, Long> initialWatermarks) {}

    @Override
    public CompletableFuture<Void> update(String processName, long currentWatermark) {
      updated.put(processName, currentWatermark);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getGlobalWatermark(@Nullable String processName, long currentWatermark) {
      if (processName != null) {
        updated.compute(
            processName, (k, v) -> Math.max(v == null ? Long.MIN_VALUE : v, currentWatermark));
      }
      return updated.values().stream().mapToLong(e -> e).min().orElse(Long.MIN_VALUE);
    }

    @Override
    public void close() {}
  }

  @Test
  public void testInvariants() {
    Tracker tracker = new Tracker();
    tracker.update("name", 0);
    assertEquals(0, tracker.getWatermark());
    tracker.finished("name");
    assertEquals(Long.MAX_VALUE, tracker.getWatermark());
  }
}
