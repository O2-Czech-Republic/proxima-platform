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
import static org.junit.Assert.assertNotSame;

import com.google.common.collect.ImmutableMap;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.ThroughputLimiter.Context;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link GlobalWatermarkThroughputLimiter}. */
public class GlobalWatermarkThroughputLimiterTest {

  private final GlobalWatermarkThroughputLimiter limiter = new GlobalWatermarkThroughputLimiter();

  public static class TestTracker implements GlobalWatermarkTracker {

    @Getter private final Map<String, Long> updates = new HashMap<>();
    @Setter private Instant globalWatermark = Instant.ofEpochMilli(Long.MIN_VALUE);
    @Getter private int testConf = -1;

    @Override
    public String getName() {
      return getClass().getSimpleName();
    }

    @Override
    public void setup(Map<String, Object> cfg) {
      testConf =
          Optional.ofNullable(cfg.get("test-tracker-conf"))
              .map(Object::toString)
              .map(Integer::parseInt)
              .orElse(-1);
    }

    @Override
    public void initWatermarks(Map<String, Long> initialWatermarks) {
      initialWatermarks.forEach(this::update);
    }

    @Override
    public CompletableFuture<Void> update(String processName, long currentWatermark) {
      updates.put(processName, currentWatermark);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getGlobalWatermark(@Nullable String processName, long currentWatermark) {
      return globalWatermark.toEpochMilli();
    }

    @Override
    public void close() {}
  }

  @Before
  public void setUp() {
    limiter.setup(cfg(TestTracker.class));
  }

  private Map<String, Object> cfg(Class<TestTracker> trackerClass) {
    return new ImmutableMap.Builder<String, Object>()
        .put(
            GlobalWatermarkThroughputLimiter.TRACKER_CFG_PREFIX
                + GlobalWatermarkThroughputLimiter.KW_CLASS,
            trackerClass.getName())
        .put(GlobalWatermarkThroughputLimiter.MAX_AHEAD_TIME_MS_CFG, 3600000)
        .put(GlobalWatermarkThroughputLimiter.GLOBAL_WATERMARK_UPDATE_MS_CFG, 100)
        .put(GlobalWatermarkThroughputLimiter.DEFAULT_SLEEP_TIME_CFG, 10)
        .build();
  }

  @Test
  public void testLimiter() {
    long watermark = Long.MIN_VALUE;
    TestTracker tracker = (TestTracker) limiter.getTracker();
    assertEquals(Duration.ZERO, limiter.getPauseTime(context(watermark)));
    Instant now = Instant.now();
    long nowMs = now.toEpochMilli();
    tracker.setGlobalWatermark(now);
    limiter.forceUpdateGlobalWatermark();
    assertEquals(Duration.ofMillis(10), limiter.getPauseTime(context(nowMs + 3600001)));
    assertEquals(Duration.ZERO, limiter.getPauseTime(context(nowMs + 3600000)));
  }

  @Test
  public void testLimiterSerializable() throws IOException, ClassNotFoundException {
    Object deserialized = TestUtils.deserializeObject(TestUtils.serializeObject(limiter));
    assertNotSame(limiter, deserialized);
  }

  private Context context(long watermark) {
    return new Context() {

      @Override
      public Collection<Partition> getConsumedPartitions() {
        return Collections.singleton(Partition.of(0));
      }

      @Override
      public long getMinWatermark() {
        return watermark;
      }
    };
  }
}
