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
package cz.o2.proxima.direct.kafka;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;

/** Limits poll throughput when reading from Kafka. */
class KafkaPollLimiter {
  private final long desiredBytesPerMs;

  public KafkaPollLimiter(long maxBytesPerSec) {
    this.desiredBytesPerMs =
        maxBytesPerSec < Long.MAX_VALUE ? Math.max(1L, maxBytesPerSec / 1000) : Long.MAX_VALUE;
  }

  public void limitPoll(long poolTimeMs, long bytesPolled) throws InterruptedException {
    if (isLimited()) {
      final long sleepTimeMs = getSleepTime(bytesPolled, poolTimeMs);
      if (sleepTimeMs > 0) {
        TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
      }
    }
  }

  private boolean isLimited() {
    return desiredBytesPerMs != Long.MAX_VALUE;
  }

  @VisibleForTesting
  public long getSleepTime(long bytesPolled, long pollTimeMs) {
    final long currentBytesPerMs = bytesPolled / Math.max(pollTimeMs, 1);
    if (currentBytesPerMs > desiredBytesPerMs) {
      return Math.max((currentBytesPerMs / desiredBytesPerMs * pollTimeMs) - pollTimeMs, 0);
    }
    return 0;
  }
}
