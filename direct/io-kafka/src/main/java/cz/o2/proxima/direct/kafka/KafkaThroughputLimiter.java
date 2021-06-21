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

/** Limits throughput when reading data from Kafka. */
class KafkaThroughputLimiter {
  private final long desiredBytesPerSec;

  KafkaThroughputLimiter(long maxBytesPerSec) {
    this.desiredBytesPerSec =
        maxBytesPerSec < Long.MAX_VALUE ? Math.max(1L, maxBytesPerSec) : Long.MAX_VALUE;
  }

  /**
   * Checks poll throughput and sleeps when polled bytes exceed the max bytes per seconds limit.
   *
   * @param bytesPolled Number of bytes polled from Kafka.
   * @param pollTimeMs Duration of the poll.
   */
  public void sleepToLimitThroughput(long bytesPolled, long pollTimeMs)
      throws InterruptedException {
    if (desiredBytesPerSec < Long.MAX_VALUE) {
      final long sleepTimeMs = getSleepTime(bytesPolled, pollTimeMs);
      if (sleepTimeMs > 0) {
        TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
      }
    }
  }

  @VisibleForTesting
  long getSleepTime(long bytesPolled, long pollTimeMs) {
    final long currentBytesPerSec = bytesPolled * 1000 / Math.max(pollTimeMs, 1);
    if (currentBytesPerSec > desiredBytesPerSec) {
      return Math.max((currentBytesPerSec / desiredBytesPerSec * pollTimeMs) - pollTimeMs, 0);
    }
    return 0;
  }
}
