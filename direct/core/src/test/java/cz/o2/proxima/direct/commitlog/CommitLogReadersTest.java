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
package cz.o2.proxima.direct.commitlog;

import static org.mockito.Mockito.*;

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.ThroughputLimiter;
import cz.o2.proxima.storage.commitlog.Position;
import java.time.Duration;
import java.util.Arrays;
import org.junit.Test;

public class CommitLogReadersTest {
  private static class NoOpThroughputLimiter implements ThroughputLimiter {
    @Override
    public Duration getPauseTime(ThroughputLimiter.Context context) {
      return Duration.ZERO;
    }

    @Override
    public void close() {
      // no-op
    }
  }

  @Test
  public void verifyGetPartitionsIsNotCalledDuringConstruction() {
    final CommitLogReader mockReader = mock(CommitLogReader.class);
    // Get Partitions may not be supported in test environment, because it may issue remote
    // fetches...
    CommitLogReaders.withThroughputLimit(mockReader, new NoOpThroughputLimiter());
    verify(mockReader, never()).getPartitions();
  }

  @Test
  public void testLimitedReaderPartitionCaching() {
    final CommitLogReader mockReader = mock(CommitLogReader.class);
    doReturn(Arrays.asList(Partition.of(0), Partition.of(1))).when(mockReader).getPartitions();
    final CommitLogReader reader =
        CommitLogReaders.withThroughputLimit(mockReader, new NoOpThroughputLimiter());
    reader.observe("test", Position.OLDEST, mock(CommitLogObserver.class));
    reader.observe("test", Position.OLDEST, mock(CommitLogObserver.class));
    reader.observe("test", Position.OLDEST, mock(CommitLogObserver.class));
    verify(mockReader, times(1)).getPartitions();
  }
}
