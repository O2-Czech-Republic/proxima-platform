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

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ObserveHandleUtils {

  /**
   * Check that the given consumer is at current HEAD position in the {@link CommitLogReader}. HEAD
   * is defined as position where the size of backlog is zero (all elements have been consumed and
   * committed). Note that this state might change immediately after this call.
   *
   * @param handle {@link ObserveHandle} that belongs to a consumer
   * @param reader {@link CommitLogReader} that the consumer consumes from
   * @return {@code true} when the consumer is at HEAD position
   */
  public static boolean isAtHead(ObserveHandle handle, CommitLogReader reader) {
    List<Offset> committed = handle.getCommittedOffsets();
    Map<Partition, Offset> endOffsets =
        reader.fetchOffsets(
            Position.NEWEST,
            committed.stream().map(Offset::getPartition).collect(Collectors.toList()));
    return committed.stream().allMatch(o -> o.equals(endOffsets.get(o.getPartition())));
  }

  private ObserveHandleUtils() {
    // nop
  }
}
