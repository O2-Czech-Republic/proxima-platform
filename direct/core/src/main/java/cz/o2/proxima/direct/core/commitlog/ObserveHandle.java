/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.commitlog;

import cz.o2.proxima.core.annotations.Stable;
import java.util.List;

/** A interface for handling progress and control consumption of running observe process. */
@Stable
public interface ObserveHandle extends AutoCloseable {

  /** Stop the consumption. */
  @Override
  void close();

  /**
   * Retrieve currently committed offsets.
   *
   * @return list of offsets that have been committed for each partition assigned to the observation
   */
  List<Offset> getCommittedOffsets();

  /**
   * Reset the consumption to given offsets.
   *
   * @param offsets the offsets to reset the processing to
   */
  void resetOffsets(List<Offset> offsets);

  /**
   * @return list of last processed element from each assigned partition.
   */
  List<Offset> getCurrentOffsets();

  /**
   * Wait until the consumer is ready to read data.
   *
   * @throws InterruptedException when interrupted before the wait is done
   */
  void waitUntilReady() throws InterruptedException;
}
