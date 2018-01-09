/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage;

import cz.o2.proxima.repository.Context;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.view.PartitionedView;
import java.io.Serializable;
import java.util.Optional;

/**
 * Interface providing various types of data access patterns to storage.
 */
public interface DataAccessor extends Serializable {

  /**
   * Retrieve writer (if applicable).
   */
  default Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve commit log reader (if applicable).
   */
  default Optional<CommitLogReader> getCommitLogReader(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve random access reader.
   */
  default Optional<RandomAccessReader> getRandomAccessReader(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve batch log observable.
   */
  default Optional<BatchLogObservable> getBatchLogObservable(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve partitioned view of the data.
   */
  default Optional<PartitionedView> getPartitionedView(Context context) {
    return Optional.empty();
  }

}
