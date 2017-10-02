/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.storage.commitlog;

import cz.o2.proxima.storage.Partition;
import java.io.Closeable;
import java.net.URI;
import java.util.Collection;
import java.util.List;

/**
 * Read access to commit log.
 * The commit log is read by registering a (possible) named
 * observer on the stream. If the observer is named then if multiple registration
 * exist with the same name, it is automatically load balanced.
 */
public interface CommitLogReader extends Closeable {

  /**
   * An enum specifying the position in the commit log to start reading from.
   */
  public enum Position {

    /** Read the commit log from the current data actually pushed to the log. */
    NEWEST,

    /** Read the commit log from the oldest data available. */
    OLDEST

  }

  /**
   * Retrieve URI representing this resource.
   */
  URI getURI();

  /**
   * Retrieve list of partitions of this commit log.
   */
  List<Partition> getPartitions();


  /**
   * Subscribe observer by name to the commit log.
   * Each observer maintains its own position in the commit log, so that
   * the observers with different names do not interfere
   * If multiple observers share the same name, then the ingests
   * are load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   * @param name identifier of the consumer
   * @param position the position to seek for in the commit log
   * @param observer the observer to subscribe to the commit log
   */
  Cancellable observe(String name, Position position, LogObserver observer);


  /**
   * Subscribe observer by name to the commit log and read the newest data.
   * Each observer maintains its own position in the commit log, so that
   * the observers with different names do not interfere
   * If multiple observers share the same name, then the ingests
   * are load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   * @param name identifier of the consumer
   * @param observer the observer to subscribe to the commit log
   */
  default Cancellable observe(String name, LogObserver observer) {
    return observe(name, Position.NEWEST, observer);
  }


  /**
   * Subscribe to given set of partitions.
   * If you use this call then the reader stops being automatically
   * load balanced and the set of partitions can only be changed
   * by call to this method again.
   * @param partitions the list of partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param stopAtCurrent when {@code true} then stop the observer as soon
   *                      as it reaches most recent record
   * @param observer the observer to subscribe to the partitions
   */
  Cancellable observePartitions(
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer);


  /**
   * Subscribe to given set of partitions.
   * If you use this call then the reader stops being automatically
   * load balanced and the set of partitions can only be changed
   * by call to this method again.
   * @param partitions the list of partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param observer the observer to subscribe to the partitions
   */
  default Cancellable observePartitions(
      Collection<Partition> partitions,
      Position position,
      LogObserver observer) {

    return observePartitions(partitions, position, false, observer);
  }


  /**
   * Subscribe to given set of partitions and read newest data.
   * If you use this call then the reader stops being automatically
   * load balanced and the set of partitions can only be changed
   * by call to this method again.
   * @param partitions the partitions to subscribe to
   * @param observer the observer to subscribe to the given partitions
   */
  default Cancellable observePartitions(
      List<Partition> partitions,
      LogObserver observer) {

    return observePartitions(partitions, Position.NEWEST, observer);
  }


  /**
   * Subscribe to the commitlog in a bulk fashion. That implies that elements
   * are not committed one-by-one, but in a bulks, where all elements in a bulk
   * are committed at once. This is useful for micro-batching approach of
   * data processing.
   */
  Cancellable observeBulk(
      String name,
      Position position,
      BulkLogObserver observer);



}
