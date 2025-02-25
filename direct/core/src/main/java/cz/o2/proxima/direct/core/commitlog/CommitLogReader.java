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
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import java.io.Serializable;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Read access to commit log. The commit log is read by registering a (possible) named observer on
 * the stream. If the observer is named then if multiple registration exist with the same name, it
 * is automatically load balanced.
 */
@Stable
public interface CommitLogReader {

  /** {@link Serializable} factory for {@link CommitLogReader}. */
  @FunctionalInterface
  interface Factory<T extends CommitLogReader> extends UnaryFunction<Repository, T> {}

  /**
   * Retrieve URI representing this resource.
   *
   * @return URI representing this resource
   */
  URI getUri();

  /**
   * Retrieve list of partitions of this commit log.
   *
   * @return list of partitions of this reader
   */
  List<Partition> getPartitions();

  /**
   * Subscribe observer by name to the commit log. Each observer maintains its own position in the
   * commit log, so that the observers with different names do not interfere If multiple observers
   * share the same name, then the ingests are load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   *
   * @param name identifier of the consumer
   * @param position the position to seek for in the commit log
   * @param observer the observer to subscribe to the commit log
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  ObserveHandle observe(String name, Position position, CommitLogObserver observer);

  /**
   * Subscribe observer by name to the commit log and read the newest data. Each observer maintains
   * its own position in the commit log, so that the observers with different names do not interfere
   * If multiple observers share the same name, then the ingests are load-balanced between them (in
   * an undefined manner). This is a non blocking call.
   *
   * @param name identifier of the consumer
   * @param observer the observer to subscribe to the commit log
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observe(String name, CommitLogObserver observer) {
    return observe(name, Position.NEWEST, observer);
  }

  /**
   * Subscribe to given set of partitions. If you use this call then the reader stops being
   * automatically load balanced and the set of partitions can only be changed by call to this
   * method again.
   *
   * @param name name of the observer
   * @param partitions the list of partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param stopAtCurrent when {@code true} then stop the observer as soon as it reaches most recent
   *     record
   * @param observer the observer to subscribe to the partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  ObserveHandle observePartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer);

  /**
   * Subscribe to given set of partitions. If you use this call then the reader stops being
   * automatically load balanced and the set of partitions can only be changed by call to this
   * method again.
   *
   * @param partitions the list of partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param stopAtCurrent when {@code true} then stop the observer as soon as it reaches most recent
   *     record
   * @param observer the observer to subscribe to the partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observePartitions(
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    return observePartitions(
        "unnamed-proxima-bulk-consumer-" + UUID.randomUUID().toString(),
        partitions,
        position,
        stopAtCurrent,
        observer);
  }

  /**
   * Subscribe to given set of partitions. If you use this call then the reader stops being
   * automatically load balanced and the set of partitions can only be changed by call to this
   * method again.
   *
   * @param partitions the list of partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param observer the observer to subscribe to the partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observePartitions(
      Collection<Partition> partitions, Position position, CommitLogObserver observer) {

    return observePartitions(partitions, position, false, observer);
  }

  /**
   * Subscribe to given set of partitions and read newest data. If you use this call then the reader
   * stops being automatically load balanced and the set of partitions can only be changed by call
   * to this method again.
   *
   * @param partitions the partitions to subscribe to
   * @param observer the observer to subscribe to the given partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observePartitions(
      Collection<Partition> partitions, CommitLogObserver observer) {

    return observePartitions(partitions, Position.NEWEST, observer);
  }

  /**
   * Subscribe to the commit log in a bulk fashion. That implies that elements are not committed
   * one-by-one, but in a bulks, where all elements in a bulk are committed at once. This is useful
   * for micro-batching approach of data processing.
   *
   * @param name name of the observer
   * @param position the position to seek to in the partitions
   * @param stopAtCurrent when {@code true} then stop the observer as soon as it reaches most recent
   *     record
   * @param observer the observer to subscribe
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  ObserveHandle observeBulk(
      String name, Position position, boolean stopAtCurrent, CommitLogObserver observer);

  /**
   * Subscribe to the commit log in a bulk fashion. That implies that elements are not committed
   * one-by-one, but in a bulks, where all elements in a bulk are committed at once. This is useful
   * for micro-batching approach of data processing.
   *
   * @param name name of the observer
   * @param position the position to seek to in the partitions
   * @param observer the observer to subscribe
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observeBulk(String name, Position position, CommitLogObserver observer) {
    return observeBulk(name, position, false, observer);
  }

  /**
   * Subscribe to the commit log in a bulk fashion from newest data. That implies that elements are
   * not committed one-by-one, but in a bulks, where all elements in a bulk are committed at once.
   * This is useful for micro-batching approach of data processing.
   *
   * @param name name of the observer
   * @param observer the observer to subscribe
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observeBulk(String name, CommitLogObserver observer) {
    return observeBulk(name, Position.NEWEST, observer);
  }

  /**
   * Subscribe to given partitions in a bulk fashion.
   *
   * @param name name of the observer
   * @param partitions the partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param observer the observer to subscribe to the partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      CommitLogObserver observer) {

    return observeBulkPartitions(name, partitions, position, false, observer);
  }

  /**
   * Subscribe to given partitions in a bulk fashion.
   *
   * @param name name of the observer
   * @param partitions the partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param stopAtCurrent when {@code true} then stop the observer as soon as it reaches most recent
   *     record
   * @param observer the observer to subscribe to the partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer);

  /**
   * Subscribe to given partitions in a bulk fashion.
   *
   * @param partitions the partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param stopAtCurrent when {@code true} then stop the observer as soon as it reaches most recent
   *     record
   * @param observer the observer to subscribe to the partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observeBulkPartitions(
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    return observeBulkPartitions(
        "unnamed-proxima-bulk-consumer-" + UUID.randomUUID().toString(),
        partitions,
        position,
        stopAtCurrent,
        observer);
  }

  /**
   * Subscribe to given partitions in a bulk fashion.
   *
   * @param partitions the partitions to subscribe to
   * @param position the position to seek to in the partitions
   * @param observer the observer to subscribe to the partitions
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observeBulkPartitions(
      Collection<Partition> partitions, Position position, CommitLogObserver observer) {

    return observeBulkPartitions(partitions, position, false, observer);
  }

  /**
   * Consume from given offsets in a bulk fashion. A typical use-case for this type of consumption
   * is to first use {@link CommitLogReader#observeBulkPartitions}, observe for some time, than
   * interrupt the consumption, store associated offsets and resume the consumption from these
   * offsets later
   *
   * @param offsets the @{link Offset}s to subscribe to
   * @param observer the observer to subscribe to the offsets
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  default ObserveHandle observeBulkOffsets(Collection<Offset> offsets, CommitLogObserver observer) {
    return observeBulkOffsets(offsets, false, observer);
  }

  /**
   * Consume from given offsets in a bulk fashion. A typical use-case for this type of consumption
   * is to first use {@link CommitLogReader#observeBulkPartitions}, observe for some time, than
   * interrupt the consumption, store associated offsets and resume the consumption from these
   * offsets later
   *
   * @param offsets the @{link Offset}s to subscribe to
   * @param stopAtCurrent {@code true} if the processing should stop at current offsets
   * @param observer the observer to subscribe to the offsets
   * @return {@link ObserveHandle} to asynchronously cancel the observation
   */
  ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, boolean stopAtCurrent, CommitLogObserver observer);

  /**
   * Signals the user that offsets used by this reader can be externalized and reused later.
   *
   * @return {@code true} if {@link Offset}s of this reader are externalizable
   */
  default boolean hasExternalizableOffsets() {
    return false;
  }

  /**
   * Provides offset externalizer for serializing log specific offsets to external formats (bytes,
   * JSON).
   *
   * @return {@link OffsetExternalizer} when externalizable offsets are supported by this reader,
   *     check it by {@link #hasExternalizableOffsets}.
   */
  default OffsetExternalizer getOffsetExternalizer() {
    if (hasExternalizableOffsets()) {
      throw new IllegalStateException("Not implemented yet");
    }
    throw new UnsupportedOperationException("Offsets are not externalizable");
  }

  /**
   * Signals the user that this {@link CommitLogReader} is able to recover sequential IDs sent to it
   * in {@link StreamElement StreamElements} through its associated writer.
   *
   * @return {@code true} if StreamElement read from the commit log contain valid sequential IDs.
   */
  default boolean restoresSequentialIds() {
    return false;
  }

  /**
   * Fetch offsets at given position from given partitions.
   *
   * @param position position of the offsets
   * @param partitions partitions for which to fetch the offsets
   * @throws UnsupportedOperationException if {@link CommitLogReader} does not have externalizable
   *     offsets
   */
  default Map<Partition, Offset> fetchOffsets(Position position, List<Partition> partitions) {
    if (!hasExternalizableOffsets()) {
      throw new UnsupportedOperationException(
          getClass().getName()
              + " does not have externalizable offsets. Fetch offsets not supported.");
    }
    throw new IllegalStateException("Unimplemented. This is bug, please file an issue");
  }

  /**
   * Convert instance of this reader to {@link Factory} suitable for serialization.
   *
   * @return the {@link Factory} representing this reader
   */
  Factory<?> asFactory();
}
