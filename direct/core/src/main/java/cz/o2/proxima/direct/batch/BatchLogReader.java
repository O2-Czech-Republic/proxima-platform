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
package cz.o2.proxima.direct.batch;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Reader of batch data stored in batch storage. */
@Stable
public interface BatchLogReader {

  /** {@link Serializable} factory for {@link BatchLogReader}. */
  @FunctionalInterface
  interface Factory<T extends BatchLogReader> extends UnaryFunction<Repository, T> {}

  /**
   * Retrieve list of partitions of this batch observer.
   *
   * @return list of partitions of this reader
   */
  default List<Partition> getPartitions() {
    return getPartitions(Long.MIN_VALUE);
  }

  /**
   * Retrieve list of partitions covering data at least from given starting stamp.
   *
   * @param startStamp timestamp to start reading from
   * @return list of partitions covering the time range
   */
  default List<Partition> getPartitions(long startStamp) {
    return getPartitions(startStamp, Long.MAX_VALUE);
  }

  /**
   * Retrieve list of partitions covering data from the given range.
   *
   * @param startStamp starting timestamp (inclusive)
   * @param endStamp ending timestamp (exclusive)
   * @return list of partitions covering the time range
   */
  List<Partition> getPartitions(long startStamp, long endStamp);

  /**
   * Observe data stored in given partitions.
   *
   * @param partitions partitions to observe
   * @param attributes attribute descriptors to filter out
   * @param observer the observer by which to consume the data
   * @return an {@link ObserveHandle} to control the consumption
   */
  ObserveHandle observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer);

  /**
   * Observe data stored in given partitions for a given offsets.
   *
   * @param offsets offsets to observe
   * @param attributes attribute descriptors to filter out
   * @param observer the observer by which to consume the data
   * @return an {@link ObserveHandle} to control the consumption
   */
  default ObserveHandle observeOffsets(
      List<Offset> offsets, List<AttributeDescriptor<?>> attributes, BatchLogObserver observer) {
    final Map<Partition, Offset> partitions =
        offsets.stream().collect(Collectors.toMap(Offset::getPartition, Function.identity()));

    return observe(
        new ArrayList<>(partitions.keySet()),
        attributes,
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            final Offset startOffset = partitions.get(context.getPartition());
            if (startOffset.getElementIndex() <= context.getOffset().getElementIndex()) {
              observer.onNext(element);
            }
            // Skip processing.
            return true;
          }

          @Override
          public void onCompleted() {
            observer.onCompleted();
          }

          @Override
          public void onCancelled() {
            observer.onCancelled();
          }

          @Override
          public boolean onError(Throwable error) {
            return observer.onError(error);
          }
        });
  }

  /**
   * Convert instance of this reader to {@link Factory} suitable for serialization.
   *
   * @return the {@link Factory} representing this reader
   */
  Factory<?> asFactory();
}
