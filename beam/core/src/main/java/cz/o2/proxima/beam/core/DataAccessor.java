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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.internal.AbstractDataAccessor;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

/** A data accessor for attribute families. */
@Internal
public interface DataAccessor extends AbstractDataAccessor {

  /**
   * Create {@link PCollection} for given attribute family's commit log.
   *
   * @param name name of the consumer
   * @param pipeline pipeline to create {@link PCollection} in
   * @param position to read from
   * @param stopAtCurrent stop reading at current data
   * @param eventTime {@code true} to use event time
   * @param limit limit number of elements read. Note that the number of elements might be actually
   *     lower, because it is divided by number of partitions It is useful mostly for testing
   *     purposes
   * @return {@link PCollection} representing the commit log
   */
  PCollection<StreamElement> createStream(
      String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      long limit);

  /**
   * Create {@link PCollection} for given attribute family's batchUpdates. The created PCollection
   * is purposefully treated as unbounded (although it is bounded, in fact), which gives better
   * performance in cases when it is united with another unbounded {@link PCollection}.
   *
   * @param pipeline pipeline to create {@link PCollection} in
   * @param attrs attributes to read updates for
   * @param startStamp minimal update timestamp (inclusive)
   * @param endStamp maximal update timestamp (exclusive)
   * @param limit limit number of elements read. Note that the number of elements might be actually
   *     lower, because it is divided by number of partitions It is useful mostly for testing
   *     purposes
   * @return {@link PCollection} representing the commit log
   */
  PCollection<StreamElement> createStreamFromUpdates(
      Pipeline pipeline,
      List<AttributeDescriptor<?>> attrs,
      long startStamp,
      long endStamp,
      long limit);

  /**
   * Create {@link PCollection} for given attribute family's batch updates storage.
   *
   * @param pipeline pipeline to create {@link PCollection} in
   * @param attrs attributes to read
   * @param startStamp minimal update timestamp (inclusive)
   * @param endStamp maximal update timestamp (exclusive)
   * @return {@link PCollection} representing the batch updates
   */
  PCollection<StreamElement> createBatch(
      Pipeline pipeline, List<AttributeDescriptor<?>> attrs, long startStamp, long endStamp);
}
