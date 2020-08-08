/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.transforms;

import cz.o2.proxima.storage.StreamElement;
import java.time.Instant;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

/** Filter input {@link StreamElement} */
public class StreamElementFilter {

  private StreamElementFilter() {
    // no-op
  }

  /**
   * Filter {@link StreamElement} older or equal than provided timestamp
   *
   * @param timestamp timestamp for filtering
   * @return Filter transform
   */
  public static PTransform<PCollection<StreamElement>, PCollection<StreamElement>> fromTimestamp(
      long timestamp) {
    return new FilteredByFn(new FromTimestamp(timestamp));
  }

  /**
   * Filter {@link StreamElement} older or equal than provided Instant
   *
   * @param instant Instant for filtering
   * @return Filter transform
   */
  public static PTransform<PCollection<StreamElement>, PCollection<StreamElement>> fromTimestamp(
      Instant instant) {
    return fromTimestamp(instant.toEpochMilli());
  }

  /**
   * Filter {@link StreamElement} with lower timestamp
   *
   * @param timestamp timestamp for filtering
   * @return Filter transform
   */
  public static FilteredByFn untilTimestamp(long timestamp) {
    return new FilteredByFn(new UntilTimestamp(timestamp));
  }

  /**
   * Filter {@link StreamElement} with lower timestamp
   *
   * @param instant Instant for filtering
   * @return Filter transform
   */
  public static PTransform<PCollection<StreamElement>, PCollection<StreamElement>> untilTimestamp(
      Instant instant) {
    return untilTimestamp(instant.toEpochMilli());
  }

  private static class FilteredByFn
      extends PTransform<PCollection<StreamElement>, PCollection<StreamElement>> {
    private final SerializableFunction<StreamElement, Boolean> filterFn;

    public FilteredByFn(SerializableFunction<StreamElement, Boolean> filterFn) {
      this.filterFn = filterFn;
    }

    @Override
    public PCollection<StreamElement> expand(PCollection<StreamElement> input) {
      return input.apply(Filter.by(filterFn));
    }
  }

  /**
   * Filter predicates that accepts elements with timestamp greater or equal the given timestamp.
   */
  static class FromTimestamp implements SerializableFunction<StreamElement, Boolean> {

    private final long timestamp;

    FromTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public Boolean apply(StreamElement input) {
      if (input.getStamp() >= timestamp) {
        return true;
      }
      Metrics.counter("filter-timestamp", "from").inc();
      return false;
    }
  }

  /** Filter predicates that accepts elements with timestamp lower than given timestamp. */
  static class UntilTimestamp implements SerializableFunction<StreamElement, Boolean> {

    private final long timestamp;

    UntilTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public Boolean apply(StreamElement input) {
      if (input.getStamp() < timestamp) {
        return true;
      }
      Metrics.counter("filter-timestamp", "until").inc();
      return false;
    }
  }
}
