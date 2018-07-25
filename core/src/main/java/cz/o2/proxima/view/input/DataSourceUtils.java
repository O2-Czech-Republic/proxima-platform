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
package cz.o2.proxima.view.input;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import cz.seznam.euphoria.core.client.io.UnboundedPartition;
import cz.seznam.euphoria.core.client.io.UnboundedReader;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import javax.annotation.Nullable;

/**
 * Utils for creating and managing {@code DataSource}s.
 */
@Internal
@Slf4j
public class DataSourceUtils {

  private DataSourceUtils() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Function to be called to start the producer of data.
   */
  @FunctionalInterface
  public static interface Producer extends Serializable {

    /**
     * Run the producer.
     */
    void run();

  }

  /**
   * Create {@link UnboundedPartition} from {@link BlockingQueue}.
   * @param <T> data type to read
   * @param <OFF> type of the offset
   * @param queue the blocking queue to read
   * @param producer producer to run to start producing data
   * @param offsetProducer function that returns current offset
   * @param offsetReset function to reset offset and start reading from given offset
   * @return the single {@code UnboundedPartition}.
   *
   */
  public static <T, OFF extends Serializable> UnboundedPartition<T, List<OFF>>
      fromBlockingQueue(
          BlockingQueue<T> queue,
          Producer producer,
          Factory<List<OFF>> offsetProducer,
          Consumer<List<OFF>> offsetReset) {

    return () -> {
      producer.run();
      return new UnboundedReader<T, List<OFF>>() {

        @Nullable
        T next = null;

        @Override
        public List<OFF> getCurrentOffset() {
          return offsetProducer.apply();
        }

        @Override
        public void reset(List<OFF> offsets) {
          offsetReset.accept(offsets);
        }

        @Override
        public void commitOffset(List<OFF> offsets) {
          // nop
        }

        @Override
        public boolean hasNext() {
          try {
            if (next != null) {
              return true;
            }
            next = queue.take();
            return true;
          } catch (InterruptedException ex) {
            log.warn("Interrupted while waiting for next queue element.");
            Thread.currentThread().interrupt();
            return false;
          }
        }

        @Override
        public T next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          T current = next;
          next = null;
          return current;
        }

        @Override
        public void close() {
          // nop
        }
      };
    };
  }

  /**
   * Create {@code Dataset} with given partitions.
   * @param <T> datatype of the source
   * @param <OFF> type of offset
   * @param partitions array of partitions
   * @return {@link DataSource} consisting of given partitions
   */
  @SafeVarargs
  public static <T, OFF extends Serializable> DataSource<T> fromPartitions(
      UnboundedPartition<T, List<OFF>>... partitions) {

    return fromPartitions(Arrays.asList(partitions));
  }


  /**
   * Create {@code Dataset} with given partitions.
   * @param <T> datatype of the source
   * @param <OFF> type of offset
   * @param partitions list of partitions
   * @return {@link DataSource} consisting of given partitions
   */
  @SuppressWarnings("unchecked")
  public static <T, OFF extends Serializable> DataSource<T> fromPartitions(
      List<UnboundedPartition<T, List<OFF>>> partitions) {

    return (UnboundedDataSource) () -> partitions;
  }

}
