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

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.VoidFunction;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import cz.seznam.euphoria.core.client.io.UnboundedPartition;
import cz.seznam.euphoria.core.client.io.UnboundedReader;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Utils for creating and managing {@code DataSource}s.
 */
@Slf4j
public class DataSourceUtils {

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
   * Create unbounded {@code DataSource} from {@code BlockingQueue}.
   * @param <T> data type to read
   * @param <OFF> type of the offset
   * @param queue the blocking queue to read
   * @param producer producer to run to start producing data
   * @param offsetProducer function that returns current offset
   * @param offsetReset function to reset offset and start reading from given offset
   * @param commitOffset function by which to commit offset
   * @return the single {@code UnboundedPartition}.
   *
   */
  public static <T, OFF extends Serializable> UnboundedPartition<T, OFF> fromBlockingQueue(
      BlockingQueue<T> queue,
      Producer producer,
      VoidFunction<OFF> offsetProducer,
      UnaryFunction<OFF, Void> offsetReset,
      UnaryFunction<OFF, Void> commitOffset) {

    return () -> {
      producer.run();
      return new UnboundedReader<T, OFF>() {
        T next = null;

        @Override
        public OFF getCurrentOffset() {
          return offsetProducer.apply();
        }

        @Override
        public void reset(OFF offset) {
          offsetReset.apply(offset);
        }

        @Override
        public void commitOffset(OFF offset) {
          commitOffset.apply(offset);
        }

        @Override
        public boolean hasNext() {
          try {
            next = queue.take();
            return true;
          } catch (InterruptedException ex) {
            log.warn("Interrupted while waiting for next queue element.");
            return false;
          }
        }

        @Override
        public T next() {
          return next;
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
      UnboundedPartition<T, OFF>... partitions) {

    return fromPartitions(Arrays.asList(partitions));
  }


  /**
   * Create {@code Dataset} with given partitions.
   * @param <T> datatype of the source
   * @param <OFF> type of offset
   * @param partitions list of partitions
   * @return {@link DataSource} consisting of given partitions
   */
  public static <T, OFF extends Serializable> DataSource<T> fromPartitions(
      List<UnboundedPartition<T, OFF>> partitions) {

    return (UnboundedDataSource<T, OFF>) () -> partitions;
  }

}
