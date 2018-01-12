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

  @FunctionalInterface
  public static interface Producer extends Serializable {

    void run();

  }

  /**
   * Create unbounded {@code DataSource} from {@code BlockingQueue}.
   * @param queue the blocking queue to read
   * @return the single partitioned {@code DataSource}.
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
   */
  @SafeVarargs
  public static <T, OFF extends Serializable> DataSource<T> fromPartitions(
      UnboundedPartition<T, OFF>... partitions) {

    return fromPartitions(Arrays.asList(partitions));
  }


  /**
   * Create {@code Dataset} with given partitions.
   */
  public static <T, OFF extends Serializable> DataSource<T> fromPartitions(
      List<UnboundedPartition<T, OFF>> partitions) {

    return (UnboundedDataSource<T, OFF>) () -> partitions;
  }

}
