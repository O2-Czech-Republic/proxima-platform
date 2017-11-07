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
package cz.o2.proxima.view.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for creating and managing {@code DataSource}s.
 */
public class DataSourceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DataSourceUtils.class);

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
  public static <T> Partition<T> fromBlockingQueue(
      BlockingQueue<T> queue,
      Producer producer) {

    return new Partition<T>() {
      @Override
      public Set<String> getLocations() {
        return Collections.singleton("local");
      }

      @Override
      public Reader<T> openReader() throws IOException {
        producer.run();
        return new Reader<T>() {

          T next = null;

          @Override
          public void close() throws IOException {
            // nop
          }

          @Override
          public boolean hasNext() {
            try {
              next = queue.take();
              return true;
            } catch (InterruptedException ex) {
              LOG.warn("Interrupted while waiting for next queue element.");
              return false;
            }
          }

          @Override
          public T next() {
            return next;
          }
        };
      }
    };
  }

  /**
   * Create {@code Dataset} with given partitions.
   */
  @SafeVarargs
  public static <T> DataSource<T> fromPartitions(Partition<T>... partitions) {
    return fromPartitions(Arrays.asList(partitions));
  }


  /**
   * Create {@code Dataset} with given partitions.
   */
  public static <T> DataSource<T> fromPartitions(List<Partition<T>> partitions) {
    return new DataSource<T>() {
      @Override
      public List<Partition<T>> getPartitions() {
        return partitions;
      }

      @Override
      public boolean isBounded() {
        return false;
      }

    };
  }

}
