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

package cz.o2.proxima.storage.kafka;

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedLogObserver.Consumer;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * Various utilities.
 */
class Utils {

  /**
   * Retrieve topic from given URI.
   * @param uri the URL
   * @return topic name
   */
  static String topic(URI uri) {
    String topic = uri.getPath().substring(1);
    while (topic.endsWith("/")) {
      topic = topic.substring(0, topic.length());
    }
    if (topic.isEmpty()) {
      throw new IllegalArgumentException("Invalid path in URI " + uri);
    }
    return topic;
  }

  /**
   * Retrieve {@code ConsumerRebalanceListener} that notifies given {@code PartitionedLogObserver}.
   */
  static ConsumerRebalanceListener rebalanceListener(PartitionedLogObserver observer) {
    return new ConsumerRebalanceListener() {

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> clctn) {

      }

      @SuppressWarnings("unchecked")
      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        observer.onRepartition(partitions
            .stream()
            .map(tp -> (Partition) () -> tp.partition()).collect(Collectors.toList()));
      }

    };
  }

  /**
   * Retrieve a {@code LogObserver} that forwards elements to given {@code BlockingQueue}
   * via given {@code PartitionedLogObserver}.
   */
  static <T> LogObserver forwardingTo(
      BlockingQueue<T> queue, PartitionedLogObserver observer) {

    Consumer<T> consumer = e -> {
      try {
        queue.put(e);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    };
    return new LogObserver() {

      @Override
      public boolean onNext(
          StreamElement ingest, Partition partition,
          LogObserver.ConfirmCallback confirm) {

        observer.onNext(ingest, confirm::confirm, partition, consumer);
        confirm.confirm();
        return true;
      }

      @Override
      public void onError(Throwable error) {
        throw new RuntimeException(error);
      }

      @Override
      public void close() throws Exception {
      }


    };
  }



  private Utils() { }

}
