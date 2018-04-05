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
package cz.o2.proxima.storage.kafka;

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.storage.URIUtil;
import cz.o2.proxima.storage.commitlog.Offset;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Various utilities.
 */
class Utils {

  @FunctionalInterface
  interface ThrowingConsumer<T> {
    void apply(T what) throws Exception;
  }

  /**
   * Retrieve topic from given URI.
   * @param uri the URL
   * @return topic name
   */
  static String topic(URI uri) {
    String topic = URIUtil.getPathNormalized(uri);
    if (topic.isEmpty()) {
      throw new IllegalArgumentException("Invalid path in URI " + uri);
    }
    return topic;
  }

  static <T> Consumer<T> unchecked(ThrowingConsumer<T> wrap) {
    return t -> {
      try {
        wrap.apply(t);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  static void seekToCommitted(
      String topic,
      Collection<Offset> offsets,
      final KafkaConsumer<String, byte[]> consumer) {

    // seek to given offsets
    offsets.forEach(o -> {
      TopicOffset to = (TopicOffset) o;
      TopicPartition tp = new TopicPartition(topic, o.getPartition().getId());
      if (to.getOffset() >= 0) {
        consumer.seek(tp, to.getOffset() + 1);
      } else {
        consumer.seekToBeginning(Arrays.asList(tp));
      }
    });
  }


  private Utils() { }

}
