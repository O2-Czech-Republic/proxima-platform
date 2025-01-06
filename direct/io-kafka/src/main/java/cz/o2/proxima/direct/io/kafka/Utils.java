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
package cz.o2.proxima.direct.io.kafka;

import cz.o2.proxima.core.storage.UriUtil;
import cz.o2.proxima.direct.core.commitlog.Offset;
import java.net.URI;
import java.util.Collection;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/** Various utilities. */
class Utils {

  static final String TOPIC_PATTERN_QUERY = "topicPattern";

  /**
   * Retrieve topic from given URI.
   *
   * @param uri the URL
   * @return topic name
   */
  @Nullable
  static String topic(URI uri) {
    String topic = UriUtil.getPathNormalized(uri);
    if (topic.isEmpty()) {
      return null;
    }
    return topic;
  }

  @Nullable
  static String topicPattern(URI uri) {
    String pattern = UriUtil.parseQuery(uri).get(TOPIC_PATTERN_QUERY);
    if (pattern != null) {
      try {
        Pattern.compile(pattern);
      } catch (Exception ex) {
        throw new IllegalArgumentException(
            String.format("Cannot parse topic pattern %s from URI %s", pattern, uri), ex);
      }
    }
    return pattern;
  }

  static void seekToOffsets(
      Collection<? extends Offset> offsets, final KafkaConsumer<?, ?> consumer) {

    // seek to given offsets
    offsets.forEach(
        o -> {
          TopicOffset to = (TopicOffset) o;
          if (to.getOffset() >= 0) {
            consumer.seek(
                new org.apache.kafka.common.TopicPartition(
                    to.getPartition().getTopic(), to.getPartition().getPartition()),
                to.getOffset());
          }
        });
  }

  private Utils() {}
}
