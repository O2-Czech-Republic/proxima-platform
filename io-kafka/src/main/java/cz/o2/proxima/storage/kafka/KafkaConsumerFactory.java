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

import cz.o2.proxima.storage.Partition;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Factory for {@code KafkaConsumer}s attached to the given commit log.
 */
@Slf4j
public class KafkaConsumerFactory {

  /** URI of the log. */
  private final URI uri;

  /** Our assigned topic from the URI. */
  private final String topic;

  /** Properties. */
  private final Properties props;

  KafkaConsumerFactory(URI uri, Properties props) {
    this.uri = uri;
    this.props = props;
    this.topic = Utils.topic(uri);
  }

  public KafkaConsumer<String, byte[]> create(
      String name,
      @Nullable ConsumerRebalanceListener listener) {

    log.debug("Creating named consumer with name {} and listener {}", name, listener);
    Properties props = new Properties(this.props);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, uri.getAuthority());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, name);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.String().deserializer().getClass());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Serdes.ByteArray().deserializer().getClass());
    KafkaConsumer<String, byte[]> ret = new KafkaConsumer<>(props);
    if (listener == null) {
      ret.subscribe(Collections.singletonList(topic));
    } else {
      ret.subscribe(Collections.singletonList(topic), listener);
    }
    return ret;
  }

  /**
   * Create kafka consumer based on given parameters.
   * @param name name of the consumer
   * @return {@link KafkaConsumer} of given name
   */
  public KafkaConsumer<String, byte[]> create(String name) {
    return create(name, null);
  }

  public KafkaConsumer<String, byte[]> create(Collection<Partition> partitions) {
    log.debug("Creating unnamed consumer for partitions {}", partitions);
    Properties props = new Properties(this.props);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, uri.getAuthority());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.String().deserializer().getClass());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Serdes.ByteArray().deserializer().getClass());
    KafkaConsumer<String, byte[]> ret = new KafkaConsumer<>(props);
    List<TopicPartition> topicPartitions = partitions.stream()
        .map(p -> new TopicPartition(topic, p.getId()))
        .collect(Collectors.toList());

    ret.assign(topicPartitions);
    return ret;
  }


  /**
   * Create an unnamed consumer consuming all partitions.
   * @return unnamed {@link KafkaConsumer} for all partitions
   */
  public KafkaConsumer<String, byte[]> create() {
    log.debug("Creating unnamed consumer for all partitions of topic {}", topic);
    Properties props = new Properties(this.props);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, uri.getAuthority());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.String().deserializer().getClass());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Serdes.ByteArray().deserializer().getClass());
    KafkaConsumer<String, byte[]> ret = new KafkaConsumer<>(props);

    List<TopicPartition> partitions = ret.partitionsFor(topic).stream()
        .map(p -> new TopicPartition(topic, p.partition()))
        .collect(Collectors.toList());

    ret.assign(partitions);
    return ret;
  }

}
