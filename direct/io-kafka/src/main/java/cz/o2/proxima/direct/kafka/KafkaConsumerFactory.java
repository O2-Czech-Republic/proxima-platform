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
package cz.o2.proxima.direct.kafka;

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
import org.apache.kafka.common.serialization.Serde;

/** Factory for {@code KafkaConsumer}s attached to the given commit log. */
@Slf4j
public class KafkaConsumerFactory<K, V> {

  /** URI of the log. */
  private final URI uri;

  /** Our assigned topic from the URI. */
  private final String topic;

  /** Properties. */
  private final Properties props;

  /** Serde for key. */
  private final Serde<K> keySerde;

  /** Serde for value. */
  private final Serde<V> valueSerde;

  KafkaConsumerFactory(URI uri, Properties props, Serde<K> keySerde, Serde<V> valueSerde) {
    this.uri = uri;
    this.props = props;
    this.topic = Utils.topic(uri);
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  public KafkaConsumer<K, V> create(String name, @Nullable ConsumerRebalanceListener listener) {

    log.debug("Creating named consumer with name {} and listener {}", name, listener);
    Properties cloned = clone(this.props);
    cloned.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, uri.getAuthority());
    cloned.put(ConsumerConfig.GROUP_ID_CONFIG, name);
    cloned.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0);
    cloned.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cloned.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde.deserializer().getClass());
    cloned.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerde.deserializer().getClass());
    KafkaConsumer<K, V> ret = new KafkaConsumer<>(cloned);
    if (listener == null) {
      ret.subscribe(Collections.singletonList(topic));
    } else {
      ret.subscribe(Collections.singletonList(topic), listener);
    }
    return ret;
  }

  /**
   * Create kafka consumer based on given parameters.
   *
   * @param name name of the consumer
   * @return {@link KafkaConsumer} of given name
   */
  public KafkaConsumer<K, V> create(String name) {
    return create(name, null);
  }

  public KafkaConsumer<K, V> create(Collection<Partition> partitions) {
    log.debug("Creating unnamed consumer for partitions {}", partitions);
    Properties cloned = clone(this.props);
    cloned.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, uri.getAuthority());
    cloned.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cloned.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde.deserializer().getClass());
    cloned.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerde.deserializer().getClass());
    KafkaConsumer<K, V> ret = new KafkaConsumer<>(cloned);
    List<TopicPartition> topicPartitions =
        partitions
            .stream()
            .map(p -> new TopicPartition(topic, p.getId()))
            .collect(Collectors.toList());

    ret.assign(topicPartitions);
    return ret;
  }

  /**
   * Create an unnamed consumer consuming all partitions.
   *
   * @return unnamed {@link KafkaConsumer} for all partitions
   */
  public KafkaConsumer<K, V> create() {
    log.debug("Creating unnamed consumer for all partitions of topic {}", topic);
    Properties cloned = clone(this.props);
    cloned.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, uri.getAuthority());
    cloned.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cloned.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde.deserializer().getClass());
    cloned.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerde.deserializer().getClass());
    KafkaConsumer<K, V> ret = new KafkaConsumer<>(cloned);

    List<TopicPartition> partitions =
        ret.partitionsFor(topic)
            .stream()
            .map(p -> new TopicPartition(topic, p.partition()))
            .collect(Collectors.toList());

    ret.assign(partitions);
    return ret;
  }

  private Properties clone(Properties props) {
    Properties ret = new Properties();
    props.forEach(ret::put);
    return ret;
  }
}
