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

import cz.o2.proxima.storage.AbstractOnlineAttributeWriter;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.StreamElement;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

/**
 * ${link OnlineAttributeWriter} implementation for Kafka.
 */
@Slf4j
public class KafkaWriter extends AbstractOnlineAttributeWriter {

  @Getter
  final KafkaAccessor accessor;
  private final Partitioner partitioner;
  private final String topic;

  private KafkaProducer<String, byte[]> producer;

  KafkaWriter(KafkaAccessor accessor) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
    this.accessor = accessor;
    this.partitioner = accessor.getPartitioner();
    this.topic = accessor.getTopic();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(StreamElement data, CommitCallback callback) {
    try {
      if (producer == null) {
        producer = createProducer();
      }
      int partition = (partitioner.getPartitionId(
          data.getKey(), data.getAttribute(), data.getValue()) & Integer.MAX_VALUE)
          % producer.partitionsFor(topic).size();
      producer.send(
          new ProducerRecord(topic, partition, data.getStamp(), data.getKey()
              + "#" + data.getAttribute(), data.getValue()),
              (metadata, exception) -> callback.commit(exception == null, exception));
    } catch (Exception ex) {
      log.warn("Failed to write ingest {}", data, ex);
      callback.commit(false, ex);
    }
  }

  @SuppressWarnings("unchecked")
  private KafkaProducer<String, byte[]> createProducer() {
    Properties props = accessor.createProps();
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getURI().getAuthority());
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    return new KafkaProducer<>(
        props, Serdes.String().serializer(), Serdes.ByteArray().serializer());
  }

  public void close() {
    if (this.producer != null) {
      this.producer.close();
      this.producer = null;
    }
  }

}
