/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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

import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Partitioner;
import java.util.Properties;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/** ${link OnlineAttributeWriter} implementation for Kafka. */
@Slf4j
public class KafkaWriter<K, V> extends AbstractOnlineAttributeWriter {

  @Getter final KafkaAccessor accessor;
  private final Partitioner partitioner;
  private final String topic;
  private final ElementSerializer<K, V> serializer;

  @Nullable private transient KafkaProducer<K, V> producer;

  KafkaWriter(KafkaAccessor accessor) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
    this.partitioner = accessor.getPartitioner();
    this.topic = accessor.getTopic();
    this.serializer = accessor.getSerializer();
  }

  @Override
  public void write(StreamElement data, CommitCallback callback) {
    try {
      if (producer == null) {
        producer = createProducer();
      }
      int partition =
          (partitioner.getPartitionId(data) & Integer.MAX_VALUE)
              % producer.partitionsFor(topic).size();

      ProducerRecord<K, V> toWrite = serializer.write(topic, partition, data);
      producer.send(
          toWrite,
          (metadata, exception) -> {
            log.debug(
                "Written {} to topic {} offset {} and partition {}",
                data,
                metadata.topic(),
                metadata.offset(),
                metadata.partition());
            callback.commit(exception == null, exception);
          });
    } catch (Exception ex) {
      log.warn("Failed to write ingest {}", data, ex);
      callback.commit(false, ex);
    }
  }

  @Override
  public OnlineAttributeWriter.Factory<?> asFactory() {
    final KafkaAccessor accessor = this.accessor;
    return repo -> new KafkaWriter<>(accessor);
  }

  private KafkaProducer<K, V> createProducer() {
    Properties props = accessor.createProps();
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getUri().getAuthority());
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    return new KafkaProducer<>(
        props, serializer.keySerde().serializer(), serializer.valueSerde().serializer());
  }

  @Override
  public void close() {
    if (this.producer != null) {
      this.producer.close();
      this.producer = null;
    }
  }
}
