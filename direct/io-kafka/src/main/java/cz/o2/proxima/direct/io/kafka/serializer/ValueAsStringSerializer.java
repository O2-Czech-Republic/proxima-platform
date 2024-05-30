/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.kafka.serializer;

import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.io.kafka.ElementSerializer;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class ValueAsStringSerializer implements ElementSerializer<Void, String> {

  private static final long serialVersionUID = 1L;

  private final UnaryFunction<EntityDescriptor, AttributeDescriptor<String>> readAttributeFn;
  private AttributeDescriptor<String> attr;

  protected ValueAsStringSerializer(String attrName) {
    this(desc -> desc.getAttribute(attrName));
  }

  protected ValueAsStringSerializer(
      UnaryFunction<EntityDescriptor, AttributeDescriptor<String>> readAttributeFn) {
    this.readAttributeFn = readAttributeFn;
  }

  @Override
  public void setup(EntityDescriptor entityDescriptor) {
    attr = readAttributeFn.apply(entityDescriptor);
  }

  @Nullable
  @Override
  public StreamElement read(
      ConsumerRecord<Void, String> consumerRecord, EntityDescriptor entityDescriptor) {
    return parseValue(
        entityDescriptor,
        consumerRecord.value(),
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp());
  }

  @VisibleForTesting
  @Nullable
  StreamElement parseValue(
      EntityDescriptor entityDescriptor, String value, int partition, long offset, long timestamp) {
    String uuid = partition + ":" + offset;
    byte[] bytes = value != null ? attr.getValueSerializer().serialize(value) : null;
    if (attr.isWildcard()) {
      return StreamElement.upsert(
          entityDescriptor, attr, uuid, uuid, attr.toAttributePrefix() + uuid, timestamp, bytes);
    } else {
      return StreamElement.upsert(
          entityDescriptor, attr, uuid, uuid, attr.getName(), timestamp, bytes);
    }
  }

  @Override
  public ProducerRecord<Void, String> write(
      String topic, int partition, StreamElement streamElement) {

    return new ProducerRecord<>(topic, null, Optionals.get(streamElement.getParsed()));
  }

  @Override
  public Serde<Void> keySerde() {
    return Serdes.Void();
  }

  @Override
  public Serde<String> valueSerde() {
    return Serdes.String();
  }
}
