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
package cz.o2.proxima.direct.kafka.serializer;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.direct.kafka.ElementSerializer;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * A {@link ElementSerializer} that reads values from kafka (discards keys) and returns values as
 * byte arrays.
 *
 * <p>You must subclass this class to create zero-parameter constructor.
 */
public class ValueAsBytesSerializer implements ElementSerializer<byte[], byte[]> {

  private static final long serialVersionUID = 1L;

  private final UnaryFunction<EntityDescriptor, AttributeDescriptor<byte[]>> readAttributeFn;
  private AttributeDescriptor<byte[]> attr;

  protected ValueAsBytesSerializer(String attrName) {
    this(desc -> desc.getAttribute(attrName));
  }

  protected ValueAsBytesSerializer(
      UnaryFunction<EntityDescriptor, AttributeDescriptor<byte[]>> readAttributeFn) {
    this.readAttributeFn = readAttributeFn;
  }

  @Override
  public void setup(EntityDescriptor entityDescriptor) {
    attr = readAttributeFn.apply(entityDescriptor);
  }

  @Nullable
  @Override
  public StreamElement read(
      ConsumerRecord<byte[], byte[]> consumerRecord, EntityDescriptor entityDescriptor) {
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
      EntityDescriptor entityDescriptor, byte[] value, int partition, long offset, long timestamp) {
    String uuid = partition + ":" + offset;
    if (attr.isWildcard()) {
      return StreamElement.upsert(
          entityDescriptor, attr, uuid, uuid, attr.toAttributePrefix() + uuid, timestamp, value);
    } else {
      return StreamElement.upsert(
          entityDescriptor, attr, uuid, uuid, attr.getName(), timestamp, value);
    }
  }

  @Override
  public ProducerRecord<byte[], byte[]> write(
      String topic, int partition, StreamElement streamElement) {
    throw new UnsupportedOperationException("Readonly storage!");
  }

  @Override
  public Serde<byte[]> keySerde() {
    return Serdes.ByteArray();
  }

  @Override
  public Serde<byte[]> valueSerde() {
    return Serdes.ByteArray();
  }
}
