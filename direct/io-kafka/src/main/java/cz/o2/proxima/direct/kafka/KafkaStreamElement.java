/**
 * Copyright 2017-${Year} O2 Czech Republic, a.s.
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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import lombok.Getter;

/** Data read from a kafka partition. */
public class KafkaStreamElement extends StreamElement {

  @Getter private final int partition;
  @Getter private final long offset;

  KafkaStreamElement(
      EntityDescriptor entityDesc,
      AttributeDescriptor attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp,
      byte[] value,
      int partition,
      long offset) {

    super(
        entityDesc,
        attributeDesc,
        uuid,
        key,
        attribute,
        stamp,
        false /* not forced, is inferred from attribute descriptor name */,
        value);

    this.partition = partition;
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "KafkaStreamElement(entityDesc="
        + getEntityDescriptor()
        + ", attributeDesc="
        + getAttributeDescriptor()
        + ", uuid="
        + getUuid()
        + ", key="
        + getKey()
        + ", attribute="
        + getAttribute()
        + ", stamp="
        + getStamp()
        + ", value.length="
        + (getValue() == null ? 0 : getValue().length)
        + ", partition="
        + partition
        + ", offset="
        + offset
        + ")";
  }
}
