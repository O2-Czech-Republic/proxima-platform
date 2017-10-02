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

package cz.o2.proxima.tools.io;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import lombok.Getter;

/**
 * A typed ingest for purpose of console processing.
 */
public class TypedIngest<T> {

  @Getter
  final EntityDescriptor entityDesc;

  @Getter
  final AttributeDescriptor<T> attrDesc;

  @Getter
  final String key;

  @Getter
  final String attribute;

  @Getter
  final long stamp;

  @Getter
  final T value;

  public static <T> TypedIngest<T> of(StreamElement element) {
    return new TypedIngest<>(element);
  }

  @SuppressWarnings("unchecked")
  private TypedIngest(StreamElement element) {
    this.entityDesc = element.getEntityDescriptor();
    this.attrDesc = (AttributeDescriptor<T>) element.getAttributeDescriptor();
    this.key = element.getKey();
    this.attribute = element.getAttribute();
    this.stamp = element.getStamp();
    if (element.getValue() != null) {
      this.value = (T) attrDesc.getValueSerializer()
          .deserialize(element.getValue())
          .get();
    } else {
      this.value = null;
    }
  }

  @Override
  public String toString() {
    return "TypedIngest(entityDesc=" + entityDesc
        + ", attrDesc=" + attrDesc
        + ", key=" + key
        + ", attribute=" + attribute
        + ", stamp=" + stamp
        + ", value=" + value + ")";
  }


}
