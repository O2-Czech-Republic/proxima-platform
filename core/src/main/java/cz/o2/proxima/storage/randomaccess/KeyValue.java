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
package cz.o2.proxima.storage.randomaccess;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * {@code KeyValue} with {@code Offset}.
 */
@Stable
public class KeyValue<T> {

  @SuppressWarnings("unchecked")
  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      RandomOffset offset,
      byte[] valueBytes) {

    return of(
        entityDesc, attrDesc, key, attribute,
        offset, valueBytes, System.currentTimeMillis());
  }

  @SuppressWarnings("unchecked")
  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      RandomOffset offset,
      byte[] valueBytes,
      long stamp) {


    Optional<T> value = attrDesc.getValueSerializer().deserialize(valueBytes);

    if (!value.isPresent()) {
      throw new IllegalArgumentException(
          "Cannot parse given bytes of length " + valueBytes.length
              + " to value with serializer " + attrDesc.getValueSerializer());
    }

    return new KeyValue<>(
        entityDesc,
        attrDesc,
        key,
        attribute,
        offset,
        value.get(),
        valueBytes,
        stamp);
  }


  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      RandomOffset offset,
      T value,
      byte[] valueBytes) {

    return new KeyValue<>(
        entityDesc,
        attrDesc,
        key,
        attribute,
        offset,
        value,
        valueBytes,
        System.currentTimeMillis());
  }


  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      RandomOffset offset,
      T value,
      byte[] valueBytes,
      long stamp) {

    return new KeyValue<>(
        entityDesc,
        attrDesc,
        key,
        attribute,
        offset,
        value,
        valueBytes,
        stamp);
  }

  @Getter
  private final EntityDescriptor entityDescriptor;

  @Getter
  private final AttributeDescriptor<T> attrDescriptor;

  @Getter
  private final String key;

  @Getter
  private final String attribute;

  @Getter
  private final T value;

  @Getter
  @Nullable
  private final byte[] valueBytes;

  @Getter
  private final RandomOffset offset;

  @Getter
  private final long stamp;


  KeyValue(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      RandomOffset offset,
      T value,
      byte[] valueBytes,
      long stamp) {

    this.entityDescriptor = Objects.requireNonNull(entityDesc);
    this.attrDescriptor = Objects.requireNonNull(attrDesc);
    this.key = Objects.requireNonNull(key);
    this.attribute = Objects.requireNonNull(attribute);
    this.value = Objects.requireNonNull(value);
    this.valueBytes = valueBytes;
    this.offset = Objects.requireNonNull(offset);
    this.stamp = stamp;
  }

  @Override
  public String toString() {
    return "KeyValue("
        + "entityDesc=" + getEntityDescriptor()
        + ", attrDesc=" + getAttrDescriptor()
        + ", key=" + getKey()
        + ", attribute=" + getAttribute()
        + ", offset=" + getOffset()
        + ", stamp=" + getStamp()
        + ", value=" + getValue() + ")";
  }

}
