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
package cz.o2.proxima.direct.randomaccess;

import com.google.common.base.MoreObjects;
import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;

/** {@code KeyValue} with {@code Offset}. */
@Evolving
public class KeyValue<T> extends StreamElement {

  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      RandomOffset offset,
      byte[] valueBytes) {

    return of(entityDesc, attrDesc, key, attribute, offset, valueBytes, System.currentTimeMillis());
  }

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
          "Cannot parse given bytes of length "
              + valueBytes.length
              + " to value with serializer "
              + attrDesc.getValueSerializer());
    }

    return new KeyValue<>(
        entityDesc, attrDesc, key, attribute, offset, value.get(), valueBytes, stamp);
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

    return new KeyValue<>(entityDesc, attrDesc, key, attribute, offset, value, valueBytes, stamp);
  }

  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      long seqId,
      String key,
      String attribute,
      RandomOffset offset,
      T value,
      byte[] valueBytes,
      long stamp) {

    return new KeyValue<>(
        entityDesc, attrDesc, seqId, key, attribute, offset, value, valueBytes, stamp);
  }

  @Getter private final RandomOffset offset;

  private KeyValue(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      RandomOffset offset,
      T value,
      @Nullable byte[] valueBytes,
      long stamp) {

    super(
        entityDesc,
        attrDesc,
        UUID.randomUUID().toString(),
        key,
        attribute,
        stamp,
        false,
        asValueBytes(attrDesc, value, valueBytes));
    this.offset = Objects.requireNonNull(offset);
    setParsed(value);
  }

  private KeyValue(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      long sequenceId,
      String key,
      String attribute,
      RandomOffset offset,
      T value,
      @Nullable byte[] valueBytes,
      long stamp) {

    super(
        entityDesc,
        attrDesc,
        sequenceId,
        key,
        attribute,
        stamp,
        false,
        asValueBytes(attrDesc, value, valueBytes));
    this.offset = Objects.requireNonNull(offset);
    setParsed(value);
  }

  private static <T> byte[] asValueBytes(AttributeDescriptor<T> attr, T value, byte[] valueBytes) {
    if (valueBytes == null) {
      return attr.getValueSerializer().serialize(value);
    }
    return valueBytes;
  }

  /**
   * Equivalent of getParsed().get() throwing explaining exceptions.
   *
   * @return value if present
   * @throws IllegalStateException if {@link #getParsed} would have returned {@link
   *     Optional#empty()}
   */
  public T getParsedRequired() {
    return this.<T>getParsed()
        .orElseThrow(() -> new IllegalStateException(String.format("Missing value in %s", this)));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("entityDescriptor", getEntityDescriptor())
        .add("attributeDescriptor", getAttributeDescriptor())
        .add("key", getKey())
        .add("attribute", getAttribute())
        .add("stamp", getStamp())
        .add("offset", getOffset())
        .add("value", getParsed().orElse(null))
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof KeyValue)) {
      return false;
    }
    return super.equals(obj) && ((KeyValue) obj).getOffset().equals(offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), offset);
  }
}
