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

package cz.o2.proxima.storage;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Data wrapper for all ingestion requests.
 */
public class StreamElement {

  /** Update given entity attribute with given value. */
  public static StreamElement update(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp,
      byte[] value) {

    return new StreamElement(
        entityDesc, attributeDesc, uuid, key,
        attribute, stamp, value);
  }

  /** Delete given instance of attribute. */
  public static StreamElement delete(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp) {

    return new StreamElement(
        entityDesc, attributeDesc, uuid, key,
        attribute, stamp, null);
  }

  /** Delete all versions of given wildcard attribute. */
  public static StreamElement deleteWildcard(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      long stamp) {

    return new StreamElement(
        entityDesc, attributeDesc, uuid,
        key, null, stamp, null);
  }

  @Getter
  private final EntityDescriptor entityDescriptor;

  @Getter
  private final AttributeDescriptor<?> attributeDescriptor;

  @Getter
  private final String uuid;

  @Getter
  private final String key;

  @Getter
  @Nullable
  private final String attribute;

  @Getter
  private final long stamp;

  @Getter
  @Nullable
  private final byte[] value;

  protected StreamElement(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      @Nullable String attribute,
      long stamp,
      @Nullable byte[] value) {

    this.entityDescriptor = Objects.requireNonNull(entityDesc);
    this.attributeDescriptor = Objects.requireNonNull(attributeDesc);
    this.uuid = Objects.requireNonNull(uuid);
    this.key = Objects.requireNonNull(key);
    this.attribute = attribute;
    this.stamp = stamp;
    this.value = value;
  }

  @Override
  public String toString() {
    return "StreamElement(uuid=" + uuid
        + ", entityDesc=" + entityDescriptor
        + ", attributeDesc=" + attributeDescriptor
        + ", key=" + key + ", attribute=" + attribute
        + ", stamp=" + stamp
        + ", value.length=" + (value == null ? -1 : value.length) + ")";

  }

  /** The element is a delete ingest. */
  public boolean isDelete() {
    return value == null;
  }

  /** The element is a delete wildcard ingest. */
  public boolean isDeleteWildcard() {
    return isDelete() && attribute == null;
  }

  /** Retrieve parsed value. */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getParsed() {
    return (Optional<T>) attributeDescriptor.getValueSerializer().deserialize(value);
  }

}
