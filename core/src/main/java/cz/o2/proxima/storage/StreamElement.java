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
package cz.o2.proxima.storage;

import com.google.common.base.MoreObjects;
import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Data wrapper for all ingestion requests. NOTE: {@link Serializable} is implemented only for
 * tests. Real-world applications should never use java serialization for passing data elements.
 */
@Evolving("Should change to interface with implementations")
public class StreamElement implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Upsert given entity attribute with given value.
   *
   * @param entityDesc descriptor of entity
   * @param attributeDesc descriptor of attribute
   * @param uuid UUID of the request
   * @param key key of entity
   * @param attribute name of attribute of the entity
   * @param stamp timestamp of the event
   * @param value serialized value
   * @return {@link StreamElement} to be written to the system
   */
  public static StreamElement upsert(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp,
      byte[] value) {

    return new StreamElement(entityDesc, attributeDesc, uuid, key, attribute, stamp, false, value);
  }

  /**
   * Delete given instance of attribute.
   *
   * @param entityDesc descriptor of entity
   * @param attributeDesc descriptor of attribute
   * @param uuid UUID of the event
   * @param key key of entity
   * @param attribute attribute of the entity
   * @param stamp timestamp of the delete event
   * @return {@link StreamElement} to be written to the system
   */
  public static StreamElement delete(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp) {

    return new StreamElement(entityDesc, attributeDesc, uuid, key, attribute, stamp, false, null);
  }

  /**
   * Delete all versions of given wildcard attribute.
   *
   * @param entityDesc descriptor of entity
   * @param attributeDesc descriptor of attribute
   * @param uuid UUID of the event
   * @param key key of entity
   * @param attribute string representation of the attribute
   * @param stamp timestamp of the event
   * @return {@link StreamElement} to be written to the system
   */
  public static StreamElement deleteWildcard(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp) {

    if (!attribute.endsWith("*")) {
      attribute += "*";
    }
    return new StreamElement(entityDesc, attributeDesc, uuid, key, attribute, stamp, true, null);
  }

  /**
   * Delete all versions of given wildcard attribute.
   *
   * @param entityDesc descriptor of entity
   * @param attributeDesc descriptor of attribute
   * @param uuid UUID of the event
   * @param key key of entity
   * @param stamp timestamp of the event
   * @return {@link StreamElement} to be written to the system
   */
  public static StreamElement deleteWildcard(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      long stamp) {

    return deleteWildcard(
        entityDesc, attributeDesc, uuid, key, attributeDesc.toAttributePrefix() + "*", stamp);
  }

  @Getter private final EntityDescriptor entityDescriptor;

  @Getter private final AttributeDescriptor<?> attributeDescriptor;

  @Getter private final String uuid;

  @Getter private final String key;

  @Getter private final String attribute;

  @Getter private final long stamp;

  @Getter @Nullable private final byte[] value;

  private final boolean deleteWildcard;

  private transient Object parsed;

  protected StreamElement(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp,
      boolean deleteWildcard,
      @Nullable byte[] value) {

    this.entityDescriptor = Objects.requireNonNull(entityDesc);
    this.attributeDescriptor = Objects.requireNonNull(attributeDesc);
    this.uuid = Objects.requireNonNull(uuid);
    this.key = Objects.requireNonNull(key);
    this.attribute = Objects.requireNonNull(attribute);
    this.stamp = stamp;
    this.value = value;
    this.deleteWildcard = deleteWildcard;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("uuid", uuid)
        .add("entityDesc", entityDescriptor)
        .add("attributeDesc", attributeDescriptor)
        .add("key", key)
        .add("attribute", attribute)
        .add("stamp", stamp)
        .add("value.length", value == null ? -1 : value.length)
        .toString();
  }

  /**
   * Check if this is a delete ingest.
   *
   * @return {@code true} if this is delete or delete wildcard event
   */
  public boolean isDelete() {
    return value == null;
  }

  /**
   * Check if this is a delete wildcard ingest.
   *
   * @return {@code true} if this is delete wildcard event
   */
  public boolean isDeleteWildcard() {
    return isDelete()
        && attributeDescriptor.isWildcard()
        && (attribute.equals(attributeDescriptor.toAttributePrefix() + "*") || deleteWildcard);
  }

  /**
   * Retrieve parsed value.
   *
   * @param <T> the deserialized datatype
   * @return optional deserialized value
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getParsed() {
    if (value == null) {
      return Optional.empty();
    }
    if (parsed == null) {
      attributeDescriptor.getValueSerializer().deserialize(value).ifPresent(this::setParsed);
    }
    return (Optional<T>) Optional.ofNullable(parsed);
  }

  protected void setParsed(Object parsed) {
    this.parsed = parsed;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StreamElement) {
      return ((StreamElement) obj).uuid.equals(uuid);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  /**
   * Dump in more verbose way into given stream.
   *
   * @return string representing the dumped element
   */
  public String dump() {
    @SuppressWarnings("unchecked")
    AttributeDescriptor<Object> attrDesc = (AttributeDescriptor<Object>) getAttributeDescriptor();
    Optional<Object> parsedValue = getParsed();
    return MoreObjects.toStringHelper(getClass())
        .add("uuid", uuid)
        .add("entityDesc", entityDescriptor)
        .add("attributeDesc", attributeDescriptor)
        .add("key", key)
        .add("attribute", attribute)
        .add("stamp", new Date(stamp))
        .add(
            "value",
            parsedValue.isPresent()
                ? attrDesc.getValueSerializer().getLogString(parsedValue.get())
                : "(null)")
        .toString();
  }
}
