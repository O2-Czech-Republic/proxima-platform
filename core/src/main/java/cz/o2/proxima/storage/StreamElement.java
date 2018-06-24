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
package cz.o2.proxima.storage;

import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Data wrapper for all ingestion requests.
 * NOTE: {@link Serializable} is implemented only for tests. Real-world applications
 * should never use java serialization for passing data elements.
 */
@Evolving("Should change to interface with implementations")
public class StreamElement<T> implements Serializable {

  /**
   * Create new builder.
   *
   * @param entityDescriptor entity descriptor
   * @param attributeDescriptor attribute descriptor
   * @param <T> type of attribute
   * @return uuid builder
   */
  public static <T> UUIDBuilder<T> of(
      EntityDescriptor entityDescriptor, AttributeDescriptor<T> attributeDescriptor) {
    return new Builder<>(entityDescriptor, attributeDescriptor);
  }

  public interface UUIDBuilder<T> {

    /**
     * Set uuid.
     *
     * @param uuid unique identifier of stream element
     * @return key builder
     */
    KeyBuilder<T> uuid(String uuid);
  }

  public interface KeyBuilder<T> {

    /**
     * Set key.
     *
     * @param key key of stream element
     * @return timestamp builder
     */
    TimestampBuilder<T> key(String key);
  }

  public interface TimestampBuilder<T> {

    /**
     * Set timestamp.
     *
     * @param timestamp timestamp of stream element
     * @return attribute builder
     */
    AttributeBuilder<T> timestamp(long timestamp);
  }

  public interface AttributeBuilder<T> {

    /**
     * Set attribute.
     *
     * @param attribute attribute of stream element
     * @return final builder
     */
    FinalBuilder<T> attribute(String attribute);

    /**
     * Build stream element, which deletes values for wildcard attribute.
     *
     * @return stream element
     */
    StreamElement<T> deleteWildcard();
  }

  public interface FinalBuilder<T> {

    /**
     * Build stream element, which updates value.
     *
     * @param value value of stream element
     * @return stream element
     */
    StreamElement<T> update(T value);

    /**
     * Build stream element, which updates value.
     *
     * @param value value of stream element
     * @return stream element
     */
    StreamElement<T> update(byte[] value);

    /**
     * Build stream element, which deletes value.
     *
     * @return stream element
     */
    StreamElement<T> delete();
  }

  /**
   * Convenient builder for {@link StreamElement} values.
   *
   * @param <T> type of wrapped value
   */
  public static class Builder<T> implements
      UUIDBuilder<T>, KeyBuilder<T>, TimestampBuilder<T>, AttributeBuilder<T>, FinalBuilder<T> {

    private final EntityDescriptor entityDescriptor;
    private final AttributeDescriptor<T> attributeDescriptor;

    private String uuid;
    private String key;
    private long timestamp;
    private String attribute;

    private Builder(EntityDescriptor entityDescriptor, AttributeDescriptor<T> attributeDescriptor) {
      this.entityDescriptor = entityDescriptor;
      this.attributeDescriptor = attributeDescriptor;
    }

    @Override
    public KeyBuilder<T> uuid(String uuid) {
      this.uuid = uuid;
      return this;
    }

    @Override
    public TimestampBuilder<T> key(String key) {
      this.key = key;
      return this;
    }

    @Override
    public AttributeBuilder<T> timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override
    public FinalBuilder<T> attribute(String attribute) {
      this.attribute = attribute;
      return this;
    }

    @Override
    public StreamElement<T> deleteWildcard() {
      return new StreamElement<>(
          entityDescriptor, attributeDescriptor, uuid, key, timestamp, null, null);
    }

    @Override
    public StreamElement<T> update(T value) {
      final byte[] serialized = attributeDescriptor.getValueSerializer().serialize(value);
      return new StreamElement<>(
          entityDescriptor, attributeDescriptor, uuid, key, timestamp, attribute, serialized);
    }

    @Override
    public StreamElement<T> update(byte[] value) {
      return new StreamElement<>(
          entityDescriptor, attributeDescriptor, uuid, key, timestamp, attribute, value);
    }

    @Override
    public StreamElement<T> delete() {
      return new StreamElement<>(
          entityDescriptor, attributeDescriptor, uuid, key, timestamp, attribute, null);
    }
  }

  /**
   * Update given entity attribute with given value.
   * @param entityDesc descriptor of entity
   * @param attributeDesc descriptor of attribute
   * @param uuid UUID of the request
   * @param key key of entity
   * @param attribute name of attribute of the entity
   * @param stamp timestamp of the event
   * @param value serialized value
   * @return {@link StreamElement} to be written to the system
   */
  @Deprecated
  public static StreamElement update(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp,
      byte[] value) {
    return new StreamElement<>(entityDesc, attributeDesc, uuid, key, stamp, attribute, value);
  }

  /**
   * Delete given instance of attribute.
   * @param entityDesc descriptor of entity
   * @param attributeDesc descriptor of attribute
   * @param uuid UUID of the event
   * @param key key of entity
   * @param attribute attribute of the entity
   * @param stamp timestamp of the delete event
   * @return {@link StreamElement} to be written to the system
   */
  @Deprecated
  public static StreamElement delete(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp) {
    return new StreamElement<>(entityDesc, attributeDesc, uuid, key, stamp, attribute, null);
  }

  /**
   * Delete all versions of given wildcard attribute.
   * @param entityDesc descriptor of entity
   * @param attributeDesc descriptor of attribute
   * @param uuid UUID of the event
   * @param key key of entity
   * @param stamp timestamp of the event
   * @return {@link StreamElement} to be written to the system
   */
  @Deprecated
  public static StreamElement deleteWildcard(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      long stamp) {
    return new StreamElement<>(entityDesc, attributeDesc, uuid, key, stamp, null, null);
  }

  @Getter
  private final EntityDescriptor entityDescriptor;

  @Getter
  private final AttributeDescriptor<T> attributeDescriptor;

  @Getter
  private final String uuid;

  @Getter
  private final String key;

  @Getter
  private final long stamp;

  @Getter
  @Nullable
  private final String attribute;

  @Getter
  @Nullable
  private final byte[] value;

  protected StreamElement(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attributeDesc,
      String uuid,
      String key,
      long stamp,
      @Nullable String attribute,
      @Nullable byte[] value) {
    this.entityDescriptor = Objects.requireNonNull(entityDesc);
    this.attributeDescriptor = Objects.requireNonNull(attributeDesc);
    this.uuid = Objects.requireNonNull(uuid);
    this.key = Objects.requireNonNull(key);
    this.stamp = stamp;
    this.attribute = attribute;
    this.value = value;
  }

  @Override
  public String toString() {
    return "StreamElement(uuid=" + uuid
        + ", entityDesc=" + entityDescriptor
        + ", attributeDesc=" + attributeDescriptor
        + ", key=" + key
        + ", stamp=" + stamp
        + ", attribute=" + attribute
        + ", value.length=" + (value == null ? -1 : value.length) + ")";
  }

  /**
   * Check if this is a delete ingest.
   * @return {@code true} if this is delete or delete wildcard event
   */
  public boolean isDelete() {
    return value == null;
  }

  /**
   * Check if this is a delete wildcard ingest.
   * @return {@code true} if this is delete wildcard event
   */
  public boolean isDeleteWildcard() {
    return isDelete() && attribute == null;
  }

  /**
   * Retrieve parsed value.
   * @param <T> the deserialized datatype
   * @return optional deserialized value
   */
  @SuppressWarnings("unchecked")
  public Optional<T> getParsed() {
    return attributeDescriptor.getValueSerializer().deserialize(value);
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
}
