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
package cz.o2.proxima.repository;

import com.google.common.base.Preconditions;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** {@link AttributeDescriptor} */
public class EntityAwareAttributeDescriptor<T> implements AttributeDescriptor<T> {

  private static final long serialVersionUID = 1L;

  public static class Regular<T> extends EntityAwareAttributeDescriptor<T> {

    private static final long serialVersionUID = 1L;

    private Regular(EntityDescriptor entity, AttributeDescriptor<T> attr) {
      super(entity, attr);
      Preconditions.checkArgument(!isWildcard());
    }

    /**
     * Create upsert update to non-wildcard attribute.
     *
     * @param uuid UUID of the upsert
     * @param key key of the upsert
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String uuid, String key, long stamp, T value) {
      return StreamElement.upsert(
          entity,
          wrapped,
          uuid,
          key,
          wrapped.getName(),
          stamp,
          wrapped.getValueSerializer().serialize(value));
    }

    /**
     * Create upsert update to non-wildcard attribute.
     *
     * @param uuid UUID of the upsert
     * @param key key of the upsert
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String uuid, String key, Instant stamp, T value) {
      return upsert(uuid, key, stamp.toEpochMilli(), value);
    }

    /**
     * Create upsert update to non-wildcard attribute. UUID is auto generated.
     *
     * @param key key of the upsert
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String key, long stamp, T value) {
      return upsert(UUID.randomUUID().toString(), key, stamp, value);
    }

    /**
     * Create upsert update to non-wildcard attribute. UUID is auto generated.
     *
     * @param sequentialId sequential ID of the update
     * @param key key of the upsert
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(long sequentialId, String key, long stamp, T value) {
      return StreamElement.upsert(
          entity,
          wrapped,
          sequentialId,
          key,
          wrapped.getName(),
          stamp,
          wrapped.getValueSerializer().serialize(value));
    }

    /**
     * Create upsert update to non-wildcard attribute. UUID is auto generated.
     *
     * @param key key of the upsert
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String key, Instant stamp, T value) {
      return upsert(UUID.randomUUID().toString(), key, stamp, value);
    }

    /**
     * Create delete for non-wildcard attribute.
     *
     * @param uuid UUID of the delete
     * @param key key of the delete
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String uuid, String key, Instant stamp) {
      return delete(uuid, key, stamp.toEpochMilli());
    }

    /**
     * Create delete for non-wildcard attribute.
     *
     * @param uuid UUID of the delete
     * @param key key of the delete
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String uuid, String key, long stamp) {
      return StreamElement.delete(entity, wrapped, uuid, key, wrapped.getName(), stamp);
    }

    /**
     * Create delete for non-wildcard attribute.
     *
     * @param sequentialId sequential ID of the delete
     * @param key key of the delete
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(long sequentialId, String key, long stamp) {
      return StreamElement.delete(entity, wrapped, sequentialId, key, wrapped.getName(), stamp);
    }

    /**
     * Create delete for non-wildcard attribute. UUID is auto generated.
     *
     * @param key key of the delete
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String key, long stamp) {
      return delete(UUID.randomUUID().toString(), key, stamp);
    }

    /**
     * Create delete for non-wildcard attribute. UUID is auto generated.
     *
     * @param key key of the delete
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String key, Instant stamp) {
      return delete(UUID.randomUUID().toString(), key, stamp.toEpochMilli());
    }
  }

  public static class Wildcard<T> extends EntityAwareAttributeDescriptor<T> {

    private static final long serialVersionUID = 1L;

    private Wildcard(EntityDescriptor entity, AttributeDescriptor<T> attr) {
      super(entity, attr);
      Preconditions.checkArgument(isWildcard());
    }

    /**
     * Create upsert update to wildcard attribute.
     *
     * @param uuid UUID of the upsert
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String uuid, String key, String attribute, long stamp, T value) {
      Preconditions.checkArgument(!attribute.startsWith(toAttributePrefix()));
      return StreamElement.upsert(
          entity,
          wrapped,
          uuid,
          key,
          wrapped.toAttributePrefix() + attribute,
          stamp,
          wrapped.getValueSerializer().serialize(value));
    }

    /**
     * Create upsert update to wildcard attribute.
     *
     * @param sequentialId sequential ID of the upsert
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(
        long sequentialId, String key, String attribute, long stamp, T value) {
      Preconditions.checkArgument(!attribute.startsWith(toAttributePrefix()));
      return StreamElement.upsert(
          entity,
          wrapped,
          sequentialId,
          key,
          wrapped.toAttributePrefix() + attribute,
          stamp,
          wrapped.getValueSerializer().serialize(value));
    }

    /**
     * Create upsert update to wildcard attribute.
     *
     * @param uuid UUID of the upsert
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String uuid, String key, String attribute, Instant stamp, T value) {
      return upsert(uuid, key, attribute, stamp.toEpochMilli(), value);
    }

    /**
     * Create upsert update to wildcard attribute. UUID is autogenerated.
     *
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String key, String attribute, long stamp, T value) {
      return upsert(UUID.randomUUID().toString(), key, attribute, stamp, value);
    }

    /**
     * Create upsert update to wildcard attribute. UUID is autogenerated.
     *
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @param value the value to write
     * @return the upsert {@link StreamElement}
     */
    public StreamElement upsert(String key, String attribute, Instant stamp, T value) {
      return upsert(key, attribute, stamp.toEpochMilli(), value);
    }

    /**
     * Create delete for wildcard attribute.
     *
     * @param uuid UUID of the delete
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String uuid, String key, String attribute, long stamp) {
      Preconditions.checkArgument(!attribute.startsWith(toAttributePrefix()));
      return StreamElement.delete(
          entity, wrapped, uuid, key, wrapped.toAttributePrefix() + attribute, stamp);
    }

    /**
     * Create delete for wildcard attribute.
     *
     * @param sequentialId sequential ID of the delete
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(long sequentialId, String key, String attribute, long stamp) {
      Preconditions.checkArgument(!attribute.startsWith(toAttributePrefix()));
      return StreamElement.delete(
          entity, wrapped, sequentialId, key, wrapped.toAttributePrefix() + attribute, stamp);
    }

    /**
     * Create delete for wildcard attribute.
     *
     * @param uuid UUID of the delete
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String uuid, String key, String attribute, Instant stamp) {
      return delete(uuid, key, attribute, stamp.toEpochMilli());
    }

    /**
     * Create delete for wildcard attribute. UUID is autogenerated.
     *
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String key, String attribute, long stamp) {
      return delete(UUID.randomUUID().toString(), key, attribute, stamp);
    }

    /**
     * Create delete for wildcard attribute. UUID is autogenerated.
     *
     * @param key key of the upsert
     * @param attribute the attribute (only the suffix part)
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement delete(String key, String attribute, Instant stamp) {
      return delete(UUID.randomUUID().toString(), key, attribute, stamp.toEpochMilli());
    }

    /**
     * Delete wildcard attribute (all versions).
     *
     * @param uuid UUID of the delete
     * @param key key of the upsert
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement deleteWildcard(String uuid, String key, long stamp) {
      return StreamElement.deleteWildcard(entity, wrapped, uuid, key, stamp);
    }

    /**
     * Delete wildcard attribute (all versions).
     *
     * @param sequentialId sequential ID of the delete
     * @param key key of the upsert
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement deleteWildcard(long sequentialId, String key, long stamp) {
      return StreamElement.deleteWildcard(entity, wrapped, sequentialId, key, stamp);
    }

    /**
     * Delete wildcard attribute (all versions).
     *
     * @param uuid UUID of the delete
     * @param key key of the upsert
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement deleteWildcard(String uuid, String key, Instant stamp) {
      return deleteWildcard(uuid, key, stamp.toEpochMilli());
    }

    /**
     * Delete wildcard attribute (all versions). UUID is autogenerated.
     *
     * @param key key of the upsert
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement deleteWildcard(String key, long stamp) {
      return deleteWildcard(UUID.randomUUID().toString(), key, stamp);
    }

    /**
     * Delete wildcard attribute (all versions). UUID is autogenerated.
     *
     * @param key key of the upsert
     * @param stamp timestamp
     * @return the delete {@link StreamElement}
     */
    public StreamElement deleteWildcard(String key, Instant stamp) {
      return deleteWildcard(key, stamp.toEpochMilli());
    }

    /**
     * Parse given attribute name (prefix.suffix) to suffix only.
     *
     * @param attribute complete name of attribute
     * @return the suffix part
     */
    public String extractSuffix(String attribute) {
      Preconditions.checkArgument(attribute.length() >= toAttributePrefix().length());
      return attribute.substring(toAttributePrefix().length());
    }
  }

  public static <T> Regular<T> regular(EntityDescriptor entity, AttributeDescriptor<T> wrapped) {
    return new Regular<>(entity, wrapped);
  }

  public static <T> Wildcard<T> wildcard(EntityDescriptor entity, AttributeDescriptor<T> wrapped) {
    return new Wildcard<>(entity, wrapped);
  }

  final EntityDescriptor entity;
  final AttributeDescriptor<T> wrapped;

  private EntityAwareAttributeDescriptor(EntityDescriptor entity, AttributeDescriptor<T> attr) {
    Preconditions.checkArgument(entity.getName().equals(attr.getEntity()));
    this.entity = entity;
    this.wrapped = attr;
  }

  @Override
  public String getName() {
    return wrapped.getName();
  }

  @Override
  public boolean isWildcard() {
    return wrapped.isWildcard();
  }

  @Override
  public URI getSchemeUri() {
    return wrapped.getSchemeUri();
  }

  @Override
  public String getEntity() {
    return wrapped.getEntity();
  }

  @Override
  public String toAttributePrefix(boolean includeLastDot) {
    return wrapped.toAttributePrefix(includeLastDot);
  }

  @Override
  public ValueSerializer<T> getValueSerializer() {
    return wrapped.getValueSerializer();
  }

  @Override
  public boolean isPublic() {
    return wrapped.isPublic();
  }

  @Override
  public Builder toBuilder(Repository repo) {
    return wrapped.toBuilder(repo);
  }

  @Override
  public String toAttributePrefix() {
    return wrapped.toAttributePrefix();
  }

  @Override
  public boolean isProxy() {
    return wrapped.isProxy();
  }

  @Override
  public TransactionMode getTransactionMode() {
    return wrapped.getTransactionMode();
  }

  @Override
  public AttributeProxyDescriptor<T> asProxy() throws ClassCastException {
    return wrapped.asProxy();
  }

  @Override
  public Optional<T> valueOf(StreamElement el) {
    return wrapped.valueOf(el);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return wrapped.equals(obj);
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}
