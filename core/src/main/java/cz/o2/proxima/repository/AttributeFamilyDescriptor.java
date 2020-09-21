/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import com.google.common.collect.Lists;
import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.repository.DefaultConsumerNameFactory.DefaultReplicationConsumerNameFactory;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.util.Classpath;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/** A family of attributes with the same storage. */
@Evolving
@Slf4j
public class AttributeFamilyDescriptor implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String CFG_REPLICATION_CONSUMER_NAME_GENERATOR =
      "replication.consumer.name.factory";

  @Getter private final StorageType type;

  public static Builder newBuilder() {
    return new Builder();
  }

  @Getter private final String name;

  @Getter private final EntityDescriptor entity;

  @Getter private final URI storageUri;

  /** Access type allowed to this family. */
  @Getter private final AccessType access;

  private final List<AttributeDescriptor<?>> attributes;
  @Getter private final StorageFilter filter;
  @Nullable private final String source;

  @Getter
  private final ConsumerNameFactory<AttributeFamilyDescriptor> replicationConsumerNameFactory;

  private final Map<String, Object> cfg;

  AttributeFamilyDescriptor(
      String name,
      EntityDescriptor entity,
      StorageType type,
      URI storageUri,
      Map<String, Object> cfg,
      Collection<AttributeDescriptor<?>> attributes,
      AccessType access,
      @Nullable StorageFilter filter,
      @Nullable String source) {

    this.name = Objects.requireNonNull(name);
    this.entity = Objects.requireNonNull(entity);
    this.type = Objects.requireNonNull(type);
    this.storageUri = Objects.requireNonNull(storageUri);
    this.cfg = Collections.unmodifiableMap(Objects.requireNonNull(cfg));
    this.attributes = Lists.newArrayList(Objects.requireNonNull(attributes));
    this.access = Objects.requireNonNull(access);
    this.filter = filter;
    this.source = source;

    this.replicationConsumerNameFactory =
        constructConsumerNameFactory(
            CFG_REPLICATION_CONSUMER_NAME_GENERATOR, DefaultReplicationConsumerNameFactory.class);
  }

  private ConsumerNameFactory<AttributeFamilyDescriptor> constructConsumerNameFactory(
      String configKey,
      Class<? extends ConsumerNameFactory<AttributeFamilyDescriptor>> defaultClass) {
    String consumerNameFactoryClass =
        this.getCfg().getOrDefault(configKey, defaultClass.getName()).toString();

    @SuppressWarnings("unchecked")
    ConsumerNameFactory<AttributeFamilyDescriptor> factory =
        Classpath.newInstance(consumerNameFactoryClass, ConsumerNameFactory.class);
    log.debug(
        "Using {} class as consumer name generator for attribute family {}.",
        factory.getClass().getName(),
        this.name);
    factory.setup(this);
    return factory;
  }

  public Map<String, Object> getCfg() {
    return Collections.unmodifiableMap(cfg);
  }

  public List<AttributeDescriptor<?>> getAttributes() {
    return Collections.unmodifiableList(attributes);
  }

  @Override
  public String toString() {
    return "AttributeFamily(name=" + name + ", attributes=" + attributes + ", type=" + type + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AttributeFamilyDescriptor) {
      AttributeFamilyDescriptor other = (AttributeFamilyDescriptor) obj;
      return other.name.equals(name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  /**
   * Retrieve optional name of source attribute family, if this is replica. The source might not be
   * explicitly specified (in which case this method returns {@code Optional.empty()} and the source
   * is determined automatically.
   *
   * @return optional specified source family
   */
  public Optional<String> getSource() {
    return Optional.ofNullable(source);
  }

  /**
   * Check if this proxied family.
   *
   * @return {@code true} if proxied family
   */
  public boolean isProxy() {
    return false;
  }

  /**
   * Convert this object to proxy descriptor.
   *
   * @return this object cast to {@link AttributeFamilyProxyDescriptor}
   */
  public AttributeFamilyProxyDescriptor toProxy() {
    throw new ClassCastException(
        getClass() + " cannot be cast to " + AttributeFamilyProxyDescriptor.class);
  }

  Builder toBuilder() {
    Builder ret =
        new Builder()
            .setAccess(access)
            .setFilter(filter)
            .setName(name)
            .setSource(source)
            .setEntity(entity)
            .setStorageUri(storageUri)
            .setType(type);

    if (isProxy()) {
      ret.setProxy(true)
          .setReadFamily(toProxy().getTargetFamilyRead())
          .setWriteFamily(toProxy().getTargetFamilyWrite());
    }

    attributes.forEach(ret::addAttribute);
    return ret;
  }

  @Accessors(chain = true)
  public static final class Builder {

    private final List<AttributeDescriptor<?>> attributes = new ArrayList<>();

    @Setter private String name;

    @Setter private EntityDescriptor entity;

    @Setter private URI storageUri;

    @Setter private StorageType type;

    @Setter private AccessType access;

    @Setter private StorageFilter filter = PassthroughFilter.INSTANCE;

    @Setter private String source;

    @Setter private Map<String, Object> cfg = new HashMap<>();

    @Setter private boolean proxy = false;

    @Setter private AttributeFamilyDescriptor readFamily = null;

    @Setter private AttributeFamilyDescriptor writeFamily = null;

    private Builder() {}

    public Builder clearAttributes() {
      attributes.clear();
      return this;
    }

    public Builder addAttribute(AttributeDescriptor<?> desc) {
      attributes.add(desc);
      return this;
    }

    public AttributeFamilyDescriptor build() {
      if (proxy) {
        List<AttributeProxyDescriptor<?>> attrs =
            attributes.stream().map(AttributeDescriptor::asProxy).collect(Collectors.toList());

        return AttributeFamilyProxyDescriptor.of(attrs, readFamily, writeFamily);
      }
      return new AttributeFamilyDescriptor(
          name, entity, type, storageUri, cfg, attributes, access, filter, source);
    }
  }
}
