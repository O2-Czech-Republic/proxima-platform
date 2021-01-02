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

import java.util.Objects;
import lombok.AccessLevel;
import lombok.Getter;

/** Default consumer name generator */
abstract class DefaultConsumerNameFactory<T> implements ConsumerNameFactory<T> {

  public static final String CFG_REPLICATION_CONSUMER_NAME_SUFFIX =
      "replication.consumer.name.suffix";
  public static final String CFG_REPLICATION_CONSUMER_NAME_PREFIX =
      "replication.consumer.name.prefix";
  public static final String CFG_TRANSFORMER_CONSUMER_NAME_PREFIX =
      "transformer.consumer.name.prefix";
  public static final String CFG_TRANSFORMER_CONSUMER_NAME_SUFFIX =
      "transformer.consumer.name.suffix";

  @Getter(AccessLevel.PACKAGE)
  private final String namePrefix;

  @Getter(AccessLevel.PACKAGE)
  private final String prefixCfgKey;

  @Getter(AccessLevel.PACKAGE)
  private final String suffixCfgKey;

  String name;
  String prefix;
  String suffix;

  public DefaultConsumerNameFactory(String namePrefix, String prefixCfgKey, String suffixCfgKey) {
    this.namePrefix = namePrefix;
    this.prefixCfgKey = prefixCfgKey;
    this.suffixCfgKey = suffixCfgKey;
  }

  @Override
  public String apply() {
    return prefix + namePrefix + name + suffix;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DefaultConsumerNameFactory) {
      DefaultConsumerNameFactory<?> that = (DefaultConsumerNameFactory<?>) obj;
      return prefix.equals(that.prefix)
          && name.equals(that.name)
          && suffix.equals(that.suffix)
          && namePrefix.equals(that.namePrefix);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, suffix, name, namePrefix);
  }

  /** Default implementation for replication */
  public static class DefaultReplicationConsumerNameFactory
      extends DefaultConsumerNameFactory<AttributeFamilyDescriptor> {

    public DefaultReplicationConsumerNameFactory() {
      super(
          "consumer-", CFG_REPLICATION_CONSUMER_NAME_PREFIX, CFG_REPLICATION_CONSUMER_NAME_SUFFIX);
    }

    @Override
    public void setup(AttributeFamilyDescriptor descriptor) {
      this.name = descriptor.getName();
      this.prefix = descriptor.getCfg().getOrDefault(getPrefixCfgKey(), "").toString();
      this.suffix = descriptor.getCfg().getOrDefault(getSuffixCfgKey(), "").toString();
    }
  }
}
