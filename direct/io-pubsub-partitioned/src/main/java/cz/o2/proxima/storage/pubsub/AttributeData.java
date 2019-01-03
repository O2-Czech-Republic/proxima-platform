/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.pubsub;

import cz.seznam.euphoria.core.annotation.stability.Experimental;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Class holding data for attribute of entity.
 */
@Experimental
@Value
public class AttributeData {

  public AttributeData(String key, String attribute, byte[] value, long stamp) {
    this(key, attribute, value, false, false, stamp);
  }

  public AttributeData(
      String key, String attribute, byte[] value,
      boolean delete, boolean deleteWildcard, long stamp) {

    this.key = key;
    this.attribute = attribute;
    this.value = value;
    this.delete = delete;
    this.deleteWildcard = deleteWildcard;
    this.stamp = stamp;
  }

  /** Key of entity. */
  final String key;

  /** Name of attribute. */
  final String attribute;

  /** Serialized value. */
  @Nullable
  final byte[] value;

  /** Timestamp. */
  final long stamp;

  /** Delete wildcard attribute. */
  final boolean deleteWildcard;

  /** Delete single attribute. */
  final boolean delete;

  public boolean isDelete() {
    return delete || deleteWildcard;
  }

}
