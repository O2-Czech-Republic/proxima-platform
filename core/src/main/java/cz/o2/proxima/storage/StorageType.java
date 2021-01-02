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

import cz.o2.proxima.annotations.Stable;
import java.util.Objects;
import lombok.Getter;

/** Types of storage. */
@Stable
public enum StorageType {

  /** A storage where each write is persisted at first. */
  PRIMARY("primary"),

  /**
   * A storage where each write is replicated from commit storage. Note that is attribute has
   * multiple storages, than exactly one has to be commit and the commit has to be of type
   * commit-log.
   */
  REPLICA("replica");

  public static StorageType of(String name) {
    Objects.requireNonNull(name);
    for (StorageType t : StorageType.values()) {
      if (t.cfgName.equalsIgnoreCase(name)) {
        return t;
      }
    }
    throw new IllegalArgumentException("Unknown storage type " + name);
  }

  @Getter private final String cfgName;

  StorageType(String cfgName) {
    this.cfgName = Objects.requireNonNull(cfgName);
  }
}
