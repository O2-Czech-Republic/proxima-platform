/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.util;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SerializableLong implements Serializable {

  private static Map<String, Long> VALUES = new ConcurrentHashMap<>();
  private final String uuid = UUID.randomUUID().toString();

  public SerializableLong(long value) {
    VALUES.put(uuid, value);
  }

  public long get() {
    return VALUES.get(uuid);
  }

  public void set(long value) {
    VALUES.put(uuid, value);
  }

  public long incrementAndGet() {
    return VALUES.compute(uuid, (k, v) -> v == null ? 1 : v + 1);
  }
}
