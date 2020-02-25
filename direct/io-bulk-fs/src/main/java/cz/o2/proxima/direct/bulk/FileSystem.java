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
package cz.o2.proxima.direct.bulk;

import cz.o2.proxima.annotations.Internal;
import java.net.URI;
import java.util.stream.Stream;

/** A proxima's abstraction of bulk FS. */
@Internal
public interface FileSystem {

  /**
   * Ger {@link URI} representation of this FileSystem
   *
   * @return the URI of this FileSystem
   */
  URI getUri();

  /**
   * Return stream of {@link Path Paths} that represent given time range.
   *
   * @param minTs minimal allowed timestamp (inclusive)
   * @param maxTs maximal allowd timestamp (exclusive)
   * @return stream of Paths satisfying given time range
   */
  Stream<Path> list(long minTs, long maxTs);

  /**
   * Create new {@link Path} (without yet assigned time range).
   *
   * @return new abstract {@link Path} object
   */
  Path newPath();

  /**
   * Get all {@link Path Paths} on this FileSystem
   *
   * @return all paths on this FileSystem
   */
  default Stream<Path> list() {
    return list(Long.MIN_VALUE, Long.MAX_VALUE);
  }
}
