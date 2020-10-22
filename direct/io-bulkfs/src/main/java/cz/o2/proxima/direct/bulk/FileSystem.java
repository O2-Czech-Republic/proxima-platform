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
import cz.o2.proxima.util.ExceptionUtils;
import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** A proxima's abstraction of bulk FS. */
@Internal
public interface FileSystem extends Serializable {

  static FileSystem local(File parent, NamingConvention convention) {
    return new FileSystem() {

      private static final long serialVersionUID = 1L;

      @Override
      public URI getUri() {
        return URI.create("file://" + parent.getAbsolutePath());
      }

      @Override
      public Stream<Path> list(long minTs, long maxTs) {
        return listRecursive(parent)
            .filter(f -> convention.isInRange(f.getAbsolutePath(), minTs, maxTs))
            .map(p -> Path.local(this, p));
      }

      private Stream<File> listRecursive(File file) {
        List<Stream<File>> parts = new ArrayList<>();
        if (file.isDirectory()) {
          for (File f : file.listFiles()) {
            parts.add(listRecursive(f));
          }
          return parts.stream().reduce(Stream.empty(), Stream::concat);
        }
        return Stream.of(file);
      }

      @Override
      public LocalPath newPath(long ts) {
        String name = convention.nameOf(ts);
        return ExceptionUtils.uncheckedFactory(() -> Path.local(this, new File(parent, name)));
      }
    };
  }

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
   * Create new {@link Path}.
   *
   * @param ts timestamp to create the path for
   * @return new abstract {@link Path} object
   */
  Path newPath(long ts);

  /**
   * Get all {@link Path Paths} on this FileSystem
   *
   * @return all paths on this FileSystem
   */
  default Stream<Path> list() {
    return list(Long.MIN_VALUE, Long.MAX_VALUE);
  }
}
