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
package cz.o2.proxima.direct.hdfs;

import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import java.net.URI;
import java.util.stream.Stream;

/** A {@link FileSystem} implementation for hadoop. */
class HadoopFileSystem implements FileSystem {

  private final HdfsDataAccessor accessor;
  private final NamingConvention namingConvention;
  private transient org.apache.hadoop.fs.FileSystem fs = null;

  HadoopFileSystem(HdfsDataAccessor accessor) {
    this(accessor, accessor.getNamingConvention());
  }

  HadoopFileSystem(HdfsDataAccessor accessor, NamingConvention namingConvention) {
    this.accessor = accessor;
    this.namingConvention = namingConvention;
  }

  @Override
  public URI getUri() {
    return accessor.getUri();
  }

  @Override
  public Stream<Path> list(long minTs, long maxTs) {
    return null;
  }

  @Override
  public Path newPath(long ts) {
    String path = getUri() + namingConvention.nameOf(ts);
    return HadoopPath.of(this, path, accessor);
  }

  private org.apache.hadoop.fs.FileSystem fs() {
    if (fs == null) {
      fs = accessor.getFs();
    }
    return fs;
  }

  @Override
  public int hashCode() {
    return getUri().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HadoopFileSystem) {
      HadoopFileSystem other = (HadoopFileSystem) obj;
      return other.getUri().equals(getUri());
    }
    return false;
  }
}
