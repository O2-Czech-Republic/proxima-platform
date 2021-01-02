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
package cz.o2.proxima.direct.hadoop;

import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

/** A {@link FileSystem} implementation for hadoop. */
class HadoopFileSystem implements FileSystem {

  private static final long serialVersionUID = 1L;

  private final URI uri;
  private final HadoopDataAccessor accessor;
  private final NamingConvention namingConvention;
  private transient org.apache.hadoop.fs.FileSystem fs = null;

  HadoopFileSystem(HadoopDataAccessor accessor) {
    this(accessor, accessor.getNamingConvention());
  }

  HadoopFileSystem(HadoopDataAccessor accessor, NamingConvention namingConvention) {
    this(accessor.getUri(), accessor, namingConvention);
  }

  HadoopFileSystem(URI uri, HadoopDataAccessor accessor, NamingConvention namingConvention) {
    this.uri = uri;
    this.accessor = accessor;
    this.namingConvention = namingConvention;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Stream<Path> list(long minTs, long maxTs) {
    URI remappedUri = HadoopStorage.remap(getUri());
    RemoteIterator<LocatedFileStatus> iterator =
        ExceptionUtils.uncheckedFactory(
            () -> fs().listFiles(new org.apache.hadoop.fs.Path(remappedUri), true));
    Spliterator<LocatedFileStatus> spliterator = asSpliterator(iterator);
    return StreamSupport.stream(spliterator, false)
        .filter(LocatedFileStatus::isFile)
        .map(f -> f.getPath().toUri().toString().substring(remappedUri.toString().length()))
        .filter(name -> namingConvention.isInRange(name, minTs, maxTs))
        .map(name -> HadoopPath.of(this, remappedUri + name, accessor));
  }

  private Spliterator<LocatedFileStatus> asSpliterator(RemoteIterator<LocatedFileStatus> iterator) {
    return new Spliterators.AbstractSpliterator<LocatedFileStatus>(-1, 0) {

      @Override
      public boolean tryAdvance(Consumer<? super LocatedFileStatus> consumer) {
        if (ExceptionUtils.uncheckedFactory(iterator::hasNext).booleanValue()) {
          consumer.accept(ExceptionUtils.uncheckedFactory(iterator::next));
          return true;
        }
        return false;
      }
    };
  }

  @Override
  public Path newPath(long ts) {
    String path = HadoopStorage.remap(getUri()) + namingConvention.nameOf(ts);
    return HadoopPath.of(this, path, accessor);
  }

  private org.apache.hadoop.fs.FileSystem fs() {
    if (fs == null) {
      fs = accessor.getFsFor(HadoopStorage.remap(getUri()));
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
