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
package cz.o2.proxima.direct.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import lombok.Getter;
import lombok.ToString;
import org.apache.hadoop.fs.FileStatus;

/** A {@link Path} implementation for hadoop. */
@Internal
@ToString
class HadoopPath implements Path {

  /** Create new {@link HadoopPath} from given path and configuration. */
  static HadoopPath of(HadoopFileSystem fs, String path, HadoopDataAccessor accessor) {
    return new HadoopPath(fs, path, accessor);
  }

  private final HadoopFileSystem fs;
  @Getter private final URI path;
  private final HadoopDataAccessor accessor;

  private HadoopPath(HadoopFileSystem fs, String path, HadoopDataAccessor accessor) {
    this.fs = fs;
    this.path = HadoopStorage.remap(URI.create(path));
    this.accessor = accessor;

    Preconditions.checkArgument(
        this.path.isAbsolute() && !Strings.isNullOrEmpty(this.path.getScheme()),
        "Passed path must be absolute URL, got [%s]",
        path);
  }

  @Override
  public InputStream reader() throws IOException {
    org.apache.hadoop.fs.Path p = toPath();
    return p.getFileSystem(accessor.getHadoopConf()).open(p);
  }

  @Override
  public OutputStream writer() throws IOException {
    org.apache.hadoop.fs.Path p = toPath();
    return p.getFileSystem(accessor.getHadoopConf()).create(p);
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public void delete() throws IOException {
    org.apache.hadoop.fs.Path p = toPath();
    p.getFileSystem(accessor.getHadoopConf()).delete(p, true);
  }

  public void move(HadoopPath target) throws IOException {
    org.apache.hadoop.fs.Path sourcePath = toPath();
    org.apache.hadoop.fs.Path targetPath = target.toPath();
    targetPath.getFileSystem(accessor.getHadoopConf()).rename(sourcePath, targetPath);
  }

  public FileStatus getFileStatus() {
    org.apache.hadoop.fs.Path p = toPath();
    return ExceptionUtils.uncheckedFactory(
        () -> p.getFileSystem(accessor.getHadoopConf()).getFileStatus(p));
  }

  private org.apache.hadoop.fs.Path toPath() {
    return new org.apache.hadoop.fs.Path(path);
  }

  public boolean isTmpPath() {
    return path.toString().startsWith(accessor.getUriRemapped() + "/_tmp");
  }
}
