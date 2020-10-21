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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;

/** A {@link Path} implementation for hadoop. */
@Internal
@Slf4j
class HadoopPath implements Path {

  private static final long serialVersionUID = 1L;

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

  @Override
  public String toString() {
    return "HadoopPath(" + path.toASCIIString() + ")";
  }

  public void move(HadoopPath target) throws IOException {
    if (this.getFileSystem().getUri().equals(target.getFileSystem().getUri())) {
      renameOnFs(target);
    } else {
      moveToRemote(target);
    }
  }

  @VisibleForTesting
  void renameOnFs(HadoopPath target) throws IOException {
    org.apache.hadoop.fs.Path sourcePath = toPath();
    org.apache.hadoop.fs.Path targetPath = target.toPath();
    org.apache.hadoop.fs.Path parentPath = targetPath.getParent();
    org.apache.hadoop.fs.FileSystem hadoopFs = targetPath.getFileSystem(accessor.getHadoopConf());
    if (!hadoopFs.exists(parentPath)) {
      Preconditions.checkState(hadoopFs.mkdirs(parentPath), "Failed to mkdirs on %s", parentPath);
    }
    hadoopFs.rename(sourcePath, targetPath);
    log.debug("Renamed {} to {}", sourcePath, target);
  }

  @VisibleForTesting
  void moveToRemote(HadoopPath target) throws IOException {
    try (InputStream in = reader();
        OutputStream output = target.writer()) {
      IOUtils.copy(in, output);
    }
    this.delete();
  }

  public FileStatus getFileStatus() throws IOException {
    org.apache.hadoop.fs.Path p = toPath();
    return p.getFileSystem(accessor.getHadoopConf()).getFileStatus(p);
  }

  private org.apache.hadoop.fs.Path toPath() {
    return new org.apache.hadoop.fs.Path(path);
  }

  public boolean isTmpPath() {
    return path.toString().startsWith(accessor.getUriRemapped() + "/_tmp");
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof HadoopPath)) {
      return false;
    }
    HadoopPath other = (HadoopPath) obj;
    return other.getPath().equals(getPath());
  }

  @Override
  public int compareTo(Path other) {
    return path.compareTo(((HadoopPath) other).getPath());
  }
}
