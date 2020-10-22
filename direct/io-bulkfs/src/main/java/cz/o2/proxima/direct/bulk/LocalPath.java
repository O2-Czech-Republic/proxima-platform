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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.annotation.Nonnull;
import lombok.Getter;

public class LocalPath implements Path {

  private static final long serialVersionUID = 1L;
  @Getter private final File path;
  private final FileSystem fs;

  public LocalPath(File path, FileSystem fs) {
    this.path = path;
    this.fs = fs;
  }

  @Override
  public InputStream reader() throws IOException {
    return new FileInputStream(path);
  }

  @Override
  public OutputStream writer() throws IOException {
    if (!path.getParentFile().exists() && !path.getParentFile().mkdirs()) {
      throw new IOException("Failed to create dir " + path.getParentFile());
    }
    return new FileOutputStream(path);
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public void delete() throws IOException {
    if (path.exists()) {
      Files.delete(Paths.get(path.toURI()));
    }
  }

  @Override
  public String toString() {
    return path.getAbsolutePath();
  }

  @Override
  public int compareTo(@Nonnull Path other) {
    return path.compareTo(((LocalPath) other).getPath());
  }
}
