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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.time.Duration;

/** Proxima's abstraction of path in {@link FileSystem}. */
@Internal
public interface Path extends Serializable {

  /**
   * Create a Path representation of the given {@link File local file}.
   *
   * @param fs local filesystem
   * @param path the local file
   * @return Path representation of this local file
   */
  static Path local(FileSystem fs, File path) {
    return new Path() {

      private static final long serialVersionUID = 1L;

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
        if (path.exists() && !path.delete()) {
          throw new IOException("Failed to delete " + path);
        }
      }

      @Override
      public String toString() {
        return path.getAbsolutePath();
      }
    };
  }

  static Path stdin(FileFormat format) {
    return new Path() {

      private static final long serialVersionUID = 1L;

      @Override
      public InputStream reader() throws IOException {
        return System.in;
      }

      @Override
      public OutputStream writer() {
        throw new UnsupportedOperationException("Can only read from stdin.");
      }

      @Override
      public FileSystem getFileSystem() {
        return FileSystem.local(
            new File("/dev/stdin"),
            NamingConvention.defaultConvention(Duration.ofHours(1), "prefix", format.fileSuffix()));
      }

      @Override
      public void delete() throws IOException {
        throw new UnsupportedOperationException("Cannot delete stdin.");
      }
    };
  }

  /**
   * Open input stream from given Path.
   *
   * @return {@link InputStream} of the {@link Path}.
   * @throws IOException on errors
   */
  InputStream reader() throws IOException;

  /**
   * Open output stream to the Path.
   *
   * @return {@link OutputStream} of the {@link Path}
   * @throws IOException on errors *
   */
  OutputStream writer() throws IOException;

  /**
   * Retrieve {@link FileSystem} of this Path.
   *
   * @return {@link FileSystem} associated with the {@link Path}.
   */
  FileSystem getFileSystem();

  /**
   * Delete this {@link Path}.
   *
   * @throws IOException on errors
   */
  void delete() throws IOException;
}
