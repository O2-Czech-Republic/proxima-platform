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
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;

/** Proxima's abstraction of path in {@link FileSystem}. */
@Internal
public interface Path extends Serializable {

  /**
   * Create a Path representation of the given {@link File local file}.
   *
   * @param path the local file
   * @return Path representation of this local file
   */
  static Path local(File path) {
    return new Path() {

      @Override
      public Reader openReader(EntityDescriptor entity) throws IOException {
        return BinaryBlob.reader(this, entity, new FileInputStream(path));
      }

      @Override
      public Writer newWriter(EntityDescriptor entity) throws IOException {
        return BinaryBlob.writer(this, false, new FileOutputStream(path));
      }

      @Override
      public File toFile() {
        return path;
      }
    };
  }

  static Path stdin() {
    return new Path() {

      @Override
      public Reader openReader(EntityDescriptor entity) throws IOException {
        return BinaryBlob.reader(this, entity, System.in);
      }

      @Override
      public Writer newWriter(EntityDescriptor entity) throws IOException {
        throw new UnsupportedOperationException("Can only read from stdin");
      }

      @Override
      public File toFile() {
        return new File("/dev/stdin");
      }
    };
  }

  /**
   * Open reader for given abstract Path.
   *
   * @param entity descriptor of entity whose data we are going to read
   * @return reader opened for reading
   * @throws IOException on errors
   */
  Reader openReader(EntityDescriptor entity) throws IOException;

  /**
   * Open abstract {@link Writer} for bulk data.
   *
   * @param entity descriptor of entity whose data we are going to write
   * @return new writer
   * @throws IOException on errors *
   */
  Writer newWriter(EntityDescriptor entity) throws IOException;

  /** Return local representation of this (possibly remote) path. */
  File toFile();

  /** Retrieve {@link FileSystem} of this Path. */
}
