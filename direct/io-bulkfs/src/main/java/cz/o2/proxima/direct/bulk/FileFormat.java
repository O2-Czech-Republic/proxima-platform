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
package cz.o2.proxima.direct.bulk;

import cz.o2.proxima.repository.EntityDescriptor;
import java.io.IOException;
import java.io.Serializable;

/** A specific format of data stored in bulk storage. */
public interface FileFormat extends Serializable {

  /**
   * Create blob file format with specified {@link FileSystem} as root.
   *
   * @param writeGzip whether produced files should be compressed using gzip
   * @return {@link BinaryBlobFormat} as {@link FileFormat}.
   */
  static FileFormat blob(boolean writeGzip) {
    return new BinaryBlobFormat(writeGzip);
  }

  /**
   * Create json file format.
   *
   * @param gzip read/write gzipped data
   * @return {@link }JsonFormat}
   */
  static FileFormat json(boolean gzip) {
    return new JsonFormat(gzip);
  }

  /**
   * Open reader for data stored at given {@link Path}
   *
   * @param path {@link Path} on associated {@link FileSystem}
   * @param entity descriptor of entity whose data we are going to read
   * @return reader of the data
   * @throws IOException on errors
   */
  Reader openReader(Path path, EntityDescriptor entity) throws IOException;

  /**
   * Open writer for data on given {@link Path}.
   *
   * @param path {@link Path} on associated {@link FileSystem}
   * @param entity descriptor of entity whose data we are going to write
   * @return writer for the data
   * @throws IOException on errors
   */
  Writer openWriter(Path path, EntityDescriptor entity) throws IOException;

  /** Retrieve suffix of file names. */
  String fileSuffix();
}
