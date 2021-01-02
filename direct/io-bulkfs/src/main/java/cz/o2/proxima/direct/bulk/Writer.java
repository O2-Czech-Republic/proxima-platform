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

import cz.o2.proxima.storage.StreamElement;
import java.io.Closeable;
import java.io.IOException;

/** Proxima's abstraction of writer for bulk data. */
public interface Writer extends Closeable {

  /**
   * Write {@link StreamElement} into this blob store.
   *
   * @param elem the element to write
   * @throws IOException on errors
   */
  void write(StreamElement elem) throws IOException;

  /**
   * Close and flush the writer to target {@link FileSystem}. Note that the associated {@link Path}
   * might be visible on the target FileSystem only after closing associated writer.
   *
   * @throws IOException on errors
   */
  @Override
  void close() throws IOException;

  /**
   * Get {@link Path} associated with the Writer.
   *
   * @return the path of this writer
   */
  Path getPath();
}
