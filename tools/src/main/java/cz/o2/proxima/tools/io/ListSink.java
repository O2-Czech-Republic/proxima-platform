/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.io;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;

/**
 * Sink collecting to local {@link List}.
 * This is only local implementation. Full blown implementation would
 * have to open some sort of HTTP server and collect results from
 * distributed workers.
 */
public class ListSink<T> implements DataSink<T> {

  @Getter
  private final List<T> result = Collections.synchronizedList(new ArrayList<>());

  @Override
  public Writer<T> openWriter(int partitionId) {
    return new Writer<T>() {
      @Override
      public void write(T elem) throws IOException {
        result.add(elem);
      }

      @Override
      public void commit() throws IOException {
        // nop
      }

      @Override
      public void close() throws IOException {
        // nop
      }

    };
  }

  @Override
  public void commit() throws IOException {
    // nop
  }

  @Override
  public void rollback() throws IOException {
    result.clear();
  }

}
