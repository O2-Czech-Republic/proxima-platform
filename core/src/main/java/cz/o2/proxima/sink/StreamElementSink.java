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
package cz.o2.proxima.sink;

import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import java.io.IOException;

/**
 * A {@link DataSink} created from {@link OnlineAttributeWriter}.
 */
@Experimental("Not well tested")
public class StreamElementSink implements DataSink<StreamElement> {

  /**
   * Create sink for given attribute.
   * @param writer the writer for attribute
   * @return sink
   */
  public static StreamElementSink of(OnlineAttributeWriter writer) {
    return new StreamElementSink(writer);
  }

  private final OnlineAttributeWriter writer;

  private StreamElementSink(OnlineAttributeWriter writer) {
    this.writer = writer;
  }

  @Override
  public Writer<StreamElement> openWriter(int i) {
    return new Writer<StreamElement>() {
      @Override
      public void write(StreamElement e) throws IOException {
        writer.write(e, (succ, exc) -> {
          if (!succ) {
            throw new RuntimeException(exc);
          }
        });
      }

      @Override
      public void commit() throws IOException {
        // nop
      }

      @Override
      public void close() throws IOException {
        // FIXME: writer missing close?
      }

    };
  }

  @Override
  public void commit() throws IOException {
    // nop
  }

  @Override
  public void rollback() throws IOException {
    writer.rollback();
  }

}
