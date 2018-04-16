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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import java.io.IOException;

/**
 * A {@link DataSink} created from {@link OnlineAttributeWriter}.
 */
public class StreamElementSink<T> implements DataSink<StreamElement> {

  /**
   * Create sink for given attribute.
   * @param <T> type parameter
   * @param attribute attribute to create writer for
   * @param writer the writer for attribute
   * @return sink
   */
  public static <T> StreamElementSink<T> of(
      AttributeDescriptor<T> attribute,
      OnlineAttributeWriter writer) {

    return new StreamElementSink<>(attribute, writer);
  }

  private final AttributeDescriptor<T> attr;
  private final OnlineAttributeWriter writer;
  private final ValueSerializer<T> serializer;

  private StreamElementSink(AttributeDescriptor<T> attr, OnlineAttributeWriter writer) {
    this.attr = attr;
    this.writer = writer;
    this.serializer = attr.getValueSerializer();
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
