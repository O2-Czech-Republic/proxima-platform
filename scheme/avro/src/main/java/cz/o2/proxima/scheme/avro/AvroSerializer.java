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
package cz.o2.proxima.scheme.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Basic Avro serializer. Currently support just Specific avro record
 *
 * @param <T> SpecificAvroRecord - expected class
 */
public class AvroSerializer<T extends GenericContainer> {
  private final SpecificDatumWriter<T> writer;
  private final SpecificDatumReader<T> reader;

  public AvroSerializer(Schema schema) {
    this.writer = new SpecificDatumWriter<>(schema);
    this.reader = new SpecificDatumReader<>(schema);
  }

  public byte[] serialize(T input) throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      serialize(input, out);
      return out.toByteArray();
    }
  }

  public void serialize(T input, ByteArrayOutputStream out) throws IOException {
    final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(input, encoder);
    encoder.flush();
  }

  public T deserialize(byte[] input) throws IOException {
    return reader.read(null, DecoderFactory.get().binaryDecoder(input, null));
  }

  public T deserialize(ByteBuffer buffer, int start, int len) throws IOException {
    return reader.read(null, DecoderFactory.get().binaryDecoder(buffer.array(), start, len, null));
  }
}
