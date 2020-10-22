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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.protobuf.util.JsonFormat.Parser;
import com.google.protobuf.util.JsonFormat.Printer;
import cz.o2.proxima.gcloud.storage.proto.Serialization.JsonElement;
import cz.o2.proxima.gcloud.storage.proto.Serialization.JsonElement.Builder;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Format storing elements as JSON values, one per line. */
@Slf4j
public class JsonFormat implements FileFormat {

  private static final long serialVersionUID = 1L;

  private static final Parser parser = com.google.protobuf.util.JsonFormat.parser();
  private static final Printer printer = com.google.protobuf.util.JsonFormat.printer();

  private static class JsonReader extends AbstractIterator<StreamElement> implements Reader {

    private final EntityDescriptor entity;
    private final Path path;
    private final boolean gzip;
    private BufferedReader input;

    JsonReader(EntityDescriptor entity, Path path, boolean gzip) {
      this.entity = entity;
      this.path = path;
      this.gzip = gzip;
    }

    @Override
    public void close() {
      if (input != null) {
        ExceptionUtils.unchecked(() -> input.close());
        input = null;
      }
    }

    @Override
    public Path getPath() {
      return path;
    }

    @Override
    protected StreamElement computeNext() {
      if (input == null) {
        InputStream raw = ExceptionUtils.uncheckedFactory(path::reader);
        InputStream inputStream = raw;
        if (gzip) {
          inputStream = ExceptionUtils.uncheckedFactory(() -> new GZIPInputStream(raw));
        }
        this.input = new BufferedReader(new InputStreamReader(inputStream));
      }
      try {
        String line = input.readLine();
        if (line != null) {
          Builder builder = JsonElement.newBuilder();
          parser.merge(line, builder);
          return toStreamElement(builder.build(), entity);
        }
      } catch (EOFException ex) {
        // pass through
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      return endOfData();
    }

    @Override
    @Nonnull
    public Iterator<StreamElement> iterator() {
      this.close();
      return this;
    }
  }

  private static class JsonWriter implements Writer {

    private final Path path;
    private final boolean gzip;
    private BufferedWriter writer;

    JsonWriter(Path path, boolean gzip) {
      this.path = path;
      this.gzip = gzip;
    }

    @Override
    public void write(StreamElement elem) throws IOException {
      if (writer == null) {
        OutputStream out = path.writer();
        if (gzip) {
          out = new GZIPOutputStream(out);
        }
        writer = new BufferedWriter(new OutputStreamWriter(out));
      }
      writer.write(printer.print(toJsonElement(elem)).replace('\n', ' '));
      writer.write('\n');
      log.debug("Written element {} into json at {}", elem, path);
    }

    @Override
    public void close() throws IOException {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    }

    @Override
    public Path getPath() {
      return path;
    }
  }

  private final boolean gzip;

  JsonFormat(boolean gzip) {
    this.gzip = gzip;
  }

  @Override
  public Reader openReader(Path path, EntityDescriptor entity) {
    return new JsonReader(entity, path, gzip);
  }

  @Override
  public Writer openWriter(Path path, EntityDescriptor entity) {
    return new JsonWriter(path, gzip);
  }

  @Override
  public String fileSuffix() {
    return gzip ? "json.gz" : "json";
  }

  @VisibleForTesting
  static StreamElement toStreamElement(JsonElement element, EntityDescriptor entity) {
    AttributeDescriptor<Object> attr = entity.getAttribute(element.getAttribute());
    if (element.getDeleteWildcard()) {
      Preconditions.checkArgument(
          attr.isWildcard(), "Attribute [%s] is not wildcard attribute", element.getAttribute());
      return StreamElement.deleteWildcard(
          entity, attr, element.getUuid(), element.getKey(), element.getStamp());
    }
    if (element.getDelete()) {
      return StreamElement.delete(
          entity,
          attr,
          element.getUuid(),
          element.getKey(),
          element.getAttribute(),
          element.getStamp());
    }
    return StreamElement.upsert(
        entity,
        attr,
        element.getUuid(),
        element.getKey(),
        element.getAttribute(),
        element.getStamp(),
        attr.getValueSerializer()
            .serialize(attr.getValueSerializer().fromJsonValue(element.getValue())));
  }

  @VisibleForTesting
  static JsonElement toJsonElement(StreamElement element) {
    @SuppressWarnings("unchecked")
    ValueSerializer<Object> serializer =
        (ValueSerializer<Object>) element.getAttributeDescriptor().getValueSerializer();
    Builder builder =
        JsonElement.newBuilder()
            .setAttribute(element.getAttribute())
            .setUuid(element.getUuid())
            .setStamp(element.getStamp())
            .setKey(element.getKey());
    if (element.isDeleteWildcard()) {
      return builder.setDeleteWildcard(true).setDelete(true).build();
    }
    if (element.isDelete()) {
      return builder.setDelete(true).build();
    }
    Optional<?> maybeValue = element.getParsed();
    Preconditions.checkArgument(
        maybeValue.isPresent(), "Cannot deserialize value in [%s]", element);
    return builder.setValue(serializer.asJsonValue(maybeValue.get())).build();
  }
}
