/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.bulkfs.parquet;

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.io.bulkfs.Path;
import cz.o2.proxima.direct.io.bulkfs.Reader;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.AbstractIterator;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

@Slf4j
public class ProximaParquetReader implements Reader {

  private final Path path;
  private final ParquetReader<StreamElement> reader;

  public ProximaParquetReader(Path path, EntityDescriptor entity) throws IOException {
    final SeekableByteChannel channel = (SeekableByteChannel) path.read();
    final Configuration configuration = new Configuration(false);
    this.reader =
        new ParquetReadBuilder(new BulkInputFile(channel), entity)
            .withConf(configuration)
            // Currently we can not use push down filter for attributes See
            // https://github.com/O2-Czech-Republic/proxima-platform/issues/196 for details
            // .withFilter()
            .build();
    this.path = path;
  }

  @Override
  public void close() {
    ExceptionUtils.unchecked(reader::close);
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public Iterator<StreamElement> iterator() {
    return new AbstractIterator<StreamElement>() {
      @Override
      protected StreamElement computeNext() {
        try {
          StreamElement element = reader.read();
          if (element == null) {
            return endOfData();
          } else {
            return element;
          }
        } catch (IOException e) {
          throw new IllegalStateException("Unable to compute next element.", e);
        }
      }
    };
  }

  private static class ParquetReadBuilder extends ParquetReader.Builder<StreamElement> {

    private final EntityDescriptor entity;

    ParquetReadBuilder(InputFile file, EntityDescriptor entity) {
      super(file);
      this.entity = entity;
    }

    @Override
    protected ReadSupport<StreamElement> getReadSupport() {
      Preconditions.checkNotNull(entity, "Entity must be specified.");
      return new StreamElementReadSupport(entity);
    }
  }

  private static class StreamElementReadSupport extends ReadSupport<StreamElement> {

    private final EntityDescriptor entity;

    public StreamElementReadSupport(EntityDescriptor entity) {
      this.entity = entity;
    }

    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<StreamElement> prepareForRead(
        Configuration configuration,
        Map<String, String> keyValueMetaData,
        MessageType fileSchema,
        ReadContext readContext) {
      final String attributeNamesPrefix =
          keyValueMetaData.getOrDefault(
              ParquetFileFormat.PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME, "");
      return new StreamElementMaterializer(fileSchema, entity, attributeNamesPrefix);
    }
  }

  private static class BulkInputFile implements InputFile {

    private final SeekableByteChannel channel;

    BulkInputFile(SeekableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public long getLength() throws IOException {
      return channel.size();
    }

    @Override
    public SeekableInputStream newStream() {
      return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {

        @Override
        public long getPos() throws IOException {
          return channel.position();
        }

        @Override
        public void seek(long newPosition) throws IOException {
          channel.position(newPosition);
        }
      };
    }
  }
}
