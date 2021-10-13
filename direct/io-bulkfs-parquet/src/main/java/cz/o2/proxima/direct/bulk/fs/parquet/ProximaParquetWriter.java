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
package cz.o2.proxima.direct.bulk.fs.parquet;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Writer;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;

/** {@link Writer} for writing data in parquet format. */
@Internal
public class ProximaParquetWriter implements Writer {

  private final Path path;
  private final ParquetWriter<StreamElement> writer;

  public ProximaParquetWriter(
      Path path,
      MessageType schema,
      String attributeNamesPrefix,
      CompressionCodecName compressionCodecName,
      Configuration config)
      throws IOException {
    this.path = path;
    this.writer =
        new ParquetWriterBuilder(new BulkOutputFile(path.writer()), schema, attributeNamesPrefix)
            .withConf(config)
            .withWriteMode(Mode.OVERWRITE)
            // For some reason Writer ignores settings this via withConf()
            .withCompressionCodec(compressionCodecName)
            .withRowGroupSize(
                config.getInt(
                    ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME,
                    ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE))
            .withPageSize(
                config.getInt(
                    ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME,
                    ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE))
            .build();
  }

  @Override
  public void write(StreamElement elem) throws IOException {
    try {
      writer.write(elem);
    } catch (InvalidRecordException iex) {
      throw new IllegalArgumentException("Unable to write StreamElement.", iex);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public Path getPath() {
    return path;
  }

  private static class ParquetWriterBuilder
      extends ParquetWriter.Builder<StreamElement, ParquetWriterBuilder> {

    private final MessageType parquetSchema;
    private final String attributeNamesPrefix;

    private ParquetWriterBuilder(
        OutputFile outputFile, MessageType parquetSchema, String attributeNamesPrefix) {
      super(outputFile);
      this.parquetSchema = parquetSchema;
      this.attributeNamesPrefix = attributeNamesPrefix;
    }

    @Override
    protected ParquetWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<StreamElement> getWriteSupport(Configuration conf) {
      return new StreamElementWriteSupport(parquetSchema, attributeNamesPrefix);
    }
  }

  private static class BulkOutputFile implements OutputFile {

    private final OutputStream outputStream;

    BulkOutputFile(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new BulkOutputStream(outputStream);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return new BulkOutputStream(outputStream);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  private static class BulkOutputStream extends PositionOutputStream {

    private final OutputStream delegate;
    private long position = 0;

    private BulkOutputStream(OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void write(int b) throws IOException {
      position++;
      delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
      position += len;
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
