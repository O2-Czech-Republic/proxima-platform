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
package cz.o2.proxima.direct.blob;

import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestBlobStorageAccessor extends BlobStorageAccessor {

  @Value
  class TestBlob implements BlobBase {

    private static final long serialVersionUID = 1L;

    String name;

    @Override
    public long getSize() {
      return Optional.ofNullable(outputStreams.get(name))
          .map(ByteArrayOutputStream::size)
          .orElse(0);
    }
  }

  class TestBlobPath extends BlobPath<TestBlob> {

    private static final long serialVersionUID = 1L;

    public TestBlobPath(FileSystem fs, TestBlob blob) {
      super(fs, blob);
    }

    @Override
    public InputStream reader() {
      ByteArrayOutputStream baos = outputStreams.get(getBlobName());
      ExceptionUtils.unchecked(baos::flush);
      return new ByteArrayInputStream(baos.toByteArray());
    }

    @Override
    public OutputStream writer() {
      return outputStreams.computeIfAbsent(getBlobName(), k -> new ByteArrayOutputStream());
    }

    @Override
    public void delete() {}
  }

  @FunctionalInterface
  interface Runnable extends Serializable {
    void run();
  }

  class TestBlobFileSystem implements FileSystem {

    private static final long serialVersionUID = 1L;

    private final @Nullable Runnable afterWrite;
    private final AtomicReference<Runnable> preWrite;

    TestBlobFileSystem() {
      this(null, new AtomicReference<>());
    }

    TestBlobFileSystem(@Nullable Runnable afterWrite, AtomicReference<Runnable> onWrite) {
      this.afterWrite = afterWrite;
      this.preWrite = onWrite;
    }

    @Override
    public URI getUri() {
      return TestBlobStorageAccessor.this.getUri();
    }

    @Override
    public Stream<Path> list(long minTs, long maxTs) {
      return outputStreams
          .entrySet()
          .stream()
          .filter(
              e ->
                  TestBlobStorageAccessor.this
                      .getNamingConvention()
                      .isInRange(e.getKey(), minTs, maxTs))
          .map(e -> new TestBlobPath(fs, new TestBlob(e.getKey())));
    }

    @Override
    public Path newPath(long ts) {
      return new TestBlobPath(fs, new TestBlob(getNamingConvention().nameOf(ts))) {
        @Override
        public OutputStream writer() {
          Optional.ofNullable(preWrite.get()).ifPresent(Runnable::run);
          OutputStream wrap = super.writer();
          return new OutputStream() {

            @Override
            public void write(int i) throws IOException {
              wrap.write(i);
            }

            @Override
            public void close() throws IOException {
              wrap.close();
              Optional.ofNullable(afterWrite).ifPresent(Runnable::run);
            }
          };
        }
      };
    }
  }

  class BlobWriter extends BulkBlobWriter<TestBlob, TestBlobStorageAccessor> {

    public BlobWriter(Context context) {
      super(TestBlobStorageAccessor.this, context);
    }

    @Override
    protected void deleteBlobIfExists(TestBlob blob) {}

    @Override
    public Factory<?> asFactory() {
      final Context context = getContext();
      final TestBlobStorageAccessor accessor = TestBlobStorageAccessor.this;
      return repo -> accessor.new BlobWriter(context);
    }
  }

  class BlobReader extends BlobLogReader<TestBlob, TestBlobPath> {

    public BlobReader(Context context) {
      super(TestBlobStorageAccessor.this, context);
    }

    @Override
    protected void runHandlingErrors(TestBlob blob, ThrowingRunnable runnable) {
      ExceptionUtils.unchecked(runnable::run);
    }

    @Override
    protected BlobPath<TestBlob> createPath(TestBlob blob) {
      return new TestBlobPath(fs, blob);
    }

    @Override
    public Factory<?> asFactory() {
      final Context context = getContext();
      final TestBlobStorageAccessor accessor = TestBlobStorageAccessor.this;
      return repo -> accessor.new BlobReader(context);
    }
  }

  private final Map<String, ByteArrayOutputStream> outputStreams = new ConcurrentHashMap<>();
  private final TestBlobFileSystem fs;

  public TestBlobStorageAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    this(entityDesc, uri, cfg, null, new AtomicReference<>());
  }

  public TestBlobStorageAccessor(
      EntityDescriptor entityDesc,
      URI uri,
      Map<String, Object> cfg,
      @Nullable Runnable afterWrite,
      AtomicReference<Runnable> preWrite) {

    super(entityDesc, uri, cfg);
    fs = new TestBlobFileSystem(afterWrite, preWrite);
  }

  @Override
  public FileSystem getTargetFileSystem() {
    return fs;
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(new BlobWriter(context));
  }

  @Override
  public Optional<BatchLogReader> getBatchLogReader(Context context) {
    return Optional.of(new BlobReader(context));
  }

  List<StreamElement> getWrittenFor(long ts) {
    return outputStreams
        .keySet()
        .stream()
        .filter(n -> getNamingConvention().isInRange(n, ts, ts + 1))
        .findAny()
        .map(n -> new TestBlobPath(fs, new TestBlob(n)))
        .map(
            p ->
                ExceptionUtils.uncheckedFactory(
                    () -> getFileFormat().openReader(p, getEntityDescriptor())))
        .map(r -> StreamSupport.stream(r.spliterator(), false))
        .map(s -> s.collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  int getWrittenBlobs() {
    return (int) outputStreams.values().stream().filter(s -> s.size() > 0).count();
  }
}
