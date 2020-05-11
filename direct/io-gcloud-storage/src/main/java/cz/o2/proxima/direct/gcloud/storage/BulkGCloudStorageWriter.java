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
package cz.o2.proxima.direct.gcloud.storage;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.bulk.AbstractBulkFileSystemAttributeWriter;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** {@link BulkAttributeWriter} for gcloud storage. */
@Stable
@Slf4j
public class BulkGCloudStorageWriter extends GCloudClient implements BulkAttributeWriter {

  private final GCloudStorageAccessor accessor;
  private final Context context;
  private transient File tmpDir;
  private transient FileSystem localFs;
  private transient NamingConvention namingConvention;
  private transient BulkAttributeWriter wrap;
  private transient int bufferSize;
  private transient boolean initialized;

  public BulkGCloudStorageWriter(
      EntityDescriptor entityDesc, GCloudStorageAccessor accessor, Context context) {

    super(entityDesc, accessor.getUri(), accessor.getCfg());
    this.accessor = accessor;
    this.context = context;
    init();
  }

  @Override
  public void write(StreamElement data, long watermark, CommitCallback statusCallback) {
    init();
    wrap.write(data, watermark, statusCallback);
  }

  @Override
  public void updateWatermark(long watermark) {
    wrap.updateWatermark(watermark);
  }

  @Override
  public void rollback() {
    wrap.rollback();
  }

  private void init() {
    if (!initialized) {
      tmpDir = accessor.getTmpDir();
      localFs = FileSystem.local(tmpDir, accessor.getNamingConvention());
      namingConvention = accessor.getNamingConvention();
      wrap =
          new AbstractBulkFileSystemAttributeWriter(
              getEntityDescriptor(),
              accessor.getUri(),
              localFs,
              accessor.getNamingConvention(),
              accessor.getFileFormat(),
              context,
              accessor.getRollPeriod(),
              accessor.getAllowedLateness()) {

            @Override
            protected void flush(Bulk bulk) {
              ExceptionUtils.unchecked(
                  () -> BulkGCloudStorageWriter.this.flush(bulk.getPath(), bulk.getMaxTs()));
            }
          };
      bufferSize = accessor.getBufferSize();

      if (!tmpDir.exists()) {
        tmpDir.mkdirs();
      } else if (tmpDir.isDirectory()) {
        removeDir(tmpDir);
        tmpDir.mkdirs();
      } else {
        throw new IllegalStateException("Temporary directory " + tmpDir + " is not directory");
      }
      initialized = true;
    }
  }

  private void removeDir(File dir) {
    Preconditions.checkArgument(dir.isDirectory());
    if (dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File f : files) {
          if (f.isDirectory()) {
            removeDir(f);
          }
          Preconditions.checkState(f.delete());
        }
      }
      Preconditions.checkState(dir.delete());
    }
  }

  @VisibleForTesting
  void flush() {
    wrap.updateWatermark(Long.MAX_VALUE);
  }

  private void flush(Path file, long bucketEndStamp) throws IOException {

    String name = toBlobName(bucketEndStamp - 1);
    Blob blob = null;
    try {
      blob = createBlob(name);
      flushToBlob(bucketEndStamp, file, blob);
      deleteHandlingErrors(file, false);
    } catch (Exception ex) {
      deleteBlobIfExists(blob);
      throw ex;
    }
  }

  private void deleteBlobIfExists(@Nullable Blob blob) {
    try {
      if (blob != null && blob.exists()) {
        blob.delete();
      }
    } catch (Exception ex) {
      log.warn("Failed to delete blob {}. Ignoring.", blob.getName(), ex);
    }
  }

  @VisibleForTesting
  String toBlobName(long ts) {
    return namingConvention.nameOf(ts);
  }

  @VisibleForTesting
  void flushToBlob(long bucketEndStamp, Path file, Blob blob) throws IOException {
    int written = 0;
    try (final WriteChannel channel = client().writer(blob);
        final InputStream fin = file.reader()) {

      byte[] buffer = new byte[bufferSize];
      while (fin.available() > 0) {
        int read = fin.read(buffer);
        written += read;
        channel.write(ByteBuffer.wrap(buffer, 0, read));
      }
    }
    log.info("Flushed blob {} with size {} KiB", blob.getBlobId().getName(), written / 1024.);
  }

  private void deleteHandlingErrors(Path f, boolean throwOnErrors) {
    try {
      f.delete();
    } catch (IOException ex) {
      if (throwOnErrors) {
        throw new RuntimeException(ex);
      }
      log.warn("Failed to delete {}. Ignoring", f, ex);
    }
  }

  @Override
  public void close() {
    wrap.close();
  }
}
