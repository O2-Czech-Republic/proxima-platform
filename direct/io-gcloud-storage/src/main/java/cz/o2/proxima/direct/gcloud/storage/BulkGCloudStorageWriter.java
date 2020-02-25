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
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Writer;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** {@link BulkAttributeWriter} for gcloud storage. */
@Stable
@Slf4j
public class BulkGCloudStorageWriter extends GCloudClient implements BulkAttributeWriter {

  @ToString
  class BucketData {
    @Getter final long maxTs;
    @Getter final Path blobPath;
    @Getter final Writer writer;
    @Getter @Nullable CommitCallback committer = null;
    @Getter long lastWriteWatermark = 0L;
    @Getter long lastWriteSeqNo = 0L;

    BucketData(long maxTsExclusive) {
      try {
        blobPath = createLocalBlob(maxTsExclusive);
        writer = fileFormat.openWriter(blobPath, getEntityDescriptor());
        maxTs = maxTsExclusive;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public void write(
        StreamElement data, CommitCallback statusCallback, long writeSeqNo, long watermark)
        throws IOException {
      lastWriteSeqNo = writeSeqNo;
      lastWriteWatermark = watermark;
      committer = statusCallback;
      writer.write(data);
    }
  }

  private final Factory<Executor> executorFactory;
  private final File tmpDir;
  private final FileSystem localFs;
  private final FileFormat fileFormat;
  private final NamingConvention namingConvention;
  private final long rollPeriod;
  private final int bufferSize;
  private final long allowedLateness;
  private final long flushAttemptDelay;
  private final NavigableMap<String, BucketData> buckets = new TreeMap<>();
  private long lastFlushAttempt = Long.MIN_VALUE;
  private long writeSeqNo = 0L;
  private transient Executor flushExecutor;
  private transient boolean initialized;

  public BulkGCloudStorageWriter(
      EntityDescriptor entityDesc, GCloudStorageAccessor accessor, Context context) {

    super(entityDesc, accessor.getUri(), accessor.getCfg());

    tmpDir = accessor.getTmpDir();
    localFs = FileSystem.local(tmpDir);
    rollPeriod = accessor.getRollPeriod();
    fileFormat = accessor.getFileFormat();
    namingConvention = accessor.getNamingConvention();
    bufferSize = accessor.getBufferSize();
    allowedLateness = accessor.getAllowedLateness();
    flushAttemptDelay = accessor.getFlushAttemptDelay();
    executorFactory = context::getExecutorService;
  }

  /*
   * Data might (and will) arrive out-of-order here, so we
   * must make sure the flushing mechanism is robust enough to
   * incorporate this.
   * It works as follows:
   * - no element can be present in a blob with other element
   *   that belongs to different month
   * - flushing is done in event time, but multiple writers
   *   can be opened in single time frame, each writer is flushed
   *   when allowed lateness passes
   */
  @Override
  public void write(StreamElement data, long watermark, CommitCallback statusCallback) {
    try {
      init();
      long stamp = data.getStamp();
      BucketData bucketData = getOrCreateWriterFor(stamp);
      bucketData.write(data, statusCallback, writeSeqNo++, watermark);
      long now =
          watermark > Long.MIN_VALUE + allowedLateness ? watermark - allowedLateness : watermark;
      if (lastFlushAttempt == Long.MIN_VALUE || now - lastFlushAttempt >= flushAttemptDelay) {

        flushWriters(now);
        lastFlushAttempt = now;
      }
    } catch (Exception ex) {
      log.warn("Exception writing data {}", data, ex);
      statusCallback.commit(false, ex);
    }
  }

  private BucketData getOrCreateWriterFor(long stamp) {
    String name = namingConvention.nameOf(stamp);
    return buckets.computeIfAbsent(
        name, b -> new BucketData(stamp - stamp % rollPeriod + rollPeriod));
  }

  private void flushWriters(long stamp) {
    List<Map.Entry<String, BucketData>> flushable = new ArrayList<>();
    long lastWrittenSeqNo = -1L;
    CommitCallback confirm = null;
    if (log.isDebugEnabled()) {
      log.debug("Trying to flush writers at watermark {}", Instant.ofEpochMilli(stamp));
    }
    for (Map.Entry<String, BucketData> e : buckets.entrySet()) {
      if (e.getValue().getMaxTs() <= stamp) {
        flushable.add(e);
        if (e.getValue().getLastWriteWatermark() >= e.getValue().getMaxTs()) {
          // the bucket was written after the closing timestamp
          // move the flushing to next bucket
          log.info(
              "Need to flush additional bucket, due to previous "
                  + "bucket {} being written after closing stamp {}",
              e.getKey(),
              stamp);
          stamp = e.getValue().getMaxTs() + rollPeriod;
        }
        if (e.getValue().getLastWriteSeqNo() > lastWrittenSeqNo) {
          lastWrittenSeqNo = e.getValue().getLastWriteSeqNo();
          confirm = e.getValue().getCommitter();
        }
      } else {
        break;
      }
    }
    final CommitCallback flushingCallback = confirm;
    AtomicInteger flushing = new AtomicInteger(flushable.size());
    CommitCallback finalCallback =
        (succ, exc) -> {
          if (!succ) {
            flushing.set(-1);
            flushingCallback.commit(false, exc);
          } else if (flushing.decrementAndGet() == 0) {
            flushingCallback.commit(true, null);
          }
        };
    flushable.forEach(
        e -> {
          long endStamp = e.getValue().getMaxTs();
          BucketData data = e.getValue();
          ExceptionUtils.unchecked(
              () -> flushWriter(endStamp, data.getBlobPath(), data.getWriter(), finalCallback));
          buckets.remove(e.getKey());
        });
  }

  @Override
  public void rollback() {
    init(true);
  }

  private void init() {
    if (!initialized) {
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

  private void init(boolean force) {
    if (force) {
      lastFlushAttempt = Long.MIN_VALUE;
      buckets.clear();
      writeSeqNo = 0L;
      initialized = false;
    }
    init();
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
  Path createLocalBlob(long maxTsExclusive) {
    return localFs.newPath(maxTsExclusive - rollPeriod);
  }

  @VisibleForTesting
  void flush() {
    flushWriters(Long.MAX_VALUE);
  }

  private void flush(Path file, long bucketEndStamp, CommitCallback callback) {

    String name = toBlobName(bucketEndStamp - 1);
    Blob blob = null;
    try {
      blob = createBlob(name);
      flushToBlob(bucketEndStamp, file, blob);
      deleteHandlingErrors(file, false);
      callback.commit(true, null);
    } catch (Exception ex) {
      log.warn("Failed to flush blob {}. Deleting if exists.", name, ex);
      deleteBlobIfExists(blob);
      callback.commit(false, ex);
    }
  }

  private void deleteBlobIfExists(Blob blob) {
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
      log.warn("Failed to delete {}. Ingoring", f, ex);
    }
  }

  Executor flushExecutor() {
    if (flushExecutor == null) {
      flushExecutor = executorFactory.apply();
    }
    return flushExecutor;
  }

  @Override
  public void close() {
    buckets.forEach(
        (bucket, data) -> {
          try {
            CountDownLatch latch = new CountDownLatch(1);
            flushWriter(
                data.getMaxTs(),
                data.getBlobPath(),
                data.getWriter(),
                (succ, exc) -> {
                  if (!succ) {
                    log.warn("Failed to close writer {}", data.getWriter(), exc);
                  }
                  latch.countDown();
                });
            latch.await();
          } catch (Exception ex) {
            log.warn("Failed to close writer {}", data.getWriter(), ex);
          }
        });
    buckets.clear();
  }

  private void flushWriter(
      long bucketEndStamp, Path localBlob, Writer writer, CommitCallback statusCallback)
      throws IOException {

    if (writer != null) {
      writer.close();
      flushExecutor().execute(() -> flush(localBlob, bucketEndStamp, statusCallback));
    }
  }
}
