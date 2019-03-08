/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * {@link BulkAttributeWriter} for gcloud storage.
 */
@Stable
@Slf4j
public class BulkGCloudStorageWriter
    extends GCloudClient
    implements BulkAttributeWriter {

  private static final DateTimeFormatter DIR_FORMAT = DateTimeFormatter.ofPattern(
      "yyyy/MM/");

  @VisibleForTesting
  static final String PREFIX;

  static {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(InetAddress.getLocalHost().getHostName()
          .getBytes(Charset.defaultCharset()));
      PREFIX = new String(Hex.encodeHex(digest.digest())).substring(0, 6);
    } catch (Exception ex) {
      log.error("Failed to generate bucket prefix", ex);
      throw new RuntimeException(ex);
    }
  }

  @ToString
  class BucketData {
    @Getter
    final BinaryBlob blob;
    @Getter
    final BinaryBlob.Writer writer;
    @Getter
    @Setter
    @Nullable
    CommitCallback committer = null;
    @Getter
    @Setter
    long lastWriteWatermark = 0L;
    @Getter
    @Setter
    long lastWriteSeqNo = 0L;

    BucketData() {
      try {
        blob = createLocalBlob();
        writer = blob.writer(gzip);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private final Factory<Executor> executorFactory;
  private final File tmpDir;
  private final long rollPeriod;
  private final boolean gzip;
  private final int bufferSize;
  private final long allowedLateness;
  private final long flushAttemptDelay;
  @SuppressFBWarnings(
      value = "SE_BAD_FIELD",
      justification = "Serialized empty. After first write the writer is not "
          + "considered serializable anymore.")
  // key is bucket end stamp
  private final NavigableMap<Long, BucketData> buckets = new TreeMap<>();
  private long lastFlushAttempt = Long.MIN_VALUE;
  private long writeSeqNo = 0L;
  private transient Executor flushExecutor;
  private transient boolean initialized;

  public BulkGCloudStorageWriter(
      EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg,
      Context context) {

    super(entityDesc, uri, cfg);

    tmpDir = Optional.ofNullable(cfg.get("tmp.dir"))
        .map(Object::toString)
        .map(File::new)
        .orElse(new File(System.getProperty("java.io.tmpdir") + File.separator
            + "bulk-cloud-storage-" + UUID.randomUUID()));

    rollPeriod = Optional.ofNullable(cfg.get("log-roll-interval"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(3600000L);

    gzip = Optional.ofNullable(cfg.get("gzip"))
        .map(Object::toString)
        .map(Boolean::valueOf)
        .orElse(false);

    bufferSize = Optional.ofNullable(cfg.get("buffer-size"))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(1024 * 1024);

    allowedLateness = Optional.ofNullable(cfg.get("allowed-lateness-ms"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(5 * 60000L);

    flushAttemptDelay = Optional.ofNullable(cfg.get("flush-delay-ms"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(5000L);

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
   *   when a allowed lateness passes
   */
  @Override
  public void write(StreamElement data, long watermark, CommitCallback statusCallback) {
    try {
      init();
      long stamp = data.getStamp();
      BucketData bucketData = getOrCreateWriterFor(stamp);
      bucketData.setCommitter(statusCallback);
      bucketData.getWriter().write(data);
      bucketData.setLastWriteSeqNo(writeSeqNo++);
      bucketData.setLastWriteWatermark(watermark);
      long now = watermark > Long.MIN_VALUE + allowedLateness
          ? watermark - allowedLateness
          : watermark;
      if (lastFlushAttempt == Long.MIN_VALUE
          || now - lastFlushAttempt >= flushAttemptDelay) {

        flushWriters(now);
        lastFlushAttempt = now;
      }
    } catch (Exception ex) {
      log.warn("Exception writing data {}", data, ex);
      statusCallback.commit(false, ex);
    }
  }

  private BucketData getOrCreateWriterFor(long stamp) {
    long boundary = getFlushBoundary(stamp);
    return buckets.computeIfAbsent(boundary + rollPeriod, b -> new BucketData());
  }

  private long getFlushBoundary(long stamp) {
    return (stamp / rollPeriod) * rollPeriod;
  }

  private void flushWriters(long stamp) {
    List<Map.Entry<Long, BucketData>> flushable = new ArrayList<>();
    long lastWrittenSeqNo = -1L;
    CommitCallback confirm = null;
    log.info("Trying to flush writers at watermark {}", Instant.ofEpochMilli(stamp));
    for (Map.Entry<Long, BucketData> e : buckets.entrySet()) {
      if (e.getKey() <= stamp) {
        flushable.add(e);
        if (e.getValue().getLastWriteWatermark() >= e.getKey()) {
          // the bucket was written after the closing timestamp
          // move the flushing to next bucket
          log.info("Need to flush additional bucket, due to previous "
              + "bucket {} being written after closing stamp {}",
              e.getKey(), stamp);
          stamp = e.getKey() + rollPeriod;
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
    CommitCallback finalCallback = (succ, exc) -> {
      if (!succ) {
        flushing.set(-1);
        flushingCallback.commit(false, exc);
      } else if (flushing.decrementAndGet() == 0) {
        flushingCallback.commit(true, null);
      }
    };
    flushable.forEach(e -> {
      long endStamp = e.getKey();
      BucketData data = e.getValue();
      ExceptionUtils.unchecked(() ->
          flushWriter(endStamp, data.getBlob(), data.getWriter(), finalCallback));
      buckets.remove(endStamp);
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
        remove(tmpDir);
        tmpDir.mkdirs();
      } else {
        throw new IllegalStateException(
            "Temporary directory " + tmpDir + " is not directory");
      }
      tmpDir.deleteOnExit();
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

  private void remove(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File f : files) {
          if (f.isDirectory()) {
            remove(f);
          }
          deleteHandlingErrors(f);
        }
      } else {
        deleteHandlingErrors(dir);
      }
    } else {
      deleteHandlingErrors(dir);
    }
  }

  @VisibleForTesting
  BinaryBlob createLocalBlob() {
    return new BinaryBlob(new File(tmpDir, UUID.randomUUID().toString()));
  }

  @VisibleForTesting
  void flush() {
    flushWriters(Long.MAX_VALUE);
  }

  private void flush(
      File file, long bucketEndStamp, CommitCallback callback) {

    String name = toBlobName(bucketEndStamp - rollPeriod, bucketEndStamp);
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
  String toBlobName(long min, long max) {
    String date = DIR_FORMAT.format(LocalDateTime.ofInstant(
        Instant.ofEpochMilli(min), ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
    return String.format("%s%s-%d_%d_%s.blob", date, PREFIX, min, max, uuid());
  }

  @VisibleForTesting
  String uuid() {
    return UUID.randomUUID().toString();
  }

  @VisibleForTesting
  void flushToBlob(long bucketEndStamp, File file, Blob blob) throws IOException {
    int written = 0;
    try (final WriteChannel channel = client().writer(blob);
        final FileInputStream fin = new FileInputStream(file)) {

      byte[] buffer = new byte[bufferSize];
      while (fin.available() > 0) {
        int read = fin.read(buffer);
        written += read;
        channel.write(ByteBuffer.wrap(buffer, 0, read));
      }
    }
    log.info(
        "Flushed blob {} with size {} KiB", blob.getBlobId().getName(),
        written / 1024.);
  }

  private void deleteHandlingErrors(File f) {
    deleteHandlingErrors(f, true);
  }

  private void deleteHandlingErrors(File f, boolean throwOnErrors) {
    try {
      Files.deleteIfExists(Paths.get(f.getAbsolutePath()));
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
    buckets.forEach((bucket, data) -> {
      try {
        CountDownLatch latch = new CountDownLatch(1);
        flushWriter(bucket, data.getBlob(), data.getWriter(), (succ, exc) -> {
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
      long bucketEndStamp,
      BinaryBlob localBlob,
      BinaryBlob.Writer writer,
      CommitCallback statusCallback) throws IOException {

    if (writer != null) {
      writer.close();
      final File flushFile = localBlob.getPath();
      flushExecutor().execute(() -> flush(
          flushFile, bucketEndStamp, statusCallback));
    }
  }

}
