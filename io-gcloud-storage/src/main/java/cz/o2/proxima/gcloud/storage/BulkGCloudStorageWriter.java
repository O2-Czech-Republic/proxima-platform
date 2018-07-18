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
package cz.o2.proxima.gcloud.storage;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.BulkAttributeWriter;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.StreamElement;
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;

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

  private final File tmpDir;
  private final long rollPeriod;
  private final boolean gzip;
  private final int bufferSize;
  private final Executor flushExecutor;
  private boolean anyflush = false;
  private long minTimestamp = Long.MAX_VALUE;
  private long maxTimestamp = Long.MIN_VALUE;
  private BinaryBlob localBlob = null;
  private BinaryBlob.Writer writer = null;
  private long lastFlushStamp;
  private boolean forceFlush = false;

  public BulkGCloudStorageWriter(
      EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg,
      Context context) {

    super(entityDesc, uri, cfg);

    tmpDir = Optional.ofNullable(cfg.get("tmp.dir"))
        .map(Object::toString)
        .map(File::new)
        .orElse(new File("/tmp/bulk-cloud-storage-" + UUID.randomUUID()));

    rollPeriod = Optional.ofNullable(cfg.get("log-roll-interval"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(3600000L);

    gzip = Optional.ofNullable(cfg.get("gzip"))
        .map(Object::toString)
        .map(Boolean::valueOf)
        .orElse(false);

    bufferSize = Optional.ofNullable(cfg.get("buffer.size"))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(1024 * 1024);

    flushExecutor = context.getExecutorService();
    lastFlushStamp = System.currentTimeMillis() / rollPeriod * rollPeriod;

    init();
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    try {
      if (writer == null) {
        createLocalBlob();
        writer = localBlob.writer(gzip);
      }
      writer.write(data);
      long floorStamp = anyflush
          ? data.getStamp()
          : data.getStamp() - (data.getStamp() % rollPeriod);
      if (minTimestamp > floorStamp) {
        minTimestamp = floorStamp;
      }
      if (maxTimestamp < data.getStamp()) {
        maxTimestamp = data.getStamp();
      }
      long now = System.currentTimeMillis();
      if (now - lastFlushStamp >= rollPeriod || forceFlush) {
        writer.close();
        final File flushFile = localBlob.getPath();
        final long flushMinStamp = minTimestamp;
        final long flushMaxStamp = maxTimestamp;
        lastFlushStamp = now;
        flushExecutor.execute(
            () -> flush(flushFile, flushMinStamp, flushMaxStamp, statusCallback));
        writer = null;
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
        forceFlush = false;
      }
    } catch (Exception ex) {
      log.warn("Exception writing data {}", data, ex);
      statusCallback.commit(false, ex);
    }
  }

  @Override
  public void rollback() {
    init();
  }

  private void init() {
    if (writer != null) {
      try {
        writer.close();
        writer = null;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
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
    writer = null;
    anyflush = false;
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
  void createLocalBlob() throws IOException {
    localBlob = new BinaryBlob(new File(tmpDir, UUID.randomUUID().toString()));
  }

  @VisibleForTesting
  void flush() {
    forceFlush = true;
  }

  private void flush(
      File file, long minTimestamp,
      long maxTimestamp, CommitCallback callback) {

    try {
      String name = toBlobName(minTimestamp, maxTimestamp);
      Blob blob = createBlob(name);
      flushToBlob(file, blob);
      deleteHandlingErrors(file);
      callback.commit(true, null);
      anyflush = true;
    } catch (Exception ex) {
      callback.commit(false, ex);
      throw new RuntimeException(ex);
    }
  }

  @VisibleForTesting
  String toBlobName(long min, long max) {
    String date = DIR_FORMAT.format(LocalDateTime.ofInstant(
        Instant.ofEpochMilli(min), ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
    String name = String.format(
        "%s%s-%d_%d_%s.blob", date, PREFIX, min, max,
        uuid());
    return name;
  }

  @VisibleForTesting
  String uuid() {
    return UUID.randomUUID().toString();
  }

  @VisibleForTesting
  void flushToBlob(File file, Blob blob) throws IOException {
    int written = 0;
    try (final WriteChannel writer = this.client.writer(blob);
        final FileInputStream fin = new FileInputStream(file)) {

      byte[] buffer = new byte[bufferSize];
      while (fin.available() > 0) {
        int read = fin.read(buffer);
        written += read;
        writer.write(ByteBuffer.wrap(buffer, 0, read));
      }
    }
    log.info(
        "Flushed blob {} with size {} KiB", blob.getBlobId().getName(),
        written / 1024.);
  }

  private void deleteHandlingErrors(File f) {
    try {
      Files.deleteIfExists(Paths.get(f.getAbsolutePath()));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
