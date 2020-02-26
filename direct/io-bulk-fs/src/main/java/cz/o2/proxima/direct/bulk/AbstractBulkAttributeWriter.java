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
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** An abstract parent class for {@link BulkAttributeWriter}. */
@Slf4j
public abstract class AbstractBulkAttributeWriter implements BulkAttributeWriter {

  @ToString
  protected class Bulk {
    @Getter private final Path path;
    @Getter private final Writer writer;
    @Getter private final long maxTs;

    @Getter @Nullable private CommitCallback commit = null;
    @Getter private long lastWriteWatermark = Long.MIN_VALUE;
    @Getter private long firstWriteSeqNo = -1;
    @Getter private long lastWriteSeqNo = 0;

    Bulk(Path path, long maxTs) throws IOException {
      this.path = path;
      this.writer = format.openWriter(path, entity);
      this.maxTs = maxTs;
    }

    void write(StreamElement data, CommitCallback commit, long watermark, long seqNo)
        throws IOException {
      this.commit = commit;
      this.lastWriteWatermark = watermark;
      this.lastWriteSeqNo = seqNo;
      if (this.firstWriteSeqNo < 0) {
        firstWriteSeqNo = seqNo;
      }
      writer.write(data);
    }
  }

  @Getter private final EntityDescriptor entity;
  @Getter private final URI uri;
  @Getter private final FileSystem fs;
  @Getter private final NamingConvention namingConvention;
  @Getter private final FileFormat format;
  @Getter private final long rollPeriodMs;
  @Getter private final long allowedLatenessMs;

  private long seqNo = 0;
  private transient Map<Long, Bulk> writers = null;

  protected AbstractBulkAttributeWriter(
      EntityDescriptor entity,
      URI uri,
      FileSystem fs,
      NamingConvention namingConvention,
      FileFormat format,
      long rollPeriodMs,
      long allowedLatenessMs) {

    this.entity = entity;
    this.uri = uri;
    this.fs = fs;
    this.namingConvention = namingConvention;
    this.format = format;
    this.rollPeriodMs = rollPeriodMs;
    this.allowedLatenessMs = allowedLatenessMs;
  }

  @Override
  public void write(StreamElement data, long watermark, CommitCallback statusCallback) {
    if (writers == null) {
      writers = new HashMap<>();
    }
    Path path = fs.newPath(data.getStamp());
    long bulkStartStamp = data.getStamp() - data.getStamp() % rollPeriodMs;
    Bulk bulk =
        writers.computeIfAbsent(
            bulkStartStamp,
            p -> {
              long maxStamp = data.getStamp() - data.getStamp() % rollPeriodMs + rollPeriodMs;
              return ExceptionUtils.uncheckedFactory(() -> new Bulk(path, maxStamp));
            });
    ExceptionUtils.unchecked(() -> bulk.write(data, statusCallback, watermark, seqNo++));
    log.debug("Written element {} to {} on watermark {}", data, bulk.getWriter(), watermark);
    flushOnWatermark(watermark);
  }

  @Override
  public void updateWatermark(long watermark) {
    flushOnWatermark(watermark);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void rollback() {
    close();
  }

  @Override
  public void close() {
    if (writers != null) {
      writers
          .values()
          .forEach(
              writer -> {
                Path path = writer.getWriter().getPath();
                try {
                  writer.getWriter().close();
                  path.delete();
                } catch (IOException ex) {
                  throw new RuntimeException(ex);
                }
              });
      writers.clear();
    }
    seqNo = 0;
  }

  /**
   * Flush given {@link Bulk} to final output.
   *
   * @param bulk the bulk to flush
   */
  protected abstract void flush(Bulk bulk);

  @VisibleForTesting
  void flush(long watermark) {
    flushOnWatermark(watermark);
  }

  private void flushOnWatermark(long watermark) {
    Map<Long, Bulk> flushable = collectFlushable(watermark - allowedLatenessMs);
    if (!flushable.isEmpty()) {
      log.debug("Flushable blobs at watermark {}: {}", watermark, flushable);
      CommitCallback commit =
          flushable
              .values()
              .stream()
              .min(Comparator.comparing(Bulk::getLastWriteSeqNo))
              .get()
              .getCommit();
      try {
        flushable.forEach(
            (k, v) -> {
              ExceptionUtils.unchecked(() -> v.getWriter().close());
              flush(v);
              writers.remove(k);
            });
        commit.commit(true, null);
      } catch (Exception ex) {
        log.error("Failed to flush {}", flushable, ex);
        commit.commit(false, ex);
      }
    }
  }

  private Map<Long, Bulk> collectFlushable(long watermark) {
    Map<Long, Bulk> ret =
        writers
            .entrySet()
            .stream()
            .filter(e -> e.getValue().getMaxTs() < watermark)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // search for commit callback to use for committing
    long maxWriteSeqNo =
        ret.values().stream().mapToLong(Bulk::getLastWriteSeqNo).max().orElse(Long.MIN_VALUE);

    // add all paths that were written *before* the committed seq no
    writers
        .entrySet()
        .stream()
        .filter(e -> e.getValue().getFirstWriteSeqNo() < maxWriteSeqNo)
        .forEach(e -> ret.put(e.getKey(), e.getValue()));

    return ret;
  }
}
