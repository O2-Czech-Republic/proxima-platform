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
import com.google.common.collect.Iterables;
import cz.o2.proxima.direct.core.AbstractBulkAttributeWriter;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** An abstract parent class for {@link BulkAttributeWriter}. */
@Slf4j
public abstract class AbstractBulkFileSystemAttributeWriter extends AbstractBulkAttributeWriter {

  private static final long serialVersionUID = 1L;

  @ToString
  protected class Bulk {
    @Getter private final Path path;
    @Getter private final Writer writer;
    @Getter private final long startTs;
    @Getter private final long maxTs;

    @Getter @Nullable private CommitCallback commit = null;
    @Getter private long lastWriteWatermark = Long.MIN_VALUE;
    @Getter private long firstWriteSeqNo = -1;
    @Getter private long lastWriteSeqNo = 0;

    Bulk(Path path, long startTs, long maxTs) throws IOException {
      this.path = path;
      this.writer = format.openWriter(path, getEntityDescriptor());
      this.startTs = startTs;
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

  @ToString
  private class LateBulk extends Bulk {

    private long startTs = Long.MAX_VALUE;
    private long maxSeenTs = Long.MIN_VALUE;

    LateBulk(Path path) throws IOException {
      super(path, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Override
    void write(StreamElement data, CommitCallback commit, long watermark, long seqNo)
        throws IOException {
      super.write(data, commit, watermark, seqNo);
      maxSeenTs = Math.max(data.getStamp(), maxSeenTs);
      startTs = Math.min(data.getStamp(), startTs);
    }

    @Override
    public long getStartTs() {
      return startTs;
    }

    @Override
    public long getMaxTs() {
      return maxSeenTs;
    }
  }

  @Getter private final FileSystem fs;
  @Getter private final NamingConvention namingConvention;
  @Getter private final FileFormat format;
  @Getter private final long rollPeriodMs;
  @Getter private final long allowedLatenessMs;
  private final Factory<Executor> executorFactory;
  private final Map<String, LateBulk> lateBulks = new HashMap<>();

  private long seqNo = 0;
  private transient Map<Long, Bulk> writers = null;
  private transient @Nullable Executor executor = null;

  protected AbstractBulkFileSystemAttributeWriter(
      EntityDescriptor entity,
      URI uri,
      FileSystem fs,
      NamingConvention namingConvention,
      FileFormat format,
      Context context,
      long rollPeriodMs,
      long allowedLatenessMs) {

    super(entity, uri);
    this.fs = fs;
    this.namingConvention = namingConvention;
    this.format = format;
    this.rollPeriodMs = rollPeriodMs;
    this.allowedLatenessMs = allowedLatenessMs;
    this.executorFactory = context::getExecutorService;
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
    if (writers == null) {
      writers = new HashMap<>();
    }
    long startStamp = data.getStamp() - data.getStamp() % rollPeriodMs;
    long maxStamp = Math.max(startStamp + rollPeriodMs, startStamp);
    if (maxStamp + allowedLatenessMs >= watermark) {
      Path path = fs.newPath(data.getStamp());
      Bulk bulk =
          writers.computeIfAbsent(
              startStamp,
              p -> ExceptionUtils.uncheckedFactory(() -> new Bulk(path, startStamp, maxStamp)));
      writeToBulk(data, statusCallback, watermark, bulk);
      log.debug("Written element {} to {} on watermark {}", data, bulk.getWriter(), watermark);
    } else {
      handleLateData(data, watermark, statusCallback);
    }
    flushOnWatermark(watermark);
  }

  /**
   * Handle data after watermark.
   *
   * @param data the late data
   * @param watermark current watermark
   * @param statusCallback callback for commit
   */
  protected void handleLateData(StreamElement data, long watermark, CommitCallback statusCallback) {
    LateBulk lateBulk =
        lateBulks.computeIfAbsent(
            Iterables.getOnlyElement(namingConvention.prefixesOf(data.getStamp(), data.getStamp())),
            prefix -> newLateBulkFor(data));
    writeToBulk(data, statusCallback, watermark, lateBulk);
    log.debug(
        "Written late element {} to {} on watermark {}", data, lateBulk.getWriter(), watermark);
  }

  private LateBulk newLateBulkFor(StreamElement data) {
    return ExceptionUtils.uncheckedFactory(
        () -> {
          log.debug("Created new late bulk for {}", data);
          return new LateBulk(fs.newPath(Long.MAX_VALUE));
        });
  }

  private void writeToBulk(
      StreamElement data, CommitCallback statusCallback, long watermark, Bulk bulk) {
    ExceptionUtils.unchecked(() -> bulk.write(data, statusCallback, watermark, seqNo++));
  }

  @Override
  public void updateWatermark(long watermark) {
    flushOnWatermark(watermark);
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
    Collection<Bulk> flushable = collectFlushable(watermark);
    if (log.isDebugEnabled()) {
      log.debug(
          "Collected {} flushable bulks at watermark {} with allowedLatenessMs {}.",
          flushable,
          watermark,
          allowedLatenessMs);
    }
    if (!flushable.isEmpty()) {
      CommitCallback commit =
          flushable.stream().max(Comparator.comparing(Bulk::getLastWriteSeqNo)).get().getCommit();
      try {
        AtomicInteger toFlush = new AtomicInteger(flushable.size());
        flushable.forEach(
            bulk -> {
              ExceptionUtils.unchecked(() -> bulk.getWriter().close());
              executor()
                  .execute(
                      () -> {
                        try {
                          flush(bulk);
                          log.info("Flushed path {}", bulk.getPath());
                          if (toFlush.decrementAndGet() == 0) {
                            commit.commit(true, null);
                          }
                        } catch (Exception ex) {
                          log.error("Failed to flush path {}", bulk.getPath(), ex);
                          toFlush.set(-1);
                          ExceptionUtils.unchecked(bulk.getPath()::delete);
                          commit.commit(false, ex);
                        }
                      });
              writers.remove(bulk.getStartTs());
            });

      } catch (Exception ex) {
        log.error("Failed to flush paths {}", flushable, ex);
        commit.commit(false, ex);
      }
    }
  }

  private Executor executor() {
    if (executor == null) {
      executor = executorFactory.apply();
    }
    return executor;
  }

  private Collection<Bulk> collectFlushable(long watermark) {
    if (writers != null) {
      Set<Bulk> ret =
          writers
              .values()
              .stream()
              .filter(bulk -> bulk.getMaxTs() + allowedLatenessMs < watermark)
              .collect(Collectors.toSet());

      if (!ret.isEmpty() && !lateBulks.isEmpty()) {
        // flush late bulk if there is any bulk to flush
        lateBulks.forEach((k, bulk) -> ret.add(bulk));
        log.debug("Added late bulks {} into flushable list", lateBulks);
        lateBulks.clear();
      }

      // search for commit callback to use for committing
      long maxWriteSeqNo =
          ret.stream().mapToLong(Bulk::getLastWriteSeqNo).max().orElse(Long.MIN_VALUE);

      // add all paths that were written *before* the committed seq no
      writers
          .entrySet()
          .stream()
          .filter(e -> e.getValue().getFirstWriteSeqNo() < maxWriteSeqNo)
          .forEach(e -> ret.add(e.getValue()));

      return ret;
    }
    return Collections.emptyList();
  }
}
