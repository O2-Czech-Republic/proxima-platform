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

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import cz.o2.proxima.direct.core.AbstractBulkAttributeWriter;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  private static final long serialVersionUID = 2L;

  @ToString
  protected class Bulk {
    @Getter private final Path path;
    @Getter private final Writer writer;
    @Getter private final long startTs;
    @Getter private final long maxTs;

    @Nullable private CommitCallback commit = null;
    @Getter private long lastWriteWatermark = Long.MIN_VALUE;
    @Getter private long firstWriteSeqNo = -1;
    @Getter private long lastWriteSeqNo = 0;

    Bulk(Path path, long startTs, long maxTs) throws IOException {
      this.path = path;
      this.writer = format.openWriter(path, getEntityDescriptor());
      this.startTs = startTs;
      this.maxTs = maxTs;
    }

    synchronized void write(StreamElement data, CommitCallback commit, long watermark, long seqNo)
        throws IOException {
      this.commit = commit;
      this.lastWriteWatermark = watermark;
      this.lastWriteSeqNo = seqNo;
      if (this.firstWriteSeqNo < 0) {
        firstWriteSeqNo = seqNo;
      }
      writer.write(data);
    }

    public CommitCallback getCommit() {
      return Objects.requireNonNull(commit);
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
    synchronized void write(StreamElement data, CommitCallback commit, long watermark, long seqNo)
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
  @Getter private final cz.o2.proxima.functional.Factory<Executor> executorFactory;
  @Getter private final Context context;
  private final Map<Long, Bulk> writers = Collections.synchronizedMap(new HashMap<>());
  private final Map<String, LateBulk> lateWriters = Collections.synchronizedMap(new HashMap<>());
  private final AtomicInteger inFlightFlushes = new AtomicInteger();

  private long seqNo = 0;

  private transient @Nullable Executor executor = null;

  @SuppressWarnings({"unchecked", "rawtypes"})
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
    this.executorFactory = (cz.o2.proxima.functional.Factory) context.getExecutorFactory();
    this.context = context;
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
    long startStamp = data.getStamp() - data.getStamp() % rollPeriodMs;
    long maxStamp = Math.max(startStamp + rollPeriodMs, startStamp);
    if (maxStamp + allowedLatenessMs >= watermark) {
      Bulk bulk =
          writers.computeIfAbsent(
              startStamp,
              p ->
                  ExceptionUtils.uncheckedFactory(
                      () -> new Bulk(fs.newPath(startStamp), startStamp, maxStamp)));
      writeToBulk(data, statusCallback, watermark, bulk);
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
        lateWriters.computeIfAbsent(
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
    log.debug("Written element {} to {} on watermark {}", data, bulk.getWriter(), watermark);
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
    synchronized (writers) {
      synchronized (lateWriters) {
        Streams.concat(writers.values().stream(), lateWriters.values().stream())
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
        lateWriters.clear();
      }
      writers.clear();
    }
  }

  /**
   * Flush given {@link Bulk} to final output.
   *
   * @param bulk the bulk to flush
   */
  protected abstract void flush(Bulk bulk);

  private void flushOnWatermark(long watermark) {
    Collection<Bulk> flushable = collectFlushable(watermark);
    if (!flushable.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug(
            "Collected {} flushable bulks at watermark {} with allowedLatenessMs {}.",
            flushable,
            watermark,
            allowedLatenessMs);
      }
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
                          inFlightFlushes.decrementAndGet();
                        } catch (Exception ex) {
                          inFlightFlushes.decrementAndGet();
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
    if (inFlightFlushes.get() > 0) {
      return Collections.emptyList();
    }
    synchronized (writers) {
      Set<Bulk> ret =
          writers
              .values()
              .stream()
              .filter(bulk -> bulk.getMaxTs() + allowedLatenessMs < watermark)
              .collect(Collectors.toSet());

      // search for commit callback to use for committing
      long maxWriteSeqNo = getMaxWriteSeqNo(ret);

      if (ret.isEmpty()) {
        return ret;
      }

      synchronized (lateWriters) {
        int initialSize;
        do {
          initialSize = ret.size();
          List<Map.Entry<String, LateBulk>> lateBulks =
              getAdditionalBulks(lateWriters.entrySet(), maxWriteSeqNo);
          lateBulks.forEach(e -> ret.add(lateWriters.remove(e.getKey())));
          maxWriteSeqNo = getMaxWriteSeqNo(ret);
          List<Map.Entry<Long, Bulk>> normalBulks =
              getAdditionalBulks(writers.entrySet(), maxWriteSeqNo);
          normalBulks.forEach(e -> ret.add(e.getValue()));
          maxWriteSeqNo = getMaxWriteSeqNo(ret);
        } while (initialSize < ret.size());
      }
      inFlightFlushes.addAndGet(ret.size());
      return ret;
    }
  }

  private long getMaxWriteSeqNo(Set<Bulk> ret) {
    return ret.stream().mapToLong(Bulk::getLastWriteSeqNo).max().orElse(Long.MIN_VALUE);
  }

  private <K, B extends Bulk> List<Map.Entry<K, B>> getAdditionalBulks(
      Collection<Map.Entry<K, B>> bulks, long maxWriteSeqNo) {
    return bulks
        .stream()
        .filter(e -> e.getValue().getFirstWriteSeqNo() < maxWriteSeqNo)
        .collect(Collectors.toList());
  }
}
