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
package cz.o2.proxima.beam.direct.io;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/** A {@link Reader} created from {@link CommitLogReader}. */
@Slf4j
class BeamCommitLogReader {

  private static final Instant LOWEST_INSTANT = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private static final Instant HIGHEST_INSTANT = BoundedWindow.TIMESTAMP_MAX_VALUE;
  private static final byte[] EMPTY_BYTES = new byte[] {};
  // FIXME: configuration
  private static final long AUTO_WATERMARK_LAG_MS = 500;

  static class UnboundedCommitLogReader extends UnboundedReader<StreamElement> {

    private final DirectUnboundedSource source;
    @Getter private final BeamCommitLogReader reader;

    private Instant previousWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    private boolean finished = false;

    UnboundedCommitLogReader(
        String name,
        DirectUnboundedSource source,
        CommitLogReader reader,
        Position position,
        boolean eventTime,
        long limit,
        @Nullable Partition partition,
        @Nullable Offset offset) {

      this.source = source;
      this.reader =
          new BeamCommitLogReader(
              name, reader, position, eventTime, partition, offset, limit, false);
    }

    @Override
    public DirectUnboundedSource getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      log.debug("{} started to consume reader {}", this, reader.getUri());
      return reader.start();
    }

    @Override
    public boolean advance() throws IOException {
      return reader.advance();
    }

    @Override
    public StreamElement getCurrent() throws NoSuchElementException {
      return reader.getCurrent();
    }

    @Override
    public void close() {
      reader.close();
    }

    @Override
    public Instant getWatermark() {
      if (finished) {
        return HIGHEST_INSTANT;
      }
      final Instant currentWatermark = reader.getWatermark();
      if (currentWatermark.isBefore(previousWatermark)) {
        log.warn(
            "Watermark shifts back in time. Previous: [{}], Current: [{}].",
            previousWatermark,
            currentWatermark);
      }
      previousWatermark = currentWatermark;
      return currentWatermark;
    }

    @Override
    public DirectUnboundedSource.Checkpoint getCheckpointMark() {
      return new DirectUnboundedSource.Checkpoint(reader);
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      Instant boundedPos = reader.getCurrentTimestamp();
      if (boundedPos == HIGHEST_INSTANT) {
        finished = true;
        return boundedPos;
      }
      StreamElement current = reader.getCurrent();
      if (current != null) {
        return new Instant(current.getStamp());
      }
      throw new NoSuchElementException();
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      StreamElement el = getCurrent();
      if (el == null) {
        throw new NoSuchElementException();
      }
      if (getCurrentSource().requiresDeduping()) {
        return el.getUuid().getBytes(StandardCharsets.US_ASCII);
      }
      return EMPTY_BYTES;
    }
  }

  static BoundedReader<StreamElement> bounded(
      BoundedSource<StreamElement> source,
      String name,
      CommitLogReader reader,
      Position position,
      long limit,
      Partition partition) {

    BeamCommitLogReader r =
        new BeamCommitLogReader(name, reader, position, true, partition, null, limit, true);

    return new BoundedReader<StreamElement>() {

      @Override
      public BoundedSource<StreamElement> getCurrentSource() {
        return source;
      }

      @Override
      public boolean start() throws IOException {
        return r.start();
      }

      @Override
      public boolean advance() throws IOException {
        return r.advance();
      }

      @Override
      public StreamElement getCurrent() throws NoSuchElementException {
        return r.getCurrent();
      }

      @Override
      public void close() {
        r.close();
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return r.getCurrentTimestamp();
      }
    };
  }

  static UnboundedCommitLogReader unbounded(
      DirectUnboundedSource source,
      String name,
      CommitLogReader reader,
      Position position,
      boolean eventTime,
      long limit,
      @Nullable Partition partition,
      @Nullable Offset offset) {

    return new UnboundedCommitLogReader(
        name, source, reader, position, eventTime, limit, partition, offset);
  }

  @Getter private final Partition partition;
  @Getter private ObserveHandle handle;

  @Nullable private final String name;
  private final CommitLogReader reader;
  private final Position position;
  private final boolean eventTime;
  private final boolean stopAtCurrent;
  private boolean finished;
  @Getter private long limit;
  @Nullable private final Offset offset;
  private final long offsetWatermark;
  @Nullable private BlockingQueueLogObserver observer;
  private StreamElement current;
  private Instant currentProcessingTime = Instant.now();
  private Instant lastReadWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private BeamCommitLogReader(
      @Nullable String name,
      CommitLogReader reader,
      Position position,
      boolean eventTime,
      @Nullable Partition partition,
      @Nullable Offset offset,
      long limit,
      boolean stopAtCurrent) {

    this.name = name;
    this.reader = Objects.requireNonNull(reader);
    this.position = Objects.requireNonNull(position);
    this.eventTime = eventTime;
    this.partition = partition;
    this.offset = offset;
    this.offsetWatermark = offset == null ? LOWEST_INSTANT.getMillis() : offset.getWatermark();
    this.limit = limit;
    this.stopAtCurrent = stopAtCurrent;
    this.finished = limit <= 0;

    Preconditions.checkArgument(
        partition != null || offset != null, "Either partition or offset has to be non-null");

    Preconditions.checkArgument(
        offset == null || !stopAtCurrent, "Offset can be used only for streaming reader");
  }

  private URI getUri() {
    return reader.getUri();
  }

  public boolean start() throws IOException {
    this.observer =
        BlockingQueueLogObserver.create(
            name == null ? "Source(" + reader.getUri() + ":" + partition.getId() + ")" : name,
            limit,
            offsetWatermark);
    log.debug(
        "Starting {}@{} with offset {} and partition {}", name, reader.getUri(), offset, partition);
    if (!finished) {
      if (offset != null) {
        this.handle = reader.observeBulkOffsets(Collections.singletonList(offset), observer);
      } else {
        this.handle =
            reader.observeBulkPartitions(
                name, Collections.singletonList(partition), position, stopAtCurrent, observer);
      }
    }
    return advance();
  }

  public boolean advance() throws IOException {
    if (!finished) {
      autoCommitIfBounded();
      try {
        if (limit > 0) {
          current = takeNext();
          if (current == null) {
            lastReadWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
            return false;
          }
        } else {
          current = null;
        }
        limit--;
        if (current != null) {
          if (!eventTime) {
            currentProcessingTime = Instant.now();
          }
          lastReadWatermark = Instant.ofEpochMilli(observer.getWatermark());
          return true;
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    Throwable error = observer.getError();
    if (error != null) {
      throw new IOException(error);
    }
    lastReadWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    finished = true;
    log.debug("Finished reading observer name {}, partition {}", name, partition);
    return false;
  }

  private StreamElement takeNext() throws InterruptedException {
    if (stopAtCurrent) {
      return observer.takeBlocking();
    }
    return observer.take();
  }

  public Instant getCurrentTimestamp() {
    if (!eventTime) {
      return currentProcessingTime;
    }
    if (!finished) {
      return new Instant(getCurrent().getStamp());
    }
    return lastReadWatermark;
  }

  public StreamElement getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  public void close() {
    if (observer != null) {
      observer.stop();
      if (observer.getLastWrittenContext() != null) {
        observer.getLastWrittenContext().nack();
      }
      observer = null;
    }
    if (handle != null) {
      handle.close();
      handle = null;
    }
    log.debug("Closed reader {}", this);
  }

  @Nullable
  Offset getCurrentOffset() {
    return observer == null || observer.getLastReadContext() == null
        ? null
        : observer.getLastReadContext().getOffset();
  }

  boolean hasExternalizableOffsets() {
    return reader.hasExternalizableOffsets();
  }

  @Nullable
  OffsetCommitter getLastReadCommitter() {
    return observer == null ? null : observer.getLastReadContext();
  }

  @Nullable
  OffsetCommitter getLastWrittenCommitter() {
    return observer == null ? null : observer.getLastWrittenContext();
  }

  void clearIncomingQueue() {
    if (observer != null) {
      observer.clearIncomingQueue();
    }
  }

  private Instant getWatermark() {
    final Instant watermark;
    if (finished) {
      watermark = HIGHEST_INSTANT;
    } else if (eventTime) {
      if (observer == null) {
        // Temporary workaround for race condition in UnboundedSourceWrapper.
        log.warn(
            "Call to getWatermark() before start(). This breaks UnboundedSource.Reader contract.");
        return LOWEST_INSTANT;
      }
      watermark = new Instant(observer.getWatermark());
    } else {
      watermark = new Instant(System.currentTimeMillis() - AUTO_WATERMARK_LAG_MS);
    }
    if (watermark.isBefore(LOWEST_INSTANT)) {
      return LOWEST_INSTANT;
    }
    if (watermark.isAfter(HIGHEST_INSTANT)) {
      return HIGHEST_INSTANT;
    }
    if (watermark.isAfter(lastReadWatermark)) {
      return lastReadWatermark;
    }
    return watermark;
  }

  private void autoCommitIfBounded() {
    if (stopAtCurrent) {
      Optional.ofNullable(getLastReadCommitter()).ifPresent(OffsetCommitter::confirm);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("partition", partition)
        .add("offset", offset)
        .add("eventTime", eventTime)
        .add("stopAtCurrent", stopAtCurrent)
        .add("reader", reader)
        .toString();
  }
}
