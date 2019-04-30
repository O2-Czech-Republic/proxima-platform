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
package cz.o2.proxima.beam.direct.io;

import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * A {@link Reader} created from {@link CommitLogReader}.
 */
@Slf4j
class BeamCommitLogReader {

  private static final Instant LOWEST_INSTANT = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private static final Instant HIGHEST_INSTANT = BoundedWindow.TIMESTAMP_MAX_VALUE;
  private static final byte[] EMPTY_BYTES = new byte[] { };
  // FIXME: configuration
  private static final long AUTO_WATERMARK_LAG_MS = 500;

  static class UnboundedCommitLogReader extends UnboundedReader<StreamElement> {

    private final DirectUnboundedSource source;
    @Getter
    private final BeamCommitLogReader reader;

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
      this.reader = new BeamCommitLogReader(
          name, reader, position, eventTime,
          partition, offset, limit, false);
    }

    @Override
    public DirectUnboundedSource getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
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
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public Instant getWatermark() {
      if (finished) {
        return HIGHEST_INSTANT;
      }
      return reader.getWatermark();
    }

    @Override
    public DirectUnboundedSource.Checkpoint getCheckpointMark() {
      DirectUnboundedSource.Checkpoint ret = new DirectUnboundedSource.Checkpoint(
          reader);
      return ret;
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

    BeamCommitLogReader r = new BeamCommitLogReader(
        name, reader, position, true, partition, null, limit, true);

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
      public void close() throws IOException {
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
        name, source, reader, position, eventTime,
        limit, partition, offset);

  }

  @Getter
  private final Partition partition;
  @Getter
  private ObserveHandle handle;

  @Nullable
  private final String name;
  private final CommitLogReader reader;
  private final Position position;
  private final boolean eventTime;
  private final boolean stopAtCurrent;
  private boolean finished;
  @Getter
  private long limit;
  @Nullable
  private final Offset offset;
  private final long offsetWatermark;
  @Nullable
  private BlockingQueueLogObserver observer;
  private StreamElement current;
  private Instant currentProcessingTime = Instant.now();

  private BeamCommitLogReader(
      String name, CommitLogReader reader, Position position, boolean eventTime,
      @Nullable Partition partition, @Nullable Offset offset, long limit,
      boolean stopAtCurrent) {

    this.name = name;
    this.reader = Objects.requireNonNull(reader);
    this.position = Objects.requireNonNull(position);
    this.eventTime = eventTime;
    this.partition = partition;
    this.offset = offset;
    this.offsetWatermark = offset == null
        ? LOWEST_INSTANT.getMillis()
        : offset.getWatermark();
    this.limit = limit;
    this.stopAtCurrent = stopAtCurrent;
    this.finished = limit <= 0;

    Preconditions.checkArgument(
        partition != null || offset != null,
        "Either partition or offset has to be non-null");

    Preconditions.checkArgument(
        offset == null || !stopAtCurrent,
        "Offset can be used only for streaming reader");
  }

  public BoundedSource<StreamElement> getCurrentSource() {
    throw new UnsupportedOperationException("Unsupported.");
  }

  public boolean start() throws IOException {
    this.observer = BlockingQueueLogObserver.create(name, limit, offsetWatermark);
    if (!finished) {
      if (offset != null) {
        this.handle = reader.observeBulkOffsets(Arrays.asList(offset), observer);
      } else {
        this.handle = reader.observeBulkPartitions(
            name, Arrays.asList(partition),
            position, stopAtCurrent, observer);
      }
    }
    return advance();
  }

  public boolean advance() throws IOException {
    if (!finished) {
      try {
        if (limit > 0) {
          current = takeNext();
          if (current == null) {
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
    finished = true;
    return false;
  }

  private StreamElement takeNext() throws InterruptedException {
    if (stopAtCurrent) {
      return observer.takeBlocking();
    }
    return observer.take();
  }

  public Instant getCurrentTimestamp() {
    if (!finished) {
      if (eventTime) {
        return new Instant(getCurrent().getStamp());
      }
      return currentProcessingTime;
    }
    return HIGHEST_INSTANT;
  }

  public StreamElement getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  public void close() throws IOException {
    observer.stop();
    if (handle != null) {
      handle.cancel();
      handle = null;
    }
    if (observer.getLastWrittenContext() != null) {
      observer.getLastWrittenContext().nack();
    }
    reader.close();
  }

  @Nullable Offset getCurrentOffset() {
    return observer == null || observer.getLastReadContext() == null
        ? null
        : observer.getLastReadContext().getOffset();
  }

  boolean hasExternalizableOffsets() {
    return reader.hasExternalizableOffsets();
  }

  @Nullable OffsetCommitter getLastReadCommitter() {
    return observer == null ? null : observer.getLastReadContext();
  }

  @Nullable OffsetCommitter getLastWrittenCommitter() {
    return observer == null ? null : observer.getLastWrittenContext();
  }

  void clearIncomingQueue() {
    observer.clearIncomingQueue();
  }

  private Instant getWatermark() {
    if (finished) {
      return HIGHEST_INSTANT;
    }
    if (eventTime) {
      return new Instant(observer.getWatermark());
    }
    return new Instant(System.currentTimeMillis() - AUTO_WATERMARK_LAG_MS);
  }

}
