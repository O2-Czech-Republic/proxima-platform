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

import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.common.base.Preconditions;
import org.joda.time.Instant;

/**
 * A {@link Reader} created from {@link CommitLogReader}.
 */
class BeamCommitLogReader extends Reader<StreamElement> {

  private static final Instant LOWEST_INSTANT = new Instant(Long.MIN_VALUE);
  private static final Instant HIGHEST_INSTANT = new Instant(Long.MAX_VALUE);
  private static final byte[] EMPTY_BYTES = new byte[] { };

  static class UnboundedCommitLogReader extends UnboundedReader<StreamElement> {

    private final AtomicLong watermark = new AtomicLong(Long.MIN_VALUE);
    private final DirectUnboundedSource source;
    @Getter
    private final BeamCommitLogReader reader;

    UnboundedCommitLogReader(
        String name,
        DirectUnboundedSource source,
        CommitLogReader reader,
        Position position,
        long limit,
        int splitId,
        @Nullable
        Offset offset) {

      this.source = source;
      this.reader = new BeamCommitLogReader(
          name, reader, position, splitId, offset, limit, false);
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
      boolean next = reader.advance();
      if (next) {
        long stamp = reader.getCurrent().getStamp();
        watermark.accumulateAndGet(stamp, Math::max);
      } else {
        watermark.accumulateAndGet(reader.getCurrentTimestamp().getMillis(), Math::max);
      }
      return next;
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
      return new Instant(watermark.get());
    }

    @Override
    public DirectUnboundedSource.Checkpoint getCheckpointMark() {
      return new DirectUnboundedSource.Checkpoint(
          reader.getCurrentOffset(),
          reader.getLimit(),
          reader.hasExternalizableOffsets() ? null : reader.getLastCommitter());
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      Instant boundedPos = reader.getCurrentTimestamp();
      if (boundedPos == HIGHEST_INSTANT) {
        watermark.set(boundedPos.getMillis());
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
      int splitId) {

    BeamCommitLogReader r = new BeamCommitLogReader(
        name, reader, position, splitId, null, limit, true);

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
      long limit,
      int splitId,
      @Nullable
      Offset offset) {

    return new UnboundedCommitLogReader(
        name, source, reader, position, limit, splitId, offset);

  }

  @Getter
  private final int splitId;
  @Getter
  private transient ObserveHandle handle;

  private final String name;
  private final CommitLogReader reader;
  private final Position position;
  private final boolean stopAtCurrent;
  private boolean finished = false;
  @Getter
  private long limit;
  @Nullable
  private final Offset offset;
  private transient BlockingQueueLogObserver observer;
  private transient StreamElement current;

  private BeamCommitLogReader(
      String name, CommitLogReader reader, Position position,
      int splitId, @Nullable Offset offset, long limit,
      boolean stopAtCurrent) {

    this.name = name;
    this.reader = reader;
    this.position = position;
    this.splitId = splitId;
    this.stopAtCurrent = stopAtCurrent;
    this.offset = offset;
    this.limit = limit;

    Preconditions.checkArgument(
        splitId != -1 || offset != null,
        "Either splitId has to be non-negative or offset has to be non-null");

    Preconditions.checkArgument(
        offset == null || !stopAtCurrent,
        "Offset can be used only for streaming reader");
  }

  @Override
  public BoundedSource<StreamElement> getCurrentSource() {
    throw new UnsupportedOperationException("Unsupported.");
  }

  @Override
  public boolean start() throws IOException {
    this.observer = BlockingQueueLogObserver.create(name, limit);
    if (offset != null) {
      this.handle = reader.observeBulkOffsets(
          Arrays.asList(offset), observer);
    } else {
      this.handle = reader.observeBulkPartitions(
          name, Arrays.asList(reader.getPartitions().get(splitId)),
          position, stopAtCurrent, observer);
    }
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    try {
      if (!finished) {
        current = limit-- > 0L ? observer.take() : null;
        if (current != null) {
          return true;
        }
      }
      Throwable error = observer.getError();
      if (error != null) {
        throw new IOException(error);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    finished = true;
    return false;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (!finished) {
      return LOWEST_INSTANT;
    }
    return HIGHEST_INSTANT;
  }

  @Override
  public StreamElement getCurrent() throws NoSuchElementException {
    return current;
  }

  @Override
  public void close() throws IOException {
    handle.cancel();
    reader.close();
  }

  private @Nullable Offset getCurrentOffset() {
    return observer.getLastContext() == null
        ? null
        : observer.getLastContext().getOffset();
  }

  private boolean hasExternalizableOffsets() {
    return reader.hasExternalizableOffsets();
  }

  private @Nullable OffsetCommitter getLastCommitter() {
    return observer.getLastContext();
  }

}
