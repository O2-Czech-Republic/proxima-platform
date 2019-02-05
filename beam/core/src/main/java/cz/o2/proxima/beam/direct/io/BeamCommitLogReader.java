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
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.joda.time.Instant;

/**
 * A {@link Reader} created from {@link CommitLogReader}.
 */
class BeamCommitLogReader extends Reader<StreamElement> {

  private static final Instant LOWEST_INSTANT = new Instant(Long.MIN_VALUE);
  private static final Instant HIGHEST_INSTANT = new Instant(Long.MAX_VALUE);
  private static final byte[] EMPTY_BYTES = new byte[] { };

  static BoundedReader<StreamElement> bounded(
      BoundedSource<StreamElement> source,
      CommitLogReader reader,
      Position position,
      long limit,
      int splitId) {

    BeamCommitLogReader r = new BeamCommitLogReader(
        reader, position, splitId, limit);

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

  static <C extends CheckpointMark> UnboundedReader<StreamElement> unbounded(
      UnboundedSource<StreamElement, C> source,
      C checkpointMark,
      CommitLogReader reader,
      Position position,
      long limit,
      int splitId) {

    BeamCommitLogReader r = new BeamCommitLogReader(
        reader, position, splitId, limit);
    AtomicLong watermark = new AtomicLong(Long.MIN_VALUE);

    return new UnboundedReader<StreamElement>() {

      @Override
      public UnboundedSource<StreamElement, ?> getCurrentSource() {
        return source;
      }

      @Override
      public boolean start() throws IOException {
        return r.start();
      }

      @Override
      public boolean advance() throws IOException {
        boolean next = r.advance();
        if (next) {
          long stamp = r.getCurrent().getStamp();
          watermark.accumulateAndGet(stamp, Math::max);
        } else {
          watermark.accumulateAndGet(r.getCurrentTimestamp().getMillis(), Math::max);
        }
        return next;
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
      public Instant getWatermark() {
        return new Instant(watermark.get());
      }

      @Override
      public C getCheckpointMark() {
        return checkpointMark;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        Instant boundedPos = r.getCurrentTimestamp();
        if (boundedPos == HIGHEST_INSTANT) {
          watermark.set(boundedPos.getMillis());
          return boundedPos;
        }
        StreamElement current = r.getCurrent();
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

    };

  }

  private final CommitLogReader reader;
  private final Position position;
  private final int splitId;
  private transient ObserveHandle handle;
  private transient BlockingQueueLogObserver observer;
  boolean finished = false;
  StreamElement current;
  long limit;

  private BeamCommitLogReader(
      CommitLogReader reader, Position position,
      int splitId, long limit) {

    this.reader = reader;
    this.position = position;
    this.splitId = splitId;
    this.limit = limit;
  }

  @Override
  public BoundedSource<StreamElement> getCurrentSource() {
    throw new UnsupportedOperationException("Unsupported.");
  }

  @Override
  public boolean start() throws IOException {
    this.observer = BlockingQueueLogObserver.create();
    this.handle = reader.observeBulkPartitions(
        Arrays.asList(reader.getPartitions().get(splitId)),
        position, true, observer);
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    try {
      if (!finished) {
        Optional<StreamElement> taken = limit-- > 0L
            ? observer.take()
            : Optional.empty();
        if (taken.isPresent()) {
          current = taken.get();
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

}
