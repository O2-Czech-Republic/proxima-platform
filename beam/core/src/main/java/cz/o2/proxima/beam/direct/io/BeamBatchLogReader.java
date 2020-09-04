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

import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/** A {@link BoundedReader} reading from {@link BatchLogReader}. */
@Slf4j
class BeamBatchLogReader extends BoundedReader<StreamElement> {

  private static final Instant LOWEST_INSTANT = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private static final Instant HIGHEST_INSTANT = BoundedWindow.TIMESTAMP_MAX_VALUE;

  static BeamBatchLogReader of(
      DirectBatchSource source,
      BatchLogReader reader,
      List<AttributeDescriptor<?>> attrs,
      Partition split,
      long startStamp,
      long endStamp) {

    return new BeamBatchLogReader(source, reader, attrs, split, startStamp, endStamp);
  }

  private final DirectBatchSource source;
  private final BatchLogReader reader;
  private final List<AttributeDescriptor<?>> attrs;
  private final Partition split;
  private final long startStamp;
  private final long endStamp;

  private StreamElement current;
  private BlockingQueueLogObserver observer;
  private boolean finished = false;

  private BeamBatchLogReader(
      DirectBatchSource source,
      BatchLogReader reader,
      List<AttributeDescriptor<?>> attrs,
      Partition split,
      long startStamp,
      long endStamp) {

    this.source = Objects.requireNonNull(source);
    this.reader = Objects.requireNonNull(reader);
    this.attrs = Objects.requireNonNull(attrs);
    this.split = Objects.requireNonNull(split);
    this.startStamp = startStamp;
    this.endStamp = endStamp;
  }

  @Override
  public BoundedSource<StreamElement> getCurrentSource() {
    return source;
  }

  @Override
  public boolean start() throws IOException {
    this.observer =
        BlockingQueueLogObserver.create("Source(" + split + ")", LOWEST_INSTANT.getMillis());
    reader.observe(Arrays.asList(split), attrs, observer);
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    for (; ; ) {
      try {
        current = observer.takeBlocking();
        if (current == null || current.getStamp() >= startStamp && current.getStamp() < endStamp) {
          // accept the taken element
          break;
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        close();
        return false;
      }
    }
    if (observer.getError() != null) {
      throw new IOException(observer.getError());
    }
    if (current != null) {
      return true;
    }
    finished = true;
    return false;
  }

  @Override
  public StreamElement getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public void close() throws IOException {
    // missing observe handle in observing batch log
    // @todo
    log.debug("Closing partition {}", split);
    observer.stop();
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (!finished) {
      return LOWEST_INSTANT;
    }
    return HIGHEST_INSTANT;
  }
}
