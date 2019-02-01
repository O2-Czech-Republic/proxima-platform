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

import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

/**
 * An {@link BoundedSource} created from direct operator's {@link CommitLogReader}.
 */
@Slf4j
class DirectBoundedSource extends BoundedSource<StreamElement> {

  static DirectBoundedSource of(
      Repository repo, CommitLogReader reader, Position position) {

    return new DirectBoundedSource(repo, reader, position, -1);
  }

  private final Repository repo;
  private final CommitLogReader reader;
  private final Position position;
  private final int partitions;
  private final int splitId;

  DirectBoundedSource(
      Repository repo,
      CommitLogReader reader,
      Position position,
      int splitId) {

    this.repo = repo;
    this.reader = reader;
    this.position = position;
    this.partitions = reader.getPartitions().size();
    this.splitId = splitId;
  }

  @Override
  public List<BoundedSource<StreamElement>> split(
      long desiredBundleSizeBytes, PipelineOptions opts) throws Exception {

    if (splitId != -1) {
      return Arrays.asList(this);
    }
    List<BoundedSource<StreamElement>> ret = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      ret.add(new DirectBoundedSource(repo, reader, position, i));
    }
    return ret;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return -1L;
  }

  @Override
  public BoundedReader<StreamElement> createReader(
      PipelineOptions options) throws IOException {

    log.debug(
        "Creating reader reading from position {} on partition {}",
        position,
        splitId);

    BlockingQueueLogObserver observer = BlockingQueueLogObserver.create();
    ObserveHandle handle = reader.observeBulkPartitions(
        Arrays.asList(reader.getPartitions().get(splitId)),
        position, true, observer);

    return new BoundedReader<StreamElement>() {

      boolean finished = false;
      StreamElement current;

      @Override
      public BoundedSource<StreamElement> getCurrentSource() {
        return DirectBoundedSource.this;
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        try {
          if (!finished) {
            Optional<StreamElement> taken = observer.take();
            if (taken.isPresent()) {
              current = taken.get();
              log.info(" *** taken " + current);
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
          return new Instant(Long.MIN_VALUE);
        }
        return new Instant(Long.MAX_VALUE);
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

    };
  }

  @Override
  public Coder<StreamElement> getOutputCoder() {
    return StreamElementCoder.of(repo);
  }

}
