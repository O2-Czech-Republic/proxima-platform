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
package cz.o2.proxima.source;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Position;
import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.io.UnsplittableBoundedSource;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@code DataSource} reading from a specific attribute of entity with bounded
 * characteristics.
 */
@Stable
public class BoundedStreamSource implements BoundedDataSource<StreamElement> {

  public static BoundedStreamSource of(
      CommitLogReader reader,
      Position position) {

    return new BoundedStreamSource(reader, position);
  }

  final CommitLogReader reader;
  final Position position;

  BoundedStreamSource(
      CommitLogReader reader,
      Position position) {

    this.reader = reader;
    this.position = position;
  }

  private BoundedReader<StreamElement> asBoundedReader(Partition p) {

    BlockingQueue<Optional<StreamElement>> queue = new ArrayBlockingQueue<>(100);

    AtomicReference<ObserveHandle> cancel = new AtomicReference<>();
    cancel.set(reader.observePartitions(
        Arrays.asList(p),
        position,
        true,
        partitionObserver(queue)));

    return new BoundedReader<StreamElement>() {

      @Nullable
      StreamElement current = null;

      @Override
      public void close() throws IOException {
        cancel.get().cancel();
      }

      @Override
      public boolean hasNext() {
        try {
          if (current != null) {
            return true;
          }
          Optional<StreamElement> elem = queue.take();
          if (elem.isPresent()) {
            current = elem.get();
            return true;
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          current = null;
        }
        return false;
      }

      @Override
      public StreamElement next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        StreamElement next = current;
        current = null;
        return next;
      }

    };
  }

  private LogObserver partitionObserver(
      BlockingQueue<Optional<StreamElement>> queue) {

    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter confirm) {

        try {
          return enqueue(queue, ingest, confirm);
        } catch (Exception ex) {
          confirm.fail(ex);
          throw new RuntimeException(ex);
        }
      }

      @Override
      public boolean onError(Throwable error) {
        onCompleted();
        throw new RuntimeException(error);
      }

      @Override
      public void onCompleted() {
        try {
          queue.put(Optional.empty());
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }

    };
  }

  private boolean enqueue(
      BlockingQueue<Optional<StreamElement>> queue,
      StreamElement ingest, LogObserver.OffsetCommitter confirm) {

    try {
      queue.put(Optional.of(ingest));
      confirm.confirm();
      return true;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      confirm.fail(ex);
      return false;
    }
  }

  @Override
  public List<BoundedDataSource<StreamElement>> split(long desiredSplitBytes) {
    return reader.getPartitions().stream().map(p ->
        new UnsplittableBoundedSource<StreamElement>() {

          @Override
          public Set<String> getLocations() {
            return Collections.singleton("unknown");
          }

          @Override
          public BoundedReader<StreamElement> openReader() throws IOException {
            return asBoundedReader(p);
          }

        })
    .collect(Collectors.toList());
  }

  @Override
  public Set<String> getLocations() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public BoundedReader<StreamElement> openReader() throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

}
