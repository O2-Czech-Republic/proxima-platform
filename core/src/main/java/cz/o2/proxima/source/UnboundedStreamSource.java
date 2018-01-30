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

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import cz.seznam.euphoria.core.client.io.UnboundedPartition;
import cz.seznam.euphoria.core.client.io.UnboundedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@code DataSource} reading from a specific attribute of entity.
 */
@Slf4j
public class UnboundedStreamSource
    implements UnboundedDataSource<StreamElement, Offset> {

  public static UnboundedStreamSource of(
      CommitLogReader reader,
      Position position) {

    return new UnboundedStreamSource(reader, position);
  }

  final CommitLogReader reader;
  final Position position;

  UnboundedStreamSource(
      CommitLogReader reader,
      Position position) {

    this.reader = reader;
    this.position = position;
  }

  @Override
  public List<UnboundedPartition<StreamElement, Offset>> getPartitions() {
    return reader.getPartitions().stream()
        .map(this::asUnboundedPartition)
        .collect(Collectors.toList());
  }

  private UnboundedPartition<StreamElement, Offset> asUnboundedPartition(
      Partition p) {

    return () -> {

      BlockingQueue<Optional<StreamElement>> queue = new ArrayBlockingQueue<>(100);
      AtomicReference<StreamElement> current = new AtomicReference<>();

      AtomicReference<ObserveHandle> handle = new AtomicReference<>();
      handle.set(reader.observeBulkPartitions(
          Arrays.asList(p),
          position,
          partitionObserver(queue)));

      return new UnboundedReader<StreamElement, Offset>() {

        @Override
        public void close() throws IOException {
          handle.get().cancel();
        }

        @Override
        public boolean hasNext() {
          try {
            Optional<StreamElement> elem = queue.take();
            if (elem.isPresent()) {
              current.set(elem.get());
              return true;
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
          return false;
        }

        @Override
        public StreamElement next() {
          return current.get();
        }

        @Override
        public Offset getCurrentOffset() {
          return handle.get().getCurrentOffsets().get(0);
        }

        @Override
        public void reset(Offset offset) {
          handle.get().resetOffsets(Arrays.asList(offset));
        }

        @Override
        public void commitOffset(Offset offset) {
          // nop, don't commit externally the offset at all
        }

      };
    };

  }

  private BulkLogObserver partitionObserver(
      BlockingQueue<Optional<StreamElement>> queue) {

    return new BulkLogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, BulkLogObserver.OffsetCommitter confirm) {

        try {
          try {
            queue.put(Optional.of(ingest));
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
          }
          return true;
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

}
