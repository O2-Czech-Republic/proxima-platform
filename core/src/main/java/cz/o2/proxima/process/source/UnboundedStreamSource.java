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
package cz.o2.proxima.process.source;

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.Cancellable;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
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
    implements UnboundedDataSource<StreamElement, BulkLogObserver.OffsetContext> {

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
  public List<UnboundedPartition<StreamElement, BulkLogObserver.OffsetContext>> getPartitions() {
    return reader.getPartitions().stream()
        .map(this::asUnboundedPartition)
        .collect(Collectors.toList());
  }

  private UnboundedPartition<StreamElement, BulkLogObserver.OffsetContext> asUnboundedPartition(
      Partition p) {

    return new UnboundedPartition<StreamElement, BulkLogObserver.OffsetContext>() {

      AtomicReference<BulkLogObserver.OffsetContext> last = new AtomicReference<>();

      @Override
      public UnboundedReader<StreamElement, BulkLogObserver.OffsetContext> openReader() throws IOException {
        BlockingQueue<Optional<StreamElement>> queue = new ArrayBlockingQueue<>(100);
        AtomicReference<StreamElement> current = new AtomicReference<>();

        AtomicReference<Cancellable> cancel = new AtomicReference<>();
        cancel.set(reader.observeBulkPartitions(
            Arrays.asList(p),
            position,
            partitionObserver(last, queue)));

        return new UnboundedReader<StreamElement, BulkLogObserver.OffsetContext>() {

          @Override
          public void close() throws IOException {
            cancel.get().cancel();
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
          public BulkLogObserver.OffsetContext getCurrentOffset() {
            return last.get();
          }

          @Override
          public void reset(BulkLogObserver.OffsetContext offset) {
            cancel.get().cancel();
            current.set(null);
            queue.clear();
            List<Offset> committedOffsets = offset.getCommittedOffsets();
            log.info("Restarting processing to committed offsets {}", committedOffsets);
            cancel.set(reader.observeBulkOffsets(
                committedOffsets,
                partitionObserver(last, queue)));
          }

          @Override
          public void commitOffset(BulkLogObserver.OffsetContext offset) {
            offset.confirm();
          }

        };
      }
    };

  }

  private BulkLogObserver partitionObserver(
      AtomicReference<BulkLogObserver.OffsetContext> last,
      BlockingQueue<Optional<StreamElement>> queue) {

    return new BulkLogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, BulkLogObserver.OffsetContext confirm) {

        try {
          try {
            queue.put(Optional.of(ingest));
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
          }
          last.set(confirm);
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
