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
package cz.o2.proxima.direct.euphoria.source;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.direct.commitlog.BulkLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.commitlog.Position;
import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import cz.seznam.euphoria.core.client.io.UnboundedPartition;
import cz.seznam.euphoria.core.client.io.UnboundedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@code DataSource} reading from a specific attribute of entity.
 */
@Stable
@Slf4j
public class UnboundedStreamSource
    implements UnboundedDataSource<StreamElement, Offset> {

  public static UnboundedStreamSource of(
      CommitLogReader reader,
      Position position) {

    return new UnboundedStreamSource(null, reader, position);
  }

  public static UnboundedStreamSource of(
      @Nullable String name,
      CommitLogReader reader,
      Position position) {

    return new UnboundedStreamSource(name, reader, position);
  }


  final CommitLogReader reader;
  final Position position;
  @Nullable final String consumer;

  UnboundedStreamSource(
      @Nullable String consumer,
      CommitLogReader reader,
      Position position) {

    this.consumer = consumer;
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

      final BlockingQueue<BulkLogObserver.OffsetCommitter> committers;
      committers = new LinkedBlockingDeque<>();
      AtomicReference<ObserveHandle> handle = new AtomicReference<>();
      handle.set(reader.observeBulkPartitions(
          consumer,
          Arrays.asList(p),
          position,
          partitionObserver(queue, committers)));

      return new UnboundedReader<StreamElement, Offset>() {

        @Nullable
        StreamElement current = null;

        @Override
        public void close() throws IOException {
          handle.get().cancel();
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
          List<BulkLogObserver.OffsetCommitter> toCommit = new ArrayList<>();
          committers.drainTo(toCommit);
          toCommit.forEach(BulkLogObserver.OffsetCommitter::confirm);
        }

      };
    };

  }

  private BulkLogObserver partitionObserver(
      BlockingQueue<Optional<StreamElement>> queue,
      BlockingQueue<BulkLogObserver.OffsetCommitter> committers) {

    return new BulkLogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        try {
          return enqueue(queue, committers, context, ingest);
        } catch (Exception ex) {
          context.fail(ex);
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
      BlockingQueue<BulkLogObserver.OffsetCommitter> committers,
      OffsetCommitter confirm,
      StreamElement ingest) {

    try {
      committers.add(confirm);
      queue.put(Optional.of(ingest));
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      confirm.fail(ex);
      return false;
    }
    return true;
  }

}
