/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.tools.io;

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.CommitLogReader.Position;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A {@code DataSource} reading from a specific attribute of entity.
 */
public class StreamSource<T> implements DataSource<T> {

  public static <T> StreamSource<T> of(
      CommitLogReader reader,
      Position position,
      boolean stopAtCurrent,
      UnaryFunction<StreamElement, T> transform) {

    return new StreamSource<>(reader, position, stopAtCurrent, transform);
  }

  final transient CommitLogReader reader;
  final transient Position position;
  final transient boolean stopAtCurrent;
  final UnaryFunction<StreamElement, T> transform;

  StreamSource(
      CommitLogReader reader,
      Position position,
      boolean stopAtCurrent,
      UnaryFunction<StreamElement, T> transform) {

    this.reader = reader;
    this.position = position;
    this.stopAtCurrent = stopAtCurrent;
    this.transform = transform;
  }

  @Override
  public List<Partition<T>> getPartitions() {
    return reader.getPartitions().stream()
        .map(p -> new Partition<T>() {
            @Override
            public Set<String> getLocations() {
              return Collections.singleton("Unknown");
            }

            @Override
            public Reader<T> openReader() throws IOException {
              BlockingQueue<Optional<T>> queue = new SynchronousQueue<>();
              AtomicReference<T> current = new AtomicReference<>();

              reader.observePartitions(
                  Arrays.asList(p),
                  position,
                  stopAtCurrent,
                  partitionObserver(queue));

              return new Reader<T>() {

                @Override
                public void close() throws IOException {
                }

                @Override
                public boolean hasNext() {
                  try {
                    Optional<T> elem = queue.take();
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
                public T next() {
                  return current.get();
                }
              };
            }

          })
        .collect(Collectors.toList());
  }

  private LogObserver partitionObserver(
      BlockingQueue<Optional<T>> queue) {

    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest,
          LogObserver.ConfirmCallback confirm) {
        T value = transform.apply(ingest);
        try {
          queue.put(Optional.of(value));
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          return false;
        }
        confirm.confirm();
        return true;
      }

      @Override
      public void onError(Throwable error) {
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

      @Override
      public void close() {

      }
    };
  }


  @Override
  public boolean isBounded() {
    return stopAtCurrent;
  }

}
