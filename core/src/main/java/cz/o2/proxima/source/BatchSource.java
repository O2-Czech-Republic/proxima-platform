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
package cz.o2.proxima.source;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.batch.BatchLogObserver;
import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.io.UnsplittableBoundedSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Source reading from {@code BatchLogObservable}.
 */
@Stable
@Slf4j
public class BatchSource implements BoundedDataSource<StreamElement> {

  public static BatchSource of(
      BatchLogObservable observable,
      List<AttributeDescriptor<?>> attrs,
      long startStamp,
      long endStamp) {

    return new BatchSource(observable, attrs, startStamp, endStamp);
  }

  public static BatchSource of(
      BatchLogObservable observable,
      AttributeFamilyDescriptor family,
      long startStamp,
      long endStamp) {

    return new BatchSource(
        observable, family.getAttributes(), startStamp, endStamp);
  }

  private static class Observer implements BatchLogObserver {

    @Getter
    BlockingQueue<Optional<StreamElement>> queue = new SynchronousQueue<>();

    boolean stop = false;

    @Override
    public boolean onNext(StreamElement element) {
      try {
        queue.put(Optional.of(element));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        log.warn("Interrupted while forwarding element to queue.");
      }
      return !stop;
    }

    public void stop() {
      this.stop = true;
    }

    @Override
    public void onCompleted() {
      try {
        queue.put(Optional.empty());
      } catch (InterruptedException ex) {
        log.warn("Interrupted while forwarding EOS.");
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void onError(Throwable error) {
      throw new RuntimeException(error);
    }

  }

  private final BatchLogObservable observable;
  private final List<AttributeDescriptor<?>> attributes;
  private final long startStamp;
  private final long endStamp;

  private BatchSource(
      BatchLogObservable observable,
      List<AttributeDescriptor<?>> attributes,
      long startStamp,
      long endStamp) {

    this.observable = observable;
    this.attributes = attributes;
    this.startStamp = startStamp;
    this.endStamp = endStamp;
  }

  @Override
  public Set<String> getLocations() {
    return Collections.singleton("unknown");
  }

  @Override
  public BoundedReader<StreamElement> openReader() throws IOException {
    throw new UnsupportedOperationException("Not supported. Call `split` first.");
  }


  @Override
  @SuppressWarnings("unchecked")
  public List<BoundedDataSource<StreamElement>> split(long desiredSplitBytes) {
    return observable.getPartitions(startStamp, endStamp)
        .stream()
        .map(p ->
            new UnsplittableBoundedSource<StreamElement>() {
              @Override
              public Set<String> getLocations() {
                return Collections.singleton("unknown");
              }

              @Override
              public BoundedReader<StreamElement> openReader() throws IOException {
                Observer observer = new Observer();
                observable.observe(Arrays.asList(p), attributes, observer);
                return new BoundedReader<StreamElement>() {

                  @Nullable
                  StreamElement current = null;

                  @Override
                  public void close() throws IOException {
                    observer.stop();
                  }

                  @Override
                  public boolean hasNext() {
                    try {
                      if (current != null) {
                        return true;
                      }
                      current = observer.getQueue().take().orElse(null);
                    } catch (InterruptedException ex) {
                      log.warn("Interrupted while trying to retrieve next element.");
                      Thread.currentThread().interrupt();
                      current = null;
                    }
                    return current != null;
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

            })
        .collect(Collectors.toList());
  }

}
