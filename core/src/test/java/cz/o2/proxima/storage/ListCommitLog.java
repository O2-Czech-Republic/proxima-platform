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
package cz.o2.proxima.storage;

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A bounded {@link CommitLogReader} containing predefined data.
 *
 * This is very simplistic implementation which just pushes all data
 * to the provided observer.
 */
public class ListCommitLog implements CommitLogReader {

  public static ListCommitLog of(List<StreamElement> data, Context context) {
    return new ListCommitLog(data, context);
  }

  private final List<StreamElement> data;
  private final Context context;
  private transient ExecutorService executor;

  private static final class NopObserveHandle implements ObserveHandle {

    @Override
    public void cancel() {

    }

    @Override
    public List<Offset> getCommittedOffsets() {
      return Arrays.asList((Offset) () -> () -> 0);
    }

    @Override
    public void resetOffsets(List<Offset> offsets) {

    }

    @Override
    public List<Offset> getCurrentOffsets() {
      return getCommittedOffsets();
    }

    @Override
    public void waitUntilReady() throws InterruptedException {

    }

  }

  private ListCommitLog(List<StreamElement> data, Context context) {
    this.data = Lists.newArrayList(data);
    this.context = context;
  }

  @Override
  public URI getURI() {
    try {
      return new URI("list://" + this);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public List<Partition> getPartitions() {
    return Arrays.asList(() -> 0);
  }

  @Override
  public ObserveHandle observe(
      String name, Position position, LogObserver observer) {

    pushTo(element -> observer.onNext(element, (succ, exc) -> {
      if (!succ) {
        observer.onError(exc);
      }
    }), observer::onCompleted);
    return new NopObserveHandle();
  }

  @Override
  public ObserveHandle observePartitions(
      String name, Collection<Partition> partitions,
      Position position, boolean stopAtCurrent, LogObserver observer) {

    return observe(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulk(
      String name, Position position, boolean stopAtCurrent,
      BulkLogObserver observer) {

    observer.onRestart(Arrays.asList(() -> () -> 0));
    pushTo(element -> observer.onNext(element, () -> 0, (succ, exc) -> {
      if (!succ) {
        observer.onError(exc);
      }
    }), observer::onCompleted);
    return new NopObserveHandle();
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name, Collection<Partition> partitions,
      Position position, boolean stopAtCurrent, BulkLogObserver observer) {

    return observeBulk(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, BulkLogObserver observer) {

    return observeBulk(null, null, observer);
  }

  @Override
  public void close() throws IOException {
    // nop
  }

  private void pushTo(
      Consumer<StreamElement> consumer,
      Runnable finish) {
    AtomicInteger toPush = new AtomicInteger(data.size());
    executor().execute(() -> {
      data.forEach(consumer::accept);
      finish.run();
    });
  }

  private ExecutorService executor() {
    if (executor == null) {
      executor = context.getExecutorService();
    }
    return executor;
  }

}
