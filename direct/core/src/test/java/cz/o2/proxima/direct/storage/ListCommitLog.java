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
package cz.o2.proxima.direct.storage;

import com.google.common.collect.Lists;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.ObserverUtils;
import static cz.o2.proxima.direct.commitlog.ObserverUtils.asRepartitionContext;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A bounded {@link CommitLogReader} containing predefined data.
 *
 * This is very simplistic implementation which just pushes all data
 * to the provided observer.
 */
public class ListCommitLog implements CommitLogReader {

  private static final Partition PARTITION = () -> 0;

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
      return Arrays.asList((Offset) () -> PARTITION);
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
  public URI getUri() {
    try {
      return new URI("list://" + this);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public List<Partition> getPartitions() {
    return Arrays.asList(PARTITION);
  }

  @Override
  public ObserveHandle observe(
      String name, Position position, LogObserver observer) {

    pushTo(element -> observer.onNext(
        element,
        asOnNextContext(
            (succ, exc) -> {
              if (!succ) {
                observer.onError(exc);
              }
            })),
        observer::onCompleted);
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
      LogObserver observer) {

    observer.onRepartition(asRepartitionContext(Arrays.asList(PARTITION)));
    pushTo(element -> observer.onNext(
        element, asOnNextContext(
            (succ, exc) -> {
              if (!succ) {
                observer.onError(exc);
              }
            })),
        observer::onCompleted);
    return new NopObserveHandle();
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name, Collection<Partition> partitions,
      Position position, boolean stopAtCurrent, LogObserver observer) {

    return observeBulk(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, LogObserver observer) {

    return observeBulk(null, null, observer);
  }

  @Override
  public void close() throws IOException {
    // nop
  }

  private void pushTo(
      Consumer<StreamElement> consumer,
      Runnable finish) {

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

  private static LogObserver.OnNextContext asOnNextContext(
      LogObserver.OffsetCommitter offsetCommitter) {

    return ObserverUtils.asOnNextContext(
        offsetCommitter, PARTITION, System::currentTimeMillis);
  }



}
