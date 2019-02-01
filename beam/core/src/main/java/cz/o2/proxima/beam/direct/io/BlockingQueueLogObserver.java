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

import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.storage.StreamElement;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link LogObserver} that caches data in {@link BlockingQueue}.
 */
@Slf4j
class BlockingQueueLogObserver implements LogObserver {

  static BlockingQueueLogObserver create() {
    return new BlockingQueueLogObserver(BlockingQueueLogObserver::createQueue);
  }

  private static BlockingQueue<Optional<StreamElement>> createQueue() {
    return new ArrayBlockingQueue<>(100);
  }

  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final BlockingQueue<Optional<StreamElement>> queue;

  private BlockingQueueLogObserver(
      Supplier<BlockingQueue<Optional<StreamElement>>> queueSupplier) {

    queue = queueSupplier.get();
  }

  @Override
  public boolean onError(Throwable error) {
    this.error.set(error);
    return false;
  }

  @Override
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    log.debug("Received next element {}", ingest);
    queue.add(Optional.of(ingest));
    return true;
  }

  @Override
  public void onCancelled() {
    queue.add(Optional.empty());
  }

  @Override
  public void onCompleted() {
    queue.add(Optional.empty());
  }

  Optional<StreamElement> take() throws InterruptedException {
    return queue.take();
  }

  Throwable getError() {
    return error.get();
  }

}
