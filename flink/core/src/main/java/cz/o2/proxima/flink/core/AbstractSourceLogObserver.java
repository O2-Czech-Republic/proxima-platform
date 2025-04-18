/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.flink.core;

import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.direct.core.LogObserver;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Log observer that writes consumed elements to {@link SourceFunction.SourceContext}.
 *
 * @param <OutputT> Type of extracted element.
 */
abstract class AbstractSourceLogObserver<
        OffsetT extends Serializable, ContextT extends LogObserver.OnNextContext<OffsetT>, OutputT>
    implements LogObserver<OffsetT, ContextT> {

  private final CountDownLatch completed = new CountDownLatch(1);
  private final Set<Partition> seenPartitions = new HashSet<>();

  @Getter private final SourceFunction.SourceContext<OutputT> sourceContext;
  private final ResultExtractor<OutputT> resultExtractor;

  /**
   * When restoring from checkpoint, we need to skip the first element in each partition, because it
   * has been already consumed.
   */
  private final Set<Partition> skipFirstElementFromEachPartition;

  private long watermark = Watermarks.MIN_WATERMARK;

  @Nullable private volatile Throwable error = null;

  AbstractSourceLogObserver(
      SourceFunction.SourceContext<OutputT> sourceContext,
      ResultExtractor<OutputT> resultExtractor,
      Set<Partition> skipFirstElementFromEachPartition) {
    this.sourceContext = sourceContext;
    this.resultExtractor = resultExtractor;
    this.skipFirstElementFromEachPartition = skipFirstElementFromEachPartition;
  }

  /**
   * Checkpointing in Flink is asynchronous. This method enables to atomically mark current offset
   * as read, right after emitting element, when we still hold the {@link
   * SourceFunction.SourceContext#getCheckpointLock() checkpoint lock}.
   *
   * @param context Context that we can use for committing offsets.
   */
  abstract void markOffsetAsConsumed(ContextT context);

  @Override
  public boolean onError(Throwable error) {
    this.error = error;
    completed.countDown();
    return false;
  }

  @Override
  public void onCompleted() {
    completed.countDown();
  }

  @Override
  public void onCancelled() {
    completed.countDown();
  }

  @Override
  public boolean onNext(StreamElement element, ContextT context) {
    final boolean skipElement =
        skipFirstElementFromEachPartition.contains(context.getPartition())
            && seenPartitions.add(context.getPartition());
    if (!skipElement) {
      synchronized (sourceContext.getCheckpointLock()) {
        sourceContext.collectWithTimestamp(resultExtractor.toResult(element), element.getStamp());
        markOffsetAsConsumed(context);
      }
    }
    maybeUpdateWatermark(watermark);
    return true;
  }

  public void awaitCompleted() throws InterruptedException {
    completed.await();
  }

  public Optional<Throwable> getError() {
    return Optional.ofNullable(error);
  }

  /**
   * Update watermark if it's higher than a current watermark.
   *
   * @param watermark Candidate for a new watermark.
   */
  void maybeUpdateWatermark(long watermark) {
    if (watermark > this.watermark) {
      this.watermark = watermark;
      synchronized (sourceContext.getCheckpointLock()) {
        sourceContext.emitWatermark(new Watermark(this.watermark));
      }
    }
  }
}
