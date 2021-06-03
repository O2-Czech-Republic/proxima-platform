/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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

import cz.o2.proxima.direct.LogObserver;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.Watermarks;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Log observer that writes consumed elements to {@link SourceFunction.SourceContext}.
 *
 * @see <a href="https://github.com/O2-Czech-Republic/proxima-platform/issues/220">PROXIMA-220</a>
 *     to explain {@code java:S1948} suppression.
 * @param <OutputT> Type of extracted element.
 */
@SuppressWarnings("java:S1948")
abstract class AbstractSourceLogObserver<
        OffsetT extends Serializable, ContextT extends LogObserver.OnNextContext<OffsetT>, OutputT>
    implements LogObserver<OffsetT, ContextT> {

  private final CountDownLatch completed = new CountDownLatch(1);
  private final Set<Partition> seenPartitions = new HashSet<>();

  final SourceFunction.SourceContext<OutputT> sourceContext;
  private final ResultExtractor<OutputT> resultExtractor;

  /**
   * When restoring from checkpoint, we need to skip the first element in each partition, because it
   * has been already consumed.
   */
  private final Set<Partition> skipFirstElementFromEachPartition;

  long watermark = Watermarks.MIN_WATERMARK;

  @Nullable private volatile Throwable error = null;

  AbstractSourceLogObserver(
      SourceFunction.SourceContext<OutputT> sourceContext,
      ResultExtractor<OutputT> resultExtractor,
      Set<Partition> skipFirstElementFromEachPartition) {
    this.sourceContext = sourceContext;
    this.resultExtractor = resultExtractor;
    this.skipFirstElementFromEachPartition = skipFirstElementFromEachPartition;
  }

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
  public boolean onNext(StreamElement ingest, ContextT context) {
    final boolean skipElement =
        skipFirstElementFromEachPartition.contains(context.getPartition())
            && seenPartitions.add(context.getPartition());
    if (!skipElement) {
      synchronized (sourceContext.getCheckpointLock()) {
        sourceContext.collectWithTimestamp(resultExtractor.toResult(ingest), ingest.getStamp());
        markOffsetAsConsumed(context);
      }
      if (context.getWatermark() > watermark) {
        watermark = context.getWatermark();
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.emitWatermark(new Watermark(watermark));
        }
      }
    }
    return true;
  }

  abstract void markOffsetAsConsumed(ContextT context);

  public void awaitCompleted() throws InterruptedException {
    completed.await();
  }

  public Optional<Throwable> getError() {
    return Optional.ofNullable(error);
  }
}
