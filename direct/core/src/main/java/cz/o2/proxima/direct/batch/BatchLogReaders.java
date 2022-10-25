/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.batch;

import com.google.common.base.MoreObjects;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.ThroughputLimiter;
import cz.o2.proxima.storage.ThroughputLimiter.Context;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.SerializableUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.experimental.Delegate;

/** Class constructing various {@link BatchLogReader BatchLogReaders}. */
public class BatchLogReaders {

  /**
   * Create throughput limited {@link BatchLogReader}.
   *
   * @param delegate delegate to read data from
   * @param limiter the throughput limiter
   * @return throughput limited {@link BatchLogReader}
   */
  public static BatchLogReader withLimitedThroughput(
      BatchLogReader delegate, @Nullable ThroughputLimiter limiter) {
    if (limiter != null) {
      return new ThroughputLimitedBatchLogReader(delegate, limiter);
    }
    return delegate;
  }

  private static class ForwardingObserveHandle implements ObserveHandle {

    @Delegate private final ObserveHandle delegate;

    ForwardingObserveHandle(ObserveHandle delegate) {
      this.delegate = delegate;
    }
  }

  public static class ForwardingBatchLogObserver implements BatchLogObserver {

    @Delegate private final BatchLogObserver delegate;

    public ForwardingBatchLogObserver(BatchLogObserver delegate) {
      this.delegate = delegate;
    }
  }

  private static class ForwardingLimitedBatchLogReader implements BatchLogReader {

    @Delegate private final BatchLogReader delegate;

    private ForwardingLimitedBatchLogReader(BatchLogReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }

  private static class ThroughputLimitedBatchLogReader extends ForwardingLimitedBatchLogReader {

    private final ThroughputLimiter limiter;

    public ThroughputLimitedBatchLogReader(BatchLogReader delegate, ThroughputLimiter limiter) {
      super(delegate);
      this.limiter = limiter;
    }

    @Override
    public ObserveHandle observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {

      long minWatermark =
          partitions
              .stream()
              .map(Partition::getMinTimestamp)
              .min(Comparator.naturalOrder())
              .orElse(Watermarks.MAX_WATERMARK);
      ThroughputLimiter clonedLimiter = SerializableUtils.clone(limiter);
      ThroughputLimitedBatchLogObserver limitedObserver =
          new ThroughputLimitedBatchLogObserver(observer, partitions, clonedLimiter);
      ObserveHandle delegate = super.observe(partitions, attributes, limitedObserver);
      Context context =
          new Context() {
            @Override
            public Collection<Partition> getConsumedPartitions() {
              return partitions;
            }

            @Override
            public long getMinWatermark() {
              return minWatermark;
            }
          };
      return new ForwardingObserveHandle(delegate) {
        @Override
        public boolean isReadyForProcessing() {
          return limitedObserver.getPauseTime(context).isZero();
        }

        @Override
        public void close() {
          clonedLimiter.close();
          disableRateLimiting();
          super.close();
        }

        @Override
        public void disableRateLimiting() {
          limitedObserver.disableRateLimiting();
        }
      };
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("limiter", limiter)
          .add("delegate", super.toString())
          .toString();
    }

    @Override
    public Factory<?> asFactory() {
      final Factory<?> superFactory = super.asFactory();
      final ThroughputLimiter limiter = this.limiter;
      return repo -> new ThroughputLimitedBatchLogReader(superFactory.apply(repo), limiter);
    }
  }

  private static class ThroughputLimitedBatchLogObserver extends ForwardingBatchLogObserver {

    private final Collection<Partition> assignedPartitions;
    private final ThroughputLimiter limiter;
    private final AtomicBoolean rateLimitingDisabled = new AtomicBoolean();
    private long watermark = Long.MIN_VALUE;

    public ThroughputLimitedBatchLogObserver(
        BatchLogObserver delegate,
        Collection<Partition> assignedPartitions,
        ThroughputLimiter limiter) {

      super(delegate);
      this.assignedPartitions = new ArrayList<>(assignedPartitions);
      this.limiter = limiter;
    }

    @Override
    public void onCompleted() {
      try {
        super.onCompleted();
      } finally {
        limiter.close();
      }
    }

    @Override
    public void onCancelled() {
      try {
        super.onCancelled();
      } finally {
        limiter.close();
      }
    }

    @Override
    public boolean onError(Throwable error) {
      try {
        return super.onError(error);
      } finally {
        limiter.close();
      }
    }

    @Override
    public boolean onNext(StreamElement element) {
      if (ExceptionUtils.ignoringInterrupted(this::waitIfNecessary)) {
        return false;
      }
      return super.onNext(element);
    }

    @Override
    public boolean onNext(StreamElement element, OnNextContext context) {
      watermark = context.getWatermark();
      if (ExceptionUtils.ignoringInterrupted(this::waitIfNecessary)) {
        return false;
      }
      return super.onNext(element, context);
    }

    private void waitIfNecessary() throws InterruptedException {
      Duration pause = getPauseTime(getLimiterContext());
      if (!pause.equals(Duration.ZERO)) {
        TimeUnit.MILLISECONDS.sleep(pause.toMillis());
      }
    }

    private Context getLimiterContext() {
      return new Context() {

        @Override
        public Collection<Partition> getConsumedPartitions() {
          return assignedPartitions;
        }

        @Override
        public long getMinWatermark() {
          return watermark;
        }
      };
    }

    public void disableRateLimiting() {
      rateLimitingDisabled.set(true);
    }

    public Duration getPauseTime(Context context) {
      return rateLimitingDisabled.get() ? Duration.ZERO : limiter.getPauseTime(context);
    }
  }

  private BatchLogReaders() {}
}
