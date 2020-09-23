/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
import cz.o2.proxima.util.ExceptionUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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
      BatchLogReader delegate, ThroughputLimiter limiter) {
    return new ThroughputLimitedBatchLogReader(delegate, limiter);
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
    public void observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {

      super.observe(partitions, attributes, throughputLimited(observer, partitions));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("limiter", limiter)
          .add("delegate", super.toString())
          .toString();
    }

    private BatchLogObserver throughputLimited(
        BatchLogObserver delegate, List<Partition> consumedPartitions) {

      return new ThroughputLimitedBatchLogObserver(
          delegate, getPartitions().size(), consumedPartitions, limiter);
    }
  }

  private static class ForwardingBatchLogObserver implements BatchLogObserver {

    @Delegate private final BatchLogObserver delegate;

    private ForwardingBatchLogObserver(BatchLogObserver delegate) {
      this.delegate = delegate;
    }
  }

  private static class ThroughputLimitedBatchLogObserver extends ForwardingBatchLogObserver {

    private final int numPartitions;
    private final Collection<Partition> assignedPartitions;
    private final ThroughputLimiter limiter;
    private long watermark = Long.MIN_VALUE;

    public ThroughputLimitedBatchLogObserver(
        BatchLogObserver delegate,
        int numPartitions,
        Collection<Partition> assignedPartitions,
        ThroughputLimiter limiter) {

      super(delegate);
      this.numPartitions = numPartitions;
      this.assignedPartitions = new ArrayList<>(assignedPartitions);
      this.limiter = Objects.requireNonNull(limiter);
    }

    @Override
    public void onCompleted() {
      limiter.close();
      super.onCompleted();
    }

    @Override
    public boolean onError(Throwable error) {
      limiter.close();
      return super.onError(error);
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
      Duration pause = limiter.getPauseTime(getLimiterContext());
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
        public int getNumPartitions() {
          return numPartitions;
        }

        @Override
        public long getMinWatermark() {
          return watermark;
        }
      };
    }
  }

  private BatchLogReaders() {}
}
