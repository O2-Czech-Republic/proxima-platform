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
package cz.o2.proxima.direct.commitlog;

import com.google.common.base.MoreObjects;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.ThroughputLimiter;
import cz.o2.proxima.storage.ThroughputLimiter.Context;
import cz.o2.proxima.storage.commitlog.Position;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.experimental.Delegate;

/**
 * Utility class that exposes throughput limiting behavior for {@link CommitLogReader}
 * implementations.
 */
@Internal
public class ThroughputLimitedCommitLogReader {

  /**
   * Create throughput limited {@link CommitLogReader}.
   *
   * @param delegate delegate to read data from
   * @param limiter the throughput limiter
   * @return throughput limited {@link CommitLogReader}
   */
  public static CommitLogReader withThroughputLimit(
      CommitLogReader delegate, @Nullable ThroughputLimiter limiter) {
    if (limiter != null) {
      return new LimitedCommitLogReader(delegate, limiter);
    }
    return delegate;
  }

  private static class ForwardingCommitLogReader implements CommitLogReader {

    private final CommitLogReader delegate;

    private ForwardingCommitLogReader(CommitLogReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public String toString() {
      return "ForwardingCommitLogReader{delegate=" + delegate + '}';
    }

    @Override
    public URI getUri() {
      return delegate.getUri();
    }

    @Override
    public List<Partition> getPartitions() {
      return delegate.getPartitions();
    }

    @Override
    public ObserveHandle observe(String name, Position position, LogObserver observer) {
      return delegate.observe(name, position, observer);
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return delegate.observePartitions(name, partitions, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, LogObserver observer) {
      return delegate.observeBulk(name, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return delegate.observeBulkPartitions(name, partitions, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {
      return delegate.observeBulkOffsets(offsets, observer);
    }

    @Override
    public Factory<?> asFactory() {
      return delegate.asFactory();
    }

    @Override
    public boolean hasExternalizableOffsets() {
      return delegate.hasExternalizableOffsets();
    }
  }

  private static class LimitedCommitLogReader extends ForwardingCommitLogReader {

    private final ThroughputLimiter limiter;
    private final Collection<Partition> partitions;
    private final int numPartitions;
    private long watermark = Long.MIN_VALUE;

    public LimitedCommitLogReader(CommitLogReader delegate, ThroughputLimiter limiter) {
      super(delegate);
      this.limiter = limiter;
      this.partitions = new ArrayList<>(delegate.getPartitions());
      this.numPartitions = this.partitions.size();
    }

    @Override
    public ObserveHandle observe(String name, Position position, LogObserver observer) {
      return super.observe(name, position, throughputLimited(observer));
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return super.observePartitions(
          name, partitions, position, stopAtCurrent, throughputLimited(observer));
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, LogObserver observer) {
      return super.observeBulk(name, position, stopAtCurrent, throughputLimited(observer));
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return super.observeBulkPartitions(
          name, partitions, position, stopAtCurrent, throughputLimited(observer));
    }

    @Override
    public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {
      return super.observeBulkOffsets(offsets, throughputLimited(observer));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("limiter", limiter)
          .add("watermark", watermark)
          .add("delegate", super.toString())
          .toString();
    }

    private LogObserver throughputLimited(LogObserver delegate) {
      return new ForwardingLogObserver(delegate) {
        @Override
        public boolean onNext(StreamElement ingest, OnNextContext context) {
          Duration pauseTime = limiter.getPauseTime(getPauseTimeContext());
          if (!pauseTime.equals(Duration.ZERO)) {
            try {
              TimeUnit.MILLISECONDS.sleep(pauseTime.toMillis());
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              return false;
            }
          }
          watermark = context.getWatermark();
          return super.onNext(ingest, context);
        }

        @Override
        public void onRepartition(OnRepartitionContext context) {
          super.onRepartition(context);
          partitions.clear();
          partitions.addAll(context.partitions());
        }

        @Override
        public void onIdle(OnIdleContext context) {
          if (limiter.getPauseTime(getPauseTimeContext()).equals(Duration.ZERO)) {
            super.onIdle(context);
          }
        }
      };
    }

    private Context getPauseTimeContext() {
      return new Context() {

        @Override
        public Collection<Partition> getConsumedPartitions() {
          return partitions;
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

  private static class ForwardingLogObserver implements LogObserver {

    @Delegate private final LogObserver delegate;

    public ForwardingLogObserver(LogObserver delegate) {
      this.delegate = delegate;
    }

    @Override
    public String toString() {
      return "ForwardingLogObserver{delegate=" + delegate + '}';
    }
  }
}
