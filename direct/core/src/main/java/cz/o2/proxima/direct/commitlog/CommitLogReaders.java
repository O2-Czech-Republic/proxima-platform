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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.ThroughputLimiter;
import cz.o2.proxima.storage.ThroughputLimiter.Context;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.SerializableUtils;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.experimental.Delegate;

/** Utility class that constructs various versions of {@link CommitLogReader CommitLogReaders}. */
@Internal
public class CommitLogReaders {

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

    @Getter private final CommitLogReader delegate;

    private ForwardingCommitLogReader(CommitLogReader delegate) {
      this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public String toString() {
      return delegate.toString();
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

  @VisibleForTesting
  public static class LimitedCommitLogReader extends ForwardingCommitLogReader {

    @Getter private final ThroughputLimiter limiter;
    private final Collection<Partition> partitions;

    public LimitedCommitLogReader(CommitLogReader delegate, ThroughputLimiter limiter) {
      super(delegate);
      this.limiter = SerializableUtils.clone(Objects.requireNonNull(limiter));
      this.partitions = new ArrayList<>(delegate.getPartitions());
    }

    @Override
    public ObserveHandle observe(String name, Position position, LogObserver observer) {
      return super.observe(name, position, throughputLimited(limiter, partitions, observer));
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return super.observePartitions(
          name,
          partitions,
          position,
          stopAtCurrent,
          throughputLimited(limiter, partitions, observer));
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, LogObserver observer) {
      return super.observeBulk(
          name, position, stopAtCurrent, throughputLimited(limiter, partitions, observer));
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return super.observeBulkPartitions(
          name,
          partitions,
          position,
          stopAtCurrent,
          throughputLimited(limiter, partitions, observer));
    }

    @Override
    public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {
      return super.observeBulkOffsets(offsets, throughputLimited(limiter, partitions, observer));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("limiter", limiter)
          .add("delegate", getDelegate().toString())
          .toString();
    }

    @Override
    public Factory<?> asFactory() {
      final Factory<?> delegateFactory = super.asFactory();
      final ThroughputLimiter limiter = this.limiter;
      return repo -> CommitLogReaders.withThroughputLimit(delegateFactory.apply(repo), limiter);
    }

    private static LogObserver throughputLimited(
        ThroughputLimiter readerLimiter,
        Collection<Partition> readerPartitions,
        LogObserver delegate) {

      final ThroughputLimiter limiter = SerializableUtils.clone(readerLimiter);
      final List<Partition> partitions = new ArrayList<>(readerPartitions);

      return new ForwardingLogObserver(delegate) {

        long watermark = Long.MIN_VALUE;

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
        public boolean onNext(StreamElement ingest, OnNextContext context) {
          if (ExceptionUtils.ignoringInterrupted(this::waitIfNecessary)) {
            return false;
          }
          watermark = context.getWatermark();
          return super.onNext(ingest, context);
        }

        private void waitIfNecessary() throws InterruptedException {
          Duration pauseTime = limiter.getPauseTime(getLimiterContext());
          if (!pauseTime.equals(Duration.ZERO)) {
            TimeUnit.MILLISECONDS.sleep(pauseTime.toMillis());
          }
        }

        @Override
        public void onRepartition(OnRepartitionContext context) {
          super.onRepartition(context);
          partitions.clear();
          partitions.addAll(context.partitions());
        }

        @Override
        public void onIdle(OnIdleContext context) {
          if (limiter.getPauseTime(getLimiterContext()).isZero()) {
            super.onIdle(context);
          }
        }

        private Context getLimiterContext() {
          return new Context() {

            @Override
            public Collection<Partition> getConsumedPartitions() {
              return partitions;
            }

            @Override
            public long getMinWatermark() {
              return watermark;
            }
          };
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

  private CommitLogReaders() {}
}
