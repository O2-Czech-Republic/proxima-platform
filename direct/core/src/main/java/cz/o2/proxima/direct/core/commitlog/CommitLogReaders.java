/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.commitlog;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.ThroughputLimiter;
import cz.o2.proxima.core.storage.ThroughputLimiter.Context;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.SerializableUtils;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Suppliers;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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

  private static class ForwardingObserveHandle implements ObserveHandle {

    @Delegate private final ObserveHandle delegate;

    private ForwardingObserveHandle(ObserveHandle delegate) {
      this.delegate = delegate;
    }
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
    public ObserveHandle observe(String name, Position position, CommitLogObserver observer) {
      return delegate.observe(name, position, observer);
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        CommitLogObserver observer) {

      return delegate.observePartitions(name, partitions, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, CommitLogObserver observer) {

      return delegate.observeBulk(name, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        CommitLogObserver observer) {

      return delegate.observeBulkPartitions(name, partitions, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulkOffsets(
        Collection<Offset> offsets, boolean stopAtCurrent, CommitLogObserver observer) {

      return delegate.observeBulkOffsets(offsets, stopAtCurrent, observer);
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
    private final Supplier<List<Partition>> availablePartitions;

    public LimitedCommitLogReader(CommitLogReader delegate, ThroughputLimiter limiter) {
      super(delegate);
      this.limiter = SerializableUtils.clone(Objects.requireNonNull(limiter));
      this.availablePartitions = Suppliers.memoize(delegate::getPartitions);
    }

    @Override
    public ObserveHandle observe(String name, Position position, CommitLogObserver observer) {

      ThroughputLimiter clonedLimiter = SerializableUtils.clone(limiter);
      return super.observe(
          name, position, throughputLimited(clonedLimiter, availablePartitions.get(), observer));
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        CommitLogObserver observer) {

      ThroughputLimiter clonedLimiter = SerializableUtils.clone(limiter);
      return withClosedLimiter(
          super.observePartitions(
              name,
              partitions,
              position,
              stopAtCurrent,
              throughputLimited(clonedLimiter, partitions, observer)),
          clonedLimiter);
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, CommitLogObserver observer) {

      ThroughputLimiter clonedLimiter = SerializableUtils.clone(limiter);
      return withClosedLimiter(
          super.observeBulk(
              name,
              position,
              stopAtCurrent,
              throughputLimited(clonedLimiter, availablePartitions.get(), observer)),
          clonedLimiter);
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        CommitLogObserver observer) {

      ThroughputLimiter clonedLimiter = SerializableUtils.clone(limiter);
      return withClosedLimiter(
          super.observeBulkPartitions(
              name,
              partitions,
              position,
              stopAtCurrent,
              throughputLimited(clonedLimiter, partitions, observer)),
          clonedLimiter);
    }

    @Override
    public ObserveHandle observeBulkOffsets(
        Collection<Offset> offsets, boolean stopAtCurrent, CommitLogObserver observer) {

      ThroughputLimiter clonedLimiter = SerializableUtils.clone(limiter);
      return withClosedLimiter(
          super.observeBulkOffsets(
              offsets,
              stopAtCurrent,
              throughputLimited(clonedLimiter, availablePartitions.get(), observer)),
          clonedLimiter);
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

    private static CommitLogObserver throughputLimited(
        ThroughputLimiter limiter,
        Collection<Partition> readerPartitions,
        CommitLogObserver delegate) {

      final List<Partition> partitions = new ArrayList<>(readerPartitions);

      return new ForwardingLogObserver(delegate) {

        long watermark = Long.MIN_VALUE;

        @Override
        public boolean onNext(StreamElement element, OnNextContext context) {
          if (ExceptionUtils.ignoringInterrupted(this::waitIfNecessary)) {
            return false;
          }
          watermark = context.getWatermark();
          return super.onNext(element, context);
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

    private static ObserveHandle withClosedLimiter(
        ObserveHandle delegate, ThroughputLimiter limiter) {
      return new ForwardingObserveHandle(delegate) {
        @Override
        public void close() {
          limiter.close();
          super.close();
        }
      };
    }
  }

  private static class ForwardingLogObserver implements CommitLogObserver {

    @Delegate private final CommitLogObserver delegate;

    public ForwardingLogObserver(CommitLogObserver delegate) {
      this.delegate = delegate;
    }

    @Override
    public String toString() {
      return "ForwardingLogObserver{delegate=" + delegate + '}';
    }
  }

  private CommitLogReaders() {}
}
