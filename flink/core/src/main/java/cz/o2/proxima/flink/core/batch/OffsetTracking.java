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
package cz.o2.proxima.flink.core.batch;

import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.direct.batch.Offset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetTracking {

  private static Offset mergeOffsets(Offset oldValue, Offset newValue) {
    if (oldValue.getElementIndex() < newValue.getElementIndex()) {
      return newValue;
    }
    throw new IllegalStateException(
        String.format(
            "Offsets are not monotonically increasing. Old value: %s. New value: %s.",
            oldValue, newValue));
  }

  public static BatchLogReader wrapReader(BatchLogReader reader) {
    return new OffsetTrackingBatchLogReader(reader);
  }

  public interface OffsetTrackingObserveHandle extends ObserveHandle {

    List<Offset> getCurrentOffsets();
  }

  public static class OffsetTrackingBatchLogReader implements BatchLogReader {

    private final BatchLogReader delegate;

    public OffsetTrackingBatchLogReader(BatchLogReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public List<Partition> getPartitions(long startStamp, long endStamp) {
      return delegate.getPartitions(startStamp, endStamp);
    }

    @Override
    public ObserveHandle observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {
      final OffsetTrackingBatchLogObserver wrappedObserver =
          new OffsetTrackingBatchLogObserver(observer);
      final ObserveHandle handle = delegate.observe(partitions, attributes, wrappedObserver);
      return new OffsetTrackingObserveHandle() {

        @Override
        public List<Offset> getCurrentOffsets() {
          final Map<Partition, Offset> result = new HashMap<>();
          partitions.forEach(p -> result.put(p, Offset.of(p, -1, false)));
          wrappedObserver
              .getConsumedOffsets()
              .forEach((p, o) -> result.merge(p, o, OffsetTracking::mergeOffsets));
          return new ArrayList<>(result.values());
        }

        @Override
        public void close() {
          handle.close();
        }
      };
    }

    @Override
    public Factory<?> asFactory() {
      final Factory<?> delegateFactory = delegate.asFactory();
      return repository -> new OffsetTrackingBatchLogReader(delegateFactory.apply(repository));
    }
  }

  public abstract static class OffsetTrackingOnNextContext
      implements BatchLogObserver.OnNextContext {

    private final BatchLogObserver.OnNextContext delegate;

    public OffsetTrackingOnNextContext(BatchLogObserver.OnNextContext delegate) {
      this.delegate = delegate;
    }

    @Override
    public Partition getPartition() {
      return delegate.getPartition();
    }

    @Override
    public Offset getOffset() {
      return delegate.getOffset();
    }

    @Override
    public long getWatermark() {
      return delegate.getWatermark();
    }

    public abstract void commit();
  }

  public static class OffsetTrackingBatchLogObserver implements BatchLogObserver {

    private final Map<Partition, Offset> consumedOffsets = new HashMap<>();
    private final BatchLogObserver delegate;

    public OffsetTrackingBatchLogObserver(BatchLogObserver delegate) {
      this.delegate = delegate;
    }

    public Map<Partition, Offset> getConsumedOffsets() {
      synchronized (consumedOffsets) {
        return new HashMap<>(consumedOffsets);
      }
    }

    @Override
    public boolean onNext(StreamElement element, OnNextContext context) {
      return delegate.onNext(
          element,
          new OffsetTrackingOnNextContext(context) {

            @Override
            public void commit() {
              synchronized (consumedOffsets) {
                consumedOffsets.merge(
                    context.getPartition(), context.getOffset(), OffsetTracking::mergeOffsets);
              }
            }
          });
    }

    @Override
    public void onCompleted() {
      delegate.onCompleted();
    }

    @Override
    public void onCancelled() {
      delegate.onCancelled();
    }

    @Override
    public boolean onError(Throwable error) {
      return delegate.onError(error);
    }
  }
}
