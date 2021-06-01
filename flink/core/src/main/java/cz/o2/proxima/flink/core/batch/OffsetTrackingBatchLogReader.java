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

/**
 * A simple wrapper for {@link BatchLogReader batch log reader}, that is able to track the "highest"
 * consumed offset for each partition. For offset to be consider as consumed, you must call {@link
 * OffsetCommitter#markOffsetAsConsumed()} first.
 *
 * <p>Unfortunately we can not simply extend {@link BatchLogObserver.OnNextContext} interface,
 * therefore you need to cast it to {@link OffsetCommitter} for marking offset as consumed.
 *
 * <pre>
 * new BatchLogObserver() {
 *
 *   &#64;Override
 *   public boolean onNext(StreamElement element, OnNextContext context) {
 *     final OffsetTrackingBatchLogReader.OffsetCommitter committer =
 *         (OffsetTrackingBatchLogReader.OffsetCommitter) context;
 *     committer.markOffsetAsConsumed();
 *     return true;
 *   }
 * }
 * </pre>
 *
 * Now you can simply retrieve consumed offset by casting {@link ObserveHandle} to {@link
 * OffsetTrackingObserveHandle}.
 *
 * <pre>
 * final OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle tracker =
 *   (OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle) handle;
 * tracker.getCurrentOffsets();
 * </pre>
 */
public class OffsetTrackingBatchLogReader implements BatchLogReader {

  /**
   * Wrap batch log reader into {@link OffsetTrackingBatchLogReader}.
   *
   * @param reader Log reader to wrap.
   * @return Wrapped reader.
   */
  public static OffsetTrackingBatchLogReader of(BatchLogReader reader) {
    return new OffsetTrackingBatchLogReader(reader);
  }

  /**
   * Merge two offsets, ensuring that new offset is monotonically increasing.
   *
   * @param oldValue Old offset (lower).
   * @param newValue New offset (higher).
   * @return New offset.
   */
  private static Offset mergeOffsets(Offset oldValue, Offset newValue) {
    if (oldValue.getElementIndex() < newValue.getElementIndex()) {
      return newValue;
    }
    throw new IllegalStateException(
        String.format(
            "Offsets are not monotonically increasing. Old value: %s. New value: %s.",
            oldValue, newValue));
  }

  /** Batch observe handle, that has access to consumed offsets. */
  public interface OffsetTrackingObserveHandle extends ObserveHandle {

    /**
     * Get consumed offset. Please note, that for offset to be consider as consumed, you must call
     * {@link OffsetCommitter#markOffsetAsConsumed()} first.
     *
     * @return List of consumed offsets.
     */
    List<Offset> getCurrentOffsets();
  }

  public interface OffsetCommitter {

    void markOffsetAsConsumed();
  }

  /** Log observer, that keeps track of the "highest" consumed offset for each partition. */
  public static class OffsetTrackingBatchLogObserver implements BatchLogObserver {

    private final Map<Partition, Offset> consumedOffsets = new HashMap<>();
    private final BatchLogObserver delegate;

    public OffsetTrackingBatchLogObserver(BatchLogObserver delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean onNext(StreamElement element, OnNextContext context) {
      return delegate.onNext(
          element,
          new OffsetTrackingOnNextContext(context) {

            @Override
            public void markOffsetAsConsumed() {
              synchronized (consumedOffsets) {
                consumedOffsets.merge(
                    context.getPartition(),
                    context.getOffset(),
                    OffsetTrackingBatchLogReader::mergeOffsets);
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

    private Map<Partition, Offset> getConsumedOffsets() {
      synchronized (consumedOffsets) {
        return new HashMap<>(consumedOffsets);
      }
    }
  }

  /** {@link BatchLogObserver.OnNextContext} that is able to mark offsets as consumed. */
  private abstract static class OffsetTrackingOnNextContext
      implements BatchLogObserver.OnNextContext, OffsetCommitter {

    private final BatchLogObserver.OnNextContext delegate;

    OffsetTrackingOnNextContext(BatchLogObserver.OnNextContext delegate) {
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
  }

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
            .forEach((p, o) -> result.merge(p, o, OffsetTrackingBatchLogReader::mergeOffsets));
        return new ArrayList<>(result.values());
      }

      @Override
      public void close() {
        handle.close();
      }
    };
  }

  @Override
  public BatchLogReader.Factory<?> asFactory() {
    final BatchLogReader.Factory<?> delegateFactory = delegate.asFactory();
    return repository -> new OffsetTrackingBatchLogReader(delegateFactory.apply(repository));
  }
}
