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
package cz.o2.proxima.direct.storage;

import static cz.o2.proxima.direct.commitlog.ObserverUtils.asRepartitionContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogObserver.OnNextContext;
import cz.o2.proxima.direct.batch.BatchLogObservers;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.TerminationContext;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.ObserverUtils;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.direct.randomaccess.RawOffset;
import cz.o2.proxima.direct.time.BoundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.direct.time.MinimalPartitionWatermarkEstimator;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.direct.view.LocalCachedPartitionedView;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.KeyAttributePartitioner;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.PartitionedWatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** InMemStorage for testing purposes. */
@Slf4j
public class InMemStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @VisibleForTesting static final String NUM_PARTITIONS = "num-partitions";

  private static final Partition SINGLE_PARTITION = () -> 0;
  private static final OnNextContext CONTEXT = BatchLogObservers.defaultContext(SINGLE_PARTITION);
  private static final long IDLE_FLUSH_TIME = 500L;
  private static final long BOUNDED_OUT_OF_ORDERNESS = 5000L;

  public static class ConsumedOffset implements Offset {

    private static final long serialVersionUID = 1L;

    static ConsumedOffset empty(Partition partition) {
      return new ConsumedOffset(partition, Collections.emptySet(), Long.MIN_VALUE);
    }

    @Getter final Partition partition;
    @Getter final Set<String> consumedKeyAttr;
    @Getter final long watermark;

    ConsumedOffset(Partition partition, final Set<String> consumedKeyAttr, long watermark) {
      this.partition = partition;
      synchronized (consumedKeyAttr) {
        this.consumedKeyAttr = new HashSet<>(consumedKeyAttr);
      }
      this.watermark = watermark;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("offset", consumedKeyAttr.size())
          .add("watermark", watermark)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ConsumedOffset) {
        ConsumedOffset other = (ConsumedOffset) obj;
        return other.consumedKeyAttr.equals(this.consumedKeyAttr)
            && other.watermark == this.watermark;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(watermark, consumedKeyAttr);
    }
  }

  @FunctionalInterface
  public interface ElementConsumer {

    void accept(Partition partition, StreamElement element, OffsetCommitter offsetCommitter);
  }

  @FunctionalInterface
  private interface InMemIngestWriter extends Serializable {

    void write(Partition partition, StreamElement data);
  }

  public static long getBoundedOutOfOrderness() {
    return BOUNDED_OUT_OF_ORDERNESS;
  }

  public static long getIdleFlushTime() {
    return IDLE_FLUSH_TIME;
  }

  public final class Writer extends AbstractOnlineAttributeWriter {

    private final int numPartitions;
    private final Partitioner partitioner;

    private Writer(
        EntityDescriptor entityDesc, URI uri, int numPartitions, Partitioner partitioner) {
      super(entityDesc, uri);
      this.numPartitions = numPartitions;
      this.partitioner = partitioner;
    }

    @Override
    public void write(StreamElement data, CommitCallback statusCallback) {
      NavigableMap<Integer, InMemIngestWriter> writeObservers = getObservers(getUri());
      final ArrayList<InMemIngestWriter> currentWriters;
      try (Locker ignore = holder().lockWrite()) {
        if (log.isDebugEnabled()) {
          log.debug(
              "Writing element {} to {} with {} observers", data, getUri(), writeObservers.size());
        }
        String attr =
            data.isDeleteWildcard()
                ? data.getAttributeDescriptor().toAttributePrefix()
                : data.getAttribute();
        getData()
            .compute(
                toMapKey(getUri(), data.getKey(), attr),
                (key, old) -> {
                  if (old != null && old.getFirst() > data.getStamp()) {
                    return old;
                  }
                  return Pair.of(data.getStamp(), data.getValue());
                });
        currentWriters = Lists.newArrayList(writeObservers.values());
      }
      currentWriters.forEach(
          o -> {
            final int partitionId = partitioner.getTruncatedPartitionId(data, numPartitions);
            o.write(Partition.of(partitionId), data);
            log.debug("Passed element {} to {}-{}", data, o, partitionId);
          });
      statusCallback.commit(true, null);
    }

    @Override
    public OnlineAttributeWriter.Factory<?> asFactory() {
      final EntityDescriptor entity = getEntityDescriptor();
      final URI uri = getUri();
      final int numPartitions = this.numPartitions;
      final Partitioner partitioner = this.partitioner;
      return repo -> new Writer(entity, uri, numPartitions, partitioner);
    }

    @Override
    public void close() {
      // nop
    }
  }

  private class InMemCommitLogReader extends AbstractStorage implements CommitLogReader {

    private final cz.o2.proxima.functional.Factory<ExecutorService> executorFactory;
    private final Partitioner partitioner;
    private final int numPartitions;
    private transient ExecutorService executor;

    private InMemCommitLogReader(
        EntityDescriptor entityDesc,
        URI uri,
        cz.o2.proxima.functional.Factory<ExecutorService> executorFactory,
        Partitioner partitioner,
        int numPartitions) {
      super(entityDesc, uri);
      this.executorFactory = executorFactory;
      this.partitioner = partitioner;
      this.numPartitions = numPartitions;
    }

    private ExecutorService executor() {
      if (executor == null) {
        executor = executorFactory.apply();
      }
      return executor;
    }

    @Override
    public List<Partition> getPartitions() {
      final List<Partition> partitions = new ArrayList<>();
      for (int idx = 0; idx < numPartitions; idx++) {
        partitions.add(Partition.of(idx));
      }
      return Collections.unmodifiableList(partitions);
    }

    @Override
    public ObserveHandle observe(String name, Position position, LogObserver observer) {
      return observe(name, position, false, observer);
    }

    private ObserveHandle observe(
        String name, Position position, boolean stopAtCurrent, LogObserver observer) {
      return observe(
          name,
          position,
          getPartitions().stream().map(ConsumedOffset::empty).collect(Collectors.toList()),
          stopAtCurrent,
          observer);
    }

    private ObserveHandle observe(
        String name,
        Position position,
        List<ConsumedOffset> offsets,
        boolean stopAtCurrent,
        LogObserver observer) {
      return doObserve(position, offsets, stopAtCurrent, observer, name);
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return observe(null, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, LogObserver observer) {
      return doObserve(
          position,
          getPartitions().stream().map(ConsumedOffset::empty).collect(Collectors.toList()),
          stopAtCurrent,
          observer,
          name);
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {
      return doObserve(
          position,
          partitions.stream().map(ConsumedOffset::empty).collect(Collectors.toList()),
          stopAtCurrent,
          observer,
          name);
    }

    @Override
    public ObserveHandle observeBulkOffsets(
        Collection<Offset> offsets, boolean stopAtCurrent, LogObserver observer) {
      @SuppressWarnings("unchecked")
      final List<ConsumedOffset> cast = (List) offsets;
      return doObserve(Position.OLDEST, cast, stopAtCurrent, observer, null);
    }

    private ObserveHandle doObserve(
        Position position,
        List<ConsumedOffset> offsets,
        boolean stopAtCurrent,
        LogObserver observer,
        @Nullable String name) {
      log.debug(
          "Observing {} as {} from offset {} with position {} and stopAtCurrent {} using observer {}",
          getUri(),
          name,
          offsets,
          position,
          stopAtCurrent,
          observer);
      final int id = createConsumerId(stopAtCurrent);
      observer.onRepartition(asRepartitionContext(getPartitions()));
      AtomicReference<Future<?>> observeFuture = new AtomicReference<>();
      AtomicBoolean killSwitch = new AtomicBoolean();
      Supplier<List<Offset>> offsetSupplier =
          flushBasedOnPosition(
              name, position, offsets, id, stopAtCurrent, killSwitch, observeFuture, observer);

      return createHandle(id, observer, offsetSupplier, killSwitch, observeFuture);
    }

    private int createConsumerId(boolean stopAtCurrent) {
      if (!stopAtCurrent) {
        try (Locker ignore = holder().lockRead()) {
          final NavigableMap<Integer, InMemIngestWriter> uriObservers = getObservers(getUri());
          final int id = uriObservers.isEmpty() ? 0 : uriObservers.lastKey() + 1;
          // insert placeholder
          uriObservers.put(
              id,
              (partition, data) -> {
                // Noop.
              });
          return id;
        }
      }
      return -1;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass()).add("uri", getUri()).toString();
    }

    private ObserveHandle createHandle(
        int consumerId,
        LogObserver observer,
        Supplier<List<Offset>> offsetTracker,
        AtomicBoolean killSwitch,
        AtomicReference<Future<?>> observeFuture) {

      return new ObserveHandle() {

        @Override
        public void close() {
          getObservers(getUri()).remove(consumerId);
          killSwitch.set(true);
          observeFuture.get().cancel(true);
          observer.onCancelled();
        }

        @Override
        public List<Offset> getCommittedOffsets() {
          // no commits supported for now
          return getCurrentOffsets()
              .stream()
              .map(item -> ConsumedOffset.empty(item.getPartition()))
              .collect(Collectors.toList());
        }

        @Override
        public void resetOffsets(List<Offset> offsets) {
          // nop
        }

        @Override
        public List<Offset> getCurrentOffsets() {
          return offsetTracker.get();
        }

        @Override
        public void waitUntilReady() {
          // nop
        }
      };
    }

    private Supplier<List<Offset>> flushBasedOnPosition(
        @Nullable String name,
        Position position,
        List<ConsumedOffset> initialOffsets,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        AtomicReference<Future<?>> observeFuture,
        LogObserver observer) {

      final Set<String> consumedOffsets = Collections.synchronizedSet(new HashSet<>());
      initialOffsets.forEach(item -> consumedOffsets.addAll(item.getConsumedKeyAttr()));

      final Map<Integer, WatermarkEstimator> watermarkEstimators = new HashMap<>();
      initialOffsets.forEach(
          item ->
              watermarkEstimators.put(
                  item.getPartition().getId(),
                  DataHolders.getWatermarkEstimator(
                      getUri(),
                      item.getWatermark(),
                      MoreObjects.firstNonNull(
                          name, "InMemConsumer@" + getUri() + ":" + consumerId),
                      item)));
      final PartitionedWatermarkEstimator partitionedWatermarkEstimator =
          new MinimalPartitionWatermarkEstimator(watermarkEstimators);
      CountDownLatch latch = new CountDownLatch(1);
      observeFuture.set(
          executor()
              .submit(
                  () ->
                      handleFlushDataBaseOnPosition(
                          position,
                          initialOffsets,
                          consumerId,
                          stopAtCurrent,
                          killSwitch,
                          consumedOffsets,
                          partitionedWatermarkEstimator,
                          latch,
                          observer)));
      try {
        latch.await();
      } catch (InterruptedException ex) {
        log.warn("Interrupted.", ex);
      }
      return () -> {
        final Map<Integer, Set<String>> partitionedOffsets = new HashMap<>();
        initialOffsets.forEach(
            offset -> partitionedOffsets.put(offset.getPartition().getId(), new HashSet<>()));
        consumedOffsets.forEach(
            consumedOffset -> {
              // Consumed offset always starts with partitionNumber separated by single dash.
              final int firstDash = consumedOffset.indexOf("-");
              final int partitionId = Integer.parseInt(consumedOffset.substring(0, firstDash));
              partitionedOffsets.get(partitionId).add(consumedOffset);
            });
        final long watermark = partitionedWatermarkEstimator.getWatermark();
        return partitionedOffsets
            .entrySet()
            .stream()
            .map(
                item -> new ConsumedOffset(Partition.of(item.getKey()), item.getValue(), watermark))
            .collect(Collectors.toList());
      };
    }

    private void handleFlushDataBaseOnPosition(
        Position position,
        List<ConsumedOffset> initialOffsets,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        Set<String> consumedOffsets,
        PartitionedWatermarkEstimator watermarkEstimator,
        CountDownLatch latch,
        LogObserver observer) {

      AtomicReference<ScheduledFuture<?>> onIdleRef = new AtomicReference<>();

      Runnable onIdle =
          () -> {
            try {
              synchronized (observer) {
                initialOffsets.forEach(
                    item -> watermarkEstimator.idle(item.getPartition().getId()));
                observer.onIdle(watermarkEstimator::getWatermark);
                if (watermarkEstimator.getWatermark()
                    >= (Watermarks.MAX_WATERMARK - BOUNDED_OUT_OF_ORDERNESS)) {
                  observer.onCompleted();
                  getObservers(getUri()).remove(consumerId);
                  onIdleRef.get().cancel(true);
                  killSwitch.set(true);
                }
              }
            } catch (Exception ex) {
              log.warn("Exception in onIdle", ex);
            }
          };
      ScheduledFuture<?> onIdleFuture =
          scheduler.scheduleAtFixedRate(
              onIdle, IDLE_FLUSH_TIME, IDLE_FLUSH_TIME, TimeUnit.MILLISECONDS);
      onIdleRef.set(onIdleFuture);
      AtomicReference<StreamElement> lastConsumed = new AtomicReference<>();
      final ElementConsumer consumer =
          (partition, element, committer) -> {
            try {
              if (!killSwitch.get()) {
                synchronized (observer) {
                  element = cloneAndUpdateAttribute(getEntityDescriptor(), element);
                  watermarkEstimator.update(partition.getId(), element);
                  Optional.ofNullable(lastConsumed.get())
                      .ifPresent(
                          last ->
                              consumedOffsets.add(
                                  String.format(
                                      "%d-%s#%s:%d",
                                      partition.getId(),
                                      last.getKey(),
                                      last.getAttribute(),
                                      last.getStamp())));
                  lastConsumed.set(element);
                  final boolean continueProcessing =
                      observer.onNext(
                          element,
                          asOnNextContext(
                              committer,
                              new ConsumedOffset(
                                  partition, consumedOffsets, watermarkEstimator.getWatermark())));
                  killSwitch.compareAndSet(false, !continueProcessing);
                }
              }
            } catch (Exception ex) {
              synchronized (observer) {
                killSwitch.compareAndSet(false, !observer.onError(ex));
              }
            }
          };

      NavigableMap<Integer, InMemIngestWriter> uriObservers = getObservers(getUri());
      if (position == Position.OLDEST) {
        try (Locker ignore = holder().lockRead()) {
          latch.countDown();
          String prefix = toStoragePrefix(getUri());
          int prefixLength = prefix.length();
          getData()
              .entrySet()
              .stream()
              .filter(e -> e.getKey().startsWith(prefix))
              .sorted(Comparator.comparingLong(a -> a.getValue().getFirst()))
              .forEachOrdered(
                  e -> {
                    final String keyAttr = e.getKey().substring(prefixLength);
                    final long stamp = e.getValue().getFirst();
                    final String[] parts = keyAttr.split("#");
                    final String key = parts[0];
                    final String attribute = parts[1];
                    final AttributeDescriptor<?> desc =
                        getEntityDescriptor().getAttribute(attribute, true);
                    final StreamElement element =
                        StreamElement.upsert(
                            getEntityDescriptor(),
                            desc,
                            UUID.randomUUID().toString(),
                            key,
                            attribute,
                            stamp,
                            e.getValue().getSecond());
                    final int partitionId =
                        partitioner.getTruncatedPartitionId(element, numPartitions);
                    if (consumedOffsets.contains(partitionId + "-" + keyAttr + ":" + stamp)) {
                      // this record has already been consumed in previous offset, so skip it
                      log.debug("Discarding element {} due to being already consumed.", keyAttr);
                      return;
                    }
                    consumer.accept(
                        Partition.of(partitioner.getTruncatedPartitionId(element, numPartitions)),
                        element,
                        (succ, exc) -> {
                          if (!succ && exc != null) {
                            throw new IllegalStateException("Error in observing old data", exc);
                          }
                        });
                  });
          if (!killSwitch.get()) {
            if (!stopAtCurrent) {
              uriObservers.put(
                  consumerId,
                  (partition, data) -> {
                    consumer.accept(
                        partition,
                        data,
                        (success, error) -> {
                          // Noop.
                        });
                  });
            } else {
              observer.onCompleted();
              onIdleFuture.cancel(true);
            }
          }
        }
      } else {
        if (!stopAtCurrent) {
          uriObservers.put(
              consumerId,
              (partition, data) -> {
                consumer.accept(
                    partition,
                    data,
                    (success, error) -> {
                      // Noop.
                    });
              });
        } else {
          observer.onCompleted();
          onIdleFuture.cancel(true);
        }
        latch.countDown();
      }
    }

    @Override
    public boolean hasExternalizableOffsets() {
      return true;
    }

    @Override
    public Factory<?> asFactory() {
      final EntityDescriptor entity = getEntityDescriptor();
      final URI uri = getUri();
      final cz.o2.proxima.functional.Factory<ExecutorService> executorFactory =
          this.executorFactory;
      final Partitioner partitioner = this.partitioner;
      final int numPartitions = this.numPartitions;
      return repo ->
          new InMemCommitLogReader(entity, uri, executorFactory, partitioner, numPartitions);
    }
  }

  interface ReaderFactory
      extends RandomAccessReader.Factory<Reader>, BatchLogReader.Factory<Reader> {}

  private final class Reader extends AbstractStorage implements RandomAccessReader, BatchLogReader {

    private final cz.o2.proxima.functional.Factory<ExecutorService> executorFactory;
    private transient ExecutorService executor;

    private Reader(
        EntityDescriptor entityDesc,
        URI uri,
        cz.o2.proxima.functional.Factory<ExecutorService> executorFactory) {
      super(entityDesc, uri);
      this.executorFactory = executorFactory;
    }

    @Override
    public <T> Optional<KeyValue<T>> get(
        String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

      try (Locker l = holder().lockRead()) {
        Optional<Pair<Long, byte[]>> wildcard =
            desc.isWildcard() && !attribute.equals(desc.toAttributePrefix())
                ? getMapKey(key, desc.toAttributePrefix())
                : Optional.empty();
        return getMapKey(key, attribute)
            .filter(p -> p.getSecond() != null)
            .filter(p -> !wildcard.isPresent() || wildcard.get().getFirst() < p.getFirst())
            .map(
                b -> {
                  try {
                    return KeyValue.of(
                        getEntityDescriptor(),
                        desc,
                        key,
                        attribute,
                        new RawOffset(attribute),
                        desc.getValueSerializer().deserialize(b.getSecond()).get(),
                        b.getSecond(),
                        b.getFirst());
                  } catch (Exception ex) {
                    throw new RuntimeException(ex);
                  }
                });
      }
    }

    private Optional<Pair<Long, byte[]>> getMapKey(String key, String attribute) {
      return Optional.ofNullable(getData().get(toMapKey(key, attribute)));
    }

    private String toMapKey(String key, String attribute) {
      return InMemStorage.toMapKey(getUri(), key, attribute);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset, long stamp, int limit, Consumer<KeyValue<?>> consumer) {

      scanWildcardPrefix(key, "", offset, stamp, limit, (Consumer) consumer);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> void scanWildcard(
        String key,
        AttributeDescriptor<T> wildcard,
        @Nullable RandomOffset offset,
        long stamp,
        int limit,
        Consumer<KeyValue<T>> consumer) {

      String prefix = wildcard.toAttributePrefix();
      scanWildcardPrefix(key, prefix, offset, stamp, limit, (Consumer) consumer);
    }

    private void scanWildcardPrefix(
        String key,
        String prefix,
        @Nullable RandomOffset offset,
        long stamp,
        int limit,
        Consumer<KeyValue<Object>> consumer) {

      String off = offset == null ? "" : ((RawOffset) offset).getOffset();
      String start = toMapKey(key, prefix);
      int count = 0;
      try (Locker l = holder().lockRead()) {
        SortedMap<String, Pair<Long, byte[]>> dataMap = getData().tailMap(start);
        for (Map.Entry<String, Pair<Long, byte[]>> e : dataMap.entrySet()) {
          log.trace("Scanning entry {} looking for prefix {}", e, start);
          if (e.getValue().getFirst() <= stamp) {
            if (e.getKey().startsWith(start)) {
              int hash = e.getKey().lastIndexOf("#");
              String attribute = e.getKey().substring(hash + 1);
              if (attribute.equals(off) || e.getValue().getSecond() == null) {
                continue;
              }
              Optional<AttributeDescriptor<Object>> attr;
              attr = getEntityDescriptor().findAttribute(attribute, true);
              if (attr.isPresent()) {
                Optional<Pair<Long, byte[]>> wildcard =
                    attr.get().isWildcard()
                        ? getMapKey(key, attr.get().toAttributePrefix())
                        : Optional.empty();
                if (!wildcard.isPresent() || wildcard.get().getFirst() < e.getValue().getFirst()) {

                  consumer.accept(
                      KeyValue.of(
                          getEntityDescriptor(),
                          attr.get(),
                          key,
                          attribute,
                          new RawOffset(attribute),
                          attr.get()
                              .getValueSerializer()
                              .deserialize(e.getValue().getSecond())
                              .get(),
                          e.getValue().getSecond()));

                  if (++count == limit) {
                    break;
                  }
                }
              } else {
                log.warn("Unknown attribute {} in entity {}", attribute, getEntityDescriptor());
              }
            } else {
              break;
            }
          }
        }
      }
    }

    @Override
    public void listEntities(
        RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

      String off = offset == null ? "" : ((RawOffset) offset).getOffset();
      try (Locker l = holder().lockRead()) {
        for (String k : getData().tailMap(off).keySet()) {
          if (k.compareTo(off) > 0) {
            if (limit-- != 0) {
              String substr = k.substring(k.lastIndexOf('/') + 1, k.indexOf('#'));
              consumer.accept(Pair.of(new RawOffset(substr), substr));
              off = substr;
            } else {
              break;
            }
          }
        }
      }
    }

    @Override
    public ReaderFactory asFactory() {
      final EntityDescriptor entity = getEntityDescriptor();
      final URI uri = getUri();
      final cz.o2.proxima.functional.Factory<ExecutorService> executorFactory =
          this.executorFactory;
      return repo -> new Reader(entity, uri, executorFactory);
    }

    @Override
    public void close() {}

    @Override
    public RandomOffset fetchOffset(Listing type, String key) {
      return new RawOffset(key);
    }

    @Override
    public List<Partition> getPartitions(long startStamp, long endStamp) {
      return Collections.singletonList(SINGLE_PARTITION);
    }

    @Override
    public cz.o2.proxima.direct.batch.ObserveHandle observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {

      TerminationContext terminationContext = new TerminationContext(observer);
      observeInternal(partitions, attributes, observer, terminationContext);
      return terminationContext.asObserveHandle();
    }

    private void observeInternal(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer,
        TerminationContext terminationContext) {

      log.debug(
          "Batch observing {} partitions {} for attributes {}", getUri(), partitions, attributes);
      Preconditions.checkArgument(
          partitions.size() == 1, "This reader works on single partition only, got " + partitions);
      String prefix = toStoragePrefix(getUri());
      executor()
          .submit(
              () -> {
                try {
                  try (Locker l = holder().lockRead()) {
                    final Map<String, Pair<Long, byte[]>> data = getData().tailMap(prefix);
                    for (Entry<String, Pair<Long, byte[]>> e : data.entrySet()) {
                      if (!observeElement(attributes, observer, terminationContext, prefix, e)) {
                        break;
                      }
                    }
                  }
                  terminationContext.finished();
                } catch (Throwable err) {
                  terminationContext.handleErrorCaught(
                      err,
                      () -> observeInternal(partitions, attributes, observer, terminationContext));
                }
              });
    }

    private boolean observeElement(
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer,
        TerminationContext terminationContext,
        String prefix,
        Map.Entry<String, Pair<Long, byte[]>> e) {

      if (terminationContext.isCancelled()) {
        return false;
      }
      if (!e.getKey().startsWith(prefix)) {
        return false;
      }
      String k = e.getKey();
      Pair<Long, byte[]> v = e.getValue();
      String[] parts = k.substring(prefix.length()).split("#");
      String key = parts[0];
      String attribute = parts[1];
      return getEntityDescriptor()
          .findAttribute(attribute, true)
          .filter(attributes::contains)
          .map(
              desc ->
                  observer.onNext(
                      StreamElement.upsert(
                          getEntityDescriptor(),
                          desc,
                          UUID.randomUUID().toString(),
                          key,
                          attribute,
                          v.getFirst(),
                          v.getSecond()),
                      CONTEXT))
          .orElse(true);
    }

    private ExecutorService executor() {
      if (executor == null) {
        executor = executorFactory.apply();
      }
      return executor;
    }
  }

  @FunctionalInterface
  public interface WatermarkEstimatorFactory extends Serializable {
    WatermarkEstimator apply(long stamp, String consumer, ConsumedOffset offset);
  }

  private static class DataHolders {
    static final Map<String, DataHolder> HOLDERS_MAP = new ConcurrentHashMap<>();
    static final Map<URI, WatermarkEstimatorFactory> WATERMARK_ESTIMATOR_FACTORIES =
        new ConcurrentHashMap<>();

    static void newStorage(InMemStorage storage) {
      HOLDERS_MAP.put(storage.getId(), new DataHolder());
    }

    static DataHolder get(InMemStorage storage) {
      return Objects.requireNonNull(HOLDERS_MAP.get(storage.getId()));
    }

    static WatermarkEstimator getWatermarkEstimator(
        URI uri, long initializationWatermark, String consumerName, ConsumedOffset offset) {

      return Optional.ofNullable(WATERMARK_ESTIMATOR_FACTORIES.get(uri))
          .map(f -> f.apply(initializationWatermark, consumerName, offset))
          .orElseGet(
              () ->
                  BoundedOutOfOrdernessWatermarkEstimator.newBuilder()
                      .withMinWatermark(initializationWatermark)
                      .withMaxOutOfOrderness(BOUNDED_OUT_OF_ORDERNESS)
                      .withWatermarkIdlePolicy(new IdlePolicy(getIdleFlushTime()))
                      .build());
    }

    static void addWatermarkEstimatorFactory(URI uri, WatermarkEstimatorFactory factory) {
      WATERMARK_ESTIMATOR_FACTORIES.put(uri, factory);
    }
  }

  private interface Locker extends AutoCloseable {
    void close();
  }

  private static class DataHolder {
    final NavigableMap<String, Pair<Long, byte[]>> data;
    final Map<URI, NavigableMap<Integer, InMemIngestWriter>> observers;
    final ReadWriteLock lock = new ReentrantReadWriteLock();

    DataHolder() {
      this.data = new TreeMap<>();
      this.observers = new ConcurrentHashMap<>();
    }

    Locker lockRead() {
      return locker(lock.readLock());
    }

    Locker lockWrite() {
      return locker(lock.writeLock());
    }

    private Locker locker(Lock l) {
      l.lock();
      return l::unlock;
    }
  }

  /**
   * Specify {@link WatermarkEstimator} for given inmem storage URI.
   *
   * @param uri the inmem:// URI
   * @param factory factory to use for creation of the estimator. The input parameter of the factory
   *     is initial watermark and name of consumer
   */
  public static void setWatermarkEstimatorFactory(URI uri, WatermarkEstimatorFactory factory) {
    Preconditions.checkArgument(uri.getScheme().equals("inmem"), "Expected inmem URI got %s", uri);
    DataHolders.addWatermarkEstimatorFactory(uri, factory);
  }

  private static final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(4);

  @Getter private final String id = UUID.randomUUID().toString();

  public InMemStorage() {
    log.info("Created new empty {} with id {}", getClass().getName(), id);
    DataHolders.newStorage(this);
  }

  private DataHolder holder() {
    return DataHolders.get(this);
  }

  public NavigableMap<String, Pair<Long, byte[]>> getData() {
    return holder().data;
  }

  NavigableMap<Integer, InMemIngestWriter> getObservers(URI uri) {
    return Objects.requireNonNull(
        holder().observers.get(uri), () -> String.format("Missing observer for [%s]", uri));
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("inmem") ? Accept.ACCEPT : Accept.REJECT;
  }

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator op, EntityDescriptor entity, URI uri, Map<String, Object> cfg) {
    log.info("Creating accessor {} for URI {}", getClass(), uri);
    holder()
        .observers
        .computeIfAbsent(uri, k -> Collections.synchronizedNavigableMap(new TreeMap<>()));

    final int numPartitions =
        Optional.ofNullable(cfg.get(NUM_PARTITIONS))
            .map(v -> Integer.parseInt(v.toString()))
            .orElse(1);

    final Partitioner partitioner = new KeyAttributePartitioner();

    final Repository opRepo = op.getRepository();
    final RepositoryFactory repositoryFactory = opRepo.asFactory();
    final OnlineAttributeWriter.Factory<?> writerFactory =
        new Writer(entity, uri, numPartitions, partitioner).asFactory();
    final CommitLogReader.Factory<?> commitLogReaderFactory =
        new InMemCommitLogReader(
                entity, uri, op.getContext().getExecutorFactory(), partitioner, numPartitions)
            .asFactory();
    final ReaderFactory readerFactory =
        new Reader(entity, uri, op.getContext().getExecutorFactory()).asFactory();
    final CachedView.Factory cachedViewFactory =
        new LocalCachedPartitionedView(
                entity, commitLogReaderFactory.apply(opRepo), writerFactory.apply(opRepo))
            .asFactory();

    return new DataAccessor() {

      private static final long serialVersionUID = 1L;

      private transient @Nullable Repository repo = opRepo;

      @Override
      public URI getUri() {
        return uri;
      }

      @Override
      public Optional<AttributeWriterBase> getWriter(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(writerFactory.apply(repo()));
      }

      @Override
      public Optional<CommitLogReader> getCommitLogReader(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(commitLogReaderFactory.apply(repo()));
      }

      @Override
      public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(readerFactory.apply(repo()));
      }

      @Override
      public Optional<CachedView> getCachedView(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(cachedViewFactory.apply(repo()));
      }

      @Override
      public Optional<BatchLogReader> getBatchLogReader(Context context) {
        Objects.requireNonNull(context);
        Reader createdReader = readerFactory.apply(repo());
        return Optional.of(createdReader);
      }

      private Repository repo() {
        if (this.repo == null) {
          this.repo = repositoryFactory.apply();
        }
        return this.repo;
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <T> AttributeDescriptor<T> getAttributeOfEntity(
      EntityDescriptor entity, StreamElement ingest) {

    return (AttributeDescriptor<T>)
        entity
            .findAttribute(ingest.getAttribute(), true)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing attribute " + ingest.getAttribute() + " in " + entity));
  }

  private static StreamElement cloneAndUpdateAttribute(
      EntityDescriptor entity, StreamElement elem) {

    return StreamElement.upsert(
        entity,
        getAttributeOfEntity(entity, elem),
        elem.getUuid(),
        elem.getKey(),
        elem.getAttribute(),
        elem.getStamp(),
        elem.getValue());
  }

  private static String toStoragePrefix(URI uri) {
    return Optional.ofNullable(uri.getAuthority()).map(a -> a + "/").orElse("")
        + uri.getPath()
        + "/";
  }

  private static String toMapKey(URI uri, String key, String attribute) {
    return toStoragePrefix(uri) + key + "#" + attribute;
  }

  private static LogObserver.OnNextContext asOnNextContext(
      LogObserver.OffsetCommitter committer, Offset offset) {
    return ObserverUtils.asOnNextContext(committer, offset);
  }

  static class IdlePolicy implements WatermarkIdlePolicy {
    private final long idleFlushTime;
    private long lastWatermarkWhenIdle = Watermarks.MIN_WATERMARK;

    public IdlePolicy(long idleFlushTime) {
      this.idleFlushTime = idleFlushTime;
    }

    @Override
    public void idle(long currentWatermark) {
      lastWatermarkWhenIdle = Math.max(currentWatermark, lastWatermarkWhenIdle + idleFlushTime);
    }

    @Override
    public long getIdleWatermark() {
      return lastWatermarkWhenIdle;
    }
  }
}
