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
package cz.o2.proxima.direct.core.storage;

import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asRepartitionContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.ConfigConstants;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.RepositoryFactory;
import cz.o2.proxima.core.scheme.SerializationException;
import cz.o2.proxima.core.storage.AbstractStorage;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.KeyAttributePartitioner;
import cz.o2.proxima.core.storage.commitlog.Partitioner;
import cz.o2.proxima.core.storage.commitlog.Partitioners;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.PartitionedWatermarkEstimator;
import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.WatermarkIdlePolicy;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext;
import cz.o2.proxima.direct.core.batch.BatchLogObservers;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.TerminationContext;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OffsetCommitter;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.ObserverUtils;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.commitlog.OffsetExternalizer;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.direct.core.randomaccess.RawOffset;
import cz.o2.proxima.direct.core.time.BoundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.direct.core.time.MinimalPartitionWatermarkEstimator;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.direct.core.view.LocalCachedPartitionedView;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** InMemStorage for testing purposes. */
@Slf4j
@AutoService(DataAccessorFactory.class)
public class InMemStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @VisibleForTesting static final String NUM_PARTITIONS = "num-partitions";

  private static final Partition SINGLE_PARTITION = Partition.of(0);
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
          .add("partition", partition)
          .add("offset", consumedKeyAttr.size())
          .add("watermark", watermark)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ConsumedOffset) {
        ConsumedOffset other = (ConsumedOffset) obj;
        return other.partition.equals(this.partition)
            && other.consumedKeyAttr.equals(this.consumedKeyAttr);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(partition, consumedKeyAttr);
    }
  }

  public static class ConsumedOffsetExternalizer implements OffsetExternalizer {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @Override
    public String toJson(Offset offset) {
      try {
        final ConsumedOffset consumedOffset = (ConsumedOffset) offset;
        final HashMap<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("partition", consumedOffset.getPartition().getId());
        jsonMap.put("offset", consumedOffset.getConsumedKeyAttr());
        jsonMap.put("watermark", consumedOffset.getWatermark());

        return JSON_MAPPER.writeValueAsString(jsonMap);
      } catch (JsonProcessingException e) {
        throw new SerializationException("Offset can't be externalized to Json", e);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ConsumedOffset fromJson(String json) {
      try {
        final HashMap<String, Object> jsonMap =
            JSON_MAPPER.readValue(json, new TypeReference<HashMap<String, Object>>() {});

        return new ConsumedOffset(
            Partition.of((int) jsonMap.get("partition")),
            new HashSet<>((List<String>) jsonMap.get("offset")),
            ((Number) jsonMap.get("watermark")).longValue());

      } catch (JsonProcessingException e) {
        throw new SerializationException("Offset can't be create from externalized Json", e);
      }
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
      int dollarSign = data.getAttributeDescriptor().toAttributePrefix().indexOf('$');
      String requiredPrefix =
          dollarSign < 0
              ? data.getAttributeDescriptor().toAttributePrefix()
              : data.getAttributeDescriptor().toAttributePrefix().substring(dollarSign + 1);
      Preconditions.checkArgument(
          data.getAttribute().startsWith(requiredPrefix)
              || data.getAttribute().startsWith(data.getAttributeDescriptor().toAttributePrefix()),
          "Non-matching attribute and attribute descriptor, got [ %s ] and [ %s ]",
          data.getAttribute(),
          data.getAttributeDescriptor());
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
                  if (old != null
                      && (old.getSequentialId() > data.getSequentialId()
                          || old.getStamp() > data.getStamp()
                              && old.getSequentialId() == data.getSequentialId())) {
                    return old;
                  }
                  return data;
                });
        currentWriters = Lists.newArrayList(writeObservers.values());
      }
      currentWriters.forEach(
          o -> {
            final int partitionId =
                Partitioners.getTruncatedPartitionId(partitioner, data, numPartitions);
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
      final InMemStorage storage = InMemStorage.this;
      return repo -> storage.new Writer(entity, uri, numPartitions, partitioner);
    }

    @Override
    public void close() {
      // nop
    }
  }

  private class InMemCommitLogReader extends AbstractStorage implements CommitLogReader {

    private final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory;
    private final Partitioner partitioner;
    private final int numPartitions;
    private transient ExecutorService executor;

    private InMemCommitLogReader(
        EntityDescriptor entityDesc,
        URI uri,
        cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory,
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
    public boolean restoresSequentialIds() {
      return true;
    }

    @Override
    public ObserveHandle observe(String name, Position position, CommitLogObserver observer) {
      return observe(name, position, false, observer);
    }

    private ObserveHandle observe(
        String name, Position position, boolean stopAtCurrent, CommitLogObserver observer) {

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
        CommitLogObserver observer) {

      return doObserve(position, offsets, stopAtCurrent, observer, name);
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        CommitLogObserver observer) {

      return observe(
          null,
          position,
          partitions.stream().map(ConsumedOffset::empty).collect(Collectors.toList()),
          stopAtCurrent,
          observer);
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, CommitLogObserver observer) {

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
        CommitLogObserver observer) {

      return doObserve(
          position,
          partitions.stream().map(ConsumedOffset::empty).collect(Collectors.toList()),
          stopAtCurrent,
          observer,
          name);
    }

    @Override
    public ObserveHandle observeBulkOffsets(
        Collection<Offset> offsets, boolean stopAtCurrent, CommitLogObserver observer) {

      @SuppressWarnings({"unchecked", "rawtypes"})
      final Collection<ConsumedOffset> cast = (Collection) offsets;
      return doObserve(Position.OLDEST, cast, stopAtCurrent, observer, null);
    }

    private ObserveHandle doObserve(
        Position position,
        Collection<ConsumedOffset> offsets,
        boolean stopAtCurrent,
        CommitLogObserver observer,
        @Nullable String name) {

      final int id = createConsumerId(stopAtCurrent);
      log.debug(
          "Observing {} as {} from offset {} with position {} and stopAtCurrent {} using observer {} as id {}",
          getUri(),
          name,
          offsets,
          position,
          stopAtCurrent,
          observer,
          id);
      observer.onRepartition(
          asRepartitionContext(
              offsets.stream().map(Offset::getPartition).collect(Collectors.toList())));
      AtomicReference<Future<?>> observeFuture = new AtomicReference<>();
      AtomicBoolean killSwitch = new AtomicBoolean();
      Supplier<List<Offset>> offsetSupplier =
          flushBasedOnPosition(
              name, position, offsets, id, stopAtCurrent, killSwitch, observeFuture, observer);

      return createHandle(id, observer, offsetSupplier, killSwitch, observeFuture);
    }

    private int createConsumerId(boolean isBatch) {
      if (!isBatch) {
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
        CommitLogObserver observer,
        Supplier<List<Offset>> offsetTracker,
        AtomicBoolean killSwitch,
        AtomicReference<Future<?>> observeFuture) {

      return new ObserveHandle() {

        @Override
        public void close() {
          log.debug("Closing consumer {}", consumerId);
          getObservers(getUri()).remove(consumerId);
          killSwitch.set(true);
          observeFuture.get().cancel(true);
          observer.onCancelled();
        }

        @Override
        public List<Offset> getCommittedOffsets() {
          return getCurrentOffsets();
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
        Collection<ConsumedOffset> initialOffsets,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        AtomicReference<Future<?>> observeFuture,
        CommitLogObserver observer) {

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
                          initialOffsets.stream()
                              .map(ConsumedOffset::getPartition)
                              .collect(Collectors.toSet()),
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
        return partitionedOffsets.entrySet().stream()
            .map(
                item -> new ConsumedOffset(Partition.of(item.getKey()), item.getValue(), watermark))
            .collect(Collectors.toList());
      };
    }

    private void handleFlushDataBaseOnPosition(
        Position position,
        Set<Partition> subscribedPartitions,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        Set<String> consumedOffsets,
        PartitionedWatermarkEstimator watermarkEstimator,
        CountDownLatch latch,
        CommitLogObserver observer) {

      try {
        doHandleFlushDataBaseOnPosition(
            position,
            subscribedPartitions,
            consumerId,
            stopAtCurrent,
            killSwitch,
            consumedOffsets,
            watermarkEstimator,
            latch,
            observer);
      } catch (Throwable err) {
        log.error("Error running observer", err);
        observer.onError(err);
        latch.countDown();
      }
    }

    private void doHandleFlushDataBaseOnPosition(
        Position position,
        Set<Partition> subscribedPartitions,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        Set<String> consumedOffsets,
        PartitionedWatermarkEstimator watermarkEstimator,
        CountDownLatch latch,
        CommitLogObserver observer) {

      AtomicReference<ScheduledFuture<?>> onIdleRef = new AtomicReference<>();

      Runnable onIdle =
          () -> {
            try {
              synchronized (observer) {
                subscribedPartitions.forEach(item -> watermarkEstimator.idle(item.getId()));
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
      final Map<Partition, StreamElement> lastConsumedPerPartition = new HashMap<>();
      final ElementConsumer consumer =
          (partition, element, committer) -> {
            try {
              if (!killSwitch.get() && subscribedPartitions.contains(partition)) {
                synchronized (observer) {
                  element = cloneAndUpdateAttribute(getEntityDescriptor(), element);
                  watermarkEstimator.update(partition.getId(), element);
                  @Nullable
                  final StreamElement lastConsumed = lastConsumedPerPartition.get(partition);
                  if (lastConsumed != null) {
                    consumedOffsets.add(toConsumedOffset(partition, lastConsumed));
                  }
                  lastConsumedPerPartition.put(partition, element);
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
          getData().entrySet().stream()
              .filter(e -> e.getKey().startsWith(prefix))
              .sorted(
                  (a, b) ->
                      a.getValue().getSequentialId() != b.getValue().getSequentialId()
                          ? Long.compare(
                              a.getValue().getSequentialId(), b.getValue().getSequentialId())
                          : Long.compare(a.getValue().getStamp(), b.getValue().getStamp()))
              .forEachOrdered(
                  e -> {
                    final String keyAttr = e.getKey().substring(prefixLength);
                    final StreamElement element = e.getValue();
                    final int partitionId =
                        Partitioners.getTruncatedPartitionId(partitioner, element, numPartitions);
                    if (consumedOffsets.contains(
                        partitionId + "-" + keyAttr + ":" + element.getStamp())) {
                      // this record has already been consumed in previous offset, so skip it
                      log.debug("Discarding element {} due to being already consumed.", keyAttr);
                      return;
                    }
                    consumer.accept(
                        Partition.of(partitionId),
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
                  (partition, data) ->
                      consumer.accept(
                          partition,
                          data,
                          (success, error) -> {
                            // Noop.
                          }));
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
              (partition, data) ->
                  consumer.accept(
                      partition,
                      data,
                      (success, error) -> {
                        // Noop.
                      }));
        } else {
          observer.onCompleted();
          onIdleFuture.cancel(true);
        }
        latch.countDown();
      }
    }

    private String toConsumedOffset(Partition partition, StreamElement element) {
      return String.format(
          "%d-%s:%d",
          partition.getId(),
          toKeyAttribute(element.getKey(), element.getAttribute()),
          element.getStamp());
    }

    @Override
    public boolean hasExternalizableOffsets() {
      return true;
    }

    @Override
    public OffsetExternalizer getOffsetExternalizer() {
      return new ConsumedOffsetExternalizer();
    }

    @Override
    public Map<Partition, Offset> fetchOffsets(Position position, List<Partition> partitions) {
      Preconditions.checkArgument(position == Position.OLDEST || position == Position.NEWEST);
      if (position == Position.OLDEST) {
        return partitions.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    p -> new ConsumedOffset(p, Collections.emptySet(), Watermarks.MIN_WATERMARK)));
      }
      String prefix = toStoragePrefix(getUri());
      int prefixLength = prefix.length();
      // we filter out the element with highest timestamp, so that observeOffsets(endOffsets) will
      // retrieve the last element
      Map<Integer, AtomicLong> maxStamps =
          getData().entrySet().stream()
              .filter(e -> e.getKey().startsWith(prefix))
              .map(Map.Entry::getValue)
              .collect(
                  Collectors.groupingBy(
                      e -> Partitioners.getTruncatedPartitionId(partitioner, e, numPartitions),
                      Collectors.mapping(
                          StreamElement::getStamp,
                          Collectors.reducing(
                              new AtomicLong(Long.MIN_VALUE),
                              AtomicLong::new,
                              (a, b) -> (a.longValue() < b.longValue()) ? b : a))));

      return getData().entrySet().stream()
          .filter(e -> e.getKey().startsWith(prefix))
          .map(
              e -> {
                StreamElement element = e.getValue();
                Partition part =
                    Partition.of(
                        Partitioners.getTruncatedPartitionId(partitioner, element, numPartitions));
                return Pair.of(part, element);
              })
          .filter(
              p ->
                  !maxStamps
                      .get(p.getFirst().getId())
                      .compareAndSet(p.getSecond().getStamp(), Long.MIN_VALUE))
          .collect(
              Collectors.groupingBy(
                  Pair::getFirst,
                  Collectors.mapping(
                      p -> toConsumedOffset(p.getFirst(), p.getSecond()), Collectors.toSet())))
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> new ConsumedOffset(e.getKey(), e.getValue(), Watermarks.MIN_WATERMARK)));
    }

    @Override
    public Factory<?> asFactory() {
      final EntityDescriptor entity = getEntityDescriptor();
      final URI uri = getUri();
      final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory =
          this.executorFactory;
      final Partitioner partitioner = this.partitioner;
      final int numPartitions = this.numPartitions;
      final InMemStorage storage = InMemStorage.this;
      return repo ->
          storage
          .new InMemCommitLogReader(entity, uri, executorFactory, partitioner, numPartitions);
    }
  }

  interface ReaderFactory
      extends RandomAccessReader.Factory<Reader>, BatchLogReader.Factory<Reader> {}

  private final class Reader extends AbstractStorage implements RandomAccessReader, BatchLogReader {

    private final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory;
    private transient ExecutorService executor;

    private Reader(
        EntityDescriptor entityDesc,
        URI uri,
        cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory) {
      super(entityDesc, uri);
      this.executorFactory = executorFactory;
    }

    @Override
    public <T> Optional<KeyValue<T>> get(
        String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

      try (Locker l = holder().lockRead()) {
        Optional<StreamElement> wildcard =
            desc.isWildcard() && !attribute.equals(desc.toAttributePrefix())
                ? getMapKey(key, desc.toAttributePrefix())
                : Optional.empty();
        return getMapKey(key, attribute)
            .filter(p -> !p.isDelete())
            .filter(p -> !wildcard.isPresent() || wildcard.get().getStamp() < p.getStamp())
            .map(
                b -> {
                  try {
                    if (b.hasSequentialId()) {
                      return KeyValue.of(
                          getEntityDescriptor(),
                          desc,
                          b.getSequentialId(),
                          key,
                          attribute,
                          new RawOffset(attribute),
                          desc.getValueSerializer().deserialize(b.getValue()).get(),
                          b.getValue(),
                          b.getStamp());
                    }
                    return KeyValue.of(
                        getEntityDescriptor(),
                        desc,
                        key,
                        attribute,
                        new RawOffset(attribute),
                        desc.getValueSerializer().deserialize(b.getValue()).get(),
                        b.getValue(),
                        b.getStamp());
                  } catch (Exception ex) {
                    throw new RuntimeException(ex);
                  }
                });
      }
    }

    private Optional<StreamElement> getMapKey(String key, String attribute) {
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
      String prefixKey = toMapKey(key, prefix);
      boolean isOffsetExplicit = offset != null;
      off = prefix.compareTo(off) > 0 ? prefix : off;
      String startKey = toMapKey(key, off);
      int count = 0;
      try (Locker l = holder().lockRead()) {
        SortedMap<String, StreamElement> dataMap = getData().tailMap(startKey);
        for (Map.Entry<String, StreamElement> e : dataMap.entrySet()) {
          log.trace("Scanning entry {} looking for prefix {}", e, prefixKey);
          if (e.getValue().getStamp() <= stamp) {
            if (e.getKey().startsWith(prefixKey)) {
              int hash = e.getKey().lastIndexOf("#");
              String attribute = e.getKey().substring(hash + 1);
              if ((isOffsetExplicit && attribute.equals(off)) || e.getValue().isDelete()) {
                continue;
              }
              Optional<AttributeDescriptor<Object>> attr;
              attr = getEntityDescriptor().findAttribute(attribute, true);
              if (attr.isPresent()) {
                Optional<StreamElement> wildcard =
                    attr.get().isWildcard()
                        ? getMapKey(key, attr.get().toAttributePrefix())
                        : Optional.empty();
                if (!wildcard.isPresent() || wildcard.get().getStamp() < e.getValue().getStamp()) {
                  final Object value =
                      Optionals.get(
                          attr.get().getValueSerializer().deserialize(e.getValue().getValue()));
                  if (e.getValue().hasSequentialId()) {
                    consumer.accept(
                        KeyValue.of(
                            getEntityDescriptor(),
                            attr.get(),
                            e.getValue().getSequentialId(),
                            key,
                            attribute,
                            new RawOffset(attribute),
                            value,
                            e.getValue().getValue(),
                            System.currentTimeMillis()));
                  } else {
                    consumer.accept(
                        KeyValue.of(
                            getEntityDescriptor(),
                            attr.get(),
                            key,
                            attribute,
                            new RawOffset(attribute),
                            value,
                            e.getValue().getValue()));
                  }

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
      final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory =
          this.executorFactory;
      final InMemStorage storage = InMemStorage.this;
      return repo -> storage.new Reader(entity, uri, executorFactory);
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
    public cz.o2.proxima.direct.core.batch.ObserveHandle observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {

      TerminationContext terminationContext = new TerminationContext(observer);
      observeInternal(partitions, attributes, observer, terminationContext);
      return terminationContext;
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
                    final Map<String, StreamElement> data = getData().tailMap(prefix);
                    for (Entry<String, StreamElement> e : data.entrySet()) {
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
        Map.Entry<String, StreamElement> e) {

      if (terminationContext.isCancelled()) {
        return false;
      }
      if (!e.getKey().startsWith(prefix)) {
        return false;
      }
      String k = e.getKey();
      StreamElement v = e.getValue();
      String[] parts = k.substring(prefix.length()).split("#");
      String key = parts[0];
      String attribute = parts[1];
      return getEntityDescriptor()
          .findAttribute(attribute, true)
          .filter(attributes::contains)
          .map(desc -> observer.onNext(v, CONTEXT))
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
      return HOLDERS_MAP.computeIfAbsent(storage.getId(), k -> new DataHolder());
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
    final NavigableMap<String, StreamElement> data;
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
    return Objects.requireNonNull(
        DataHolders.get(this), () -> String.format("Missing holder for %s", this));
  }

  public NavigableMap<String, StreamElement> getData() {
    return holder().data;
  }

  NavigableMap<Integer, InMemIngestWriter> getObservers(URI uri) {
    return holder()
        .observers
        .computeIfAbsent(uri, tmp -> Collections.synchronizedNavigableMap(new TreeMap<>()));
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("inmem") ? Accept.ACCEPT : Accept.REJECT;
  }

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator op, AttributeFamilyDescriptor familyDescriptor) {
    final EntityDescriptor entity = familyDescriptor.getEntity();
    final URI uri = familyDescriptor.getStorageUri();
    final Map<String, Object> cfg = familyDescriptor.getCfg();

    log.info("Creating accessor {} for URI {}", getClass(), uri);
    /*
    holder()
        .observers
        .computeIfAbsent(uri, k -> Collections.synchronizedNavigableMap(new TreeMap<>()));
     */

    final int numPartitions =
        Optional.ofNullable(cfg.get(NUM_PARTITIONS))
            .map(v -> Integer.parseInt(v.toString()))
            .orElse(1);

    final Partitioner partitioner =
        Optional.ofNullable(cfg.get(ConfigConstants.PARTITIONER))
            .map(name -> Classpath.newInstance(name.toString(), Partitioner.class))
            .orElseGet(KeyAttributePartitioner::new);

    final Repository opRepo = op.getRepository();
    final RepositoryFactory repositoryFactory = opRepo.asFactory();
    final OnlineAttributeWriter.Factory<?> writerFactory =
        new Writer(entity, uri, numPartitions, partitioner).asFactory();
    final CommitLogReader.Factory<?> commitLogReaderFactory =
        new InMemCommitLogReader(
                entity, uri, op.getContext().getExecutorFactory(), partitioner, numPartitions)
            .asFactory();

    final RandomAccessReader.Factory<Reader> randomAccessReaderFactory;
    final BatchLogReader.Factory<Reader> batchLogReaderFactory;
    final CachedView.Factory cachedViewFactory;
    if (numPartitions > 1) {
      randomAccessReaderFactory = null;
      batchLogReaderFactory = null;
    } else {
      final ReaderFactory readerFactory =
          new Reader(entity, uri, op.getContext().getExecutorFactory()).asFactory();
      randomAccessReaderFactory = readerFactory;
      batchLogReaderFactory = readerFactory;
    }
    cachedViewFactory =
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
        return Optional.ofNullable(randomAccessReaderFactory).map(item -> item.apply(repo()));
      }

      @Override
      public Optional<CachedView> getCachedView(Context context) {
        Objects.requireNonNull(context);
        return Optional.ofNullable(cachedViewFactory).map(item -> item.apply(repo()));
      }

      @Override
      public Optional<BatchLogReader> getBatchLogReader(Context context) {
        Objects.requireNonNull(context);
        return Optional.ofNullable(batchLogReaderFactory).map(item -> item.apply(repo()));
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
      EntityDescriptor entity, StreamElement element) {

    return (AttributeDescriptor<T>)
        entity
            .findAttribute(element.getAttribute(), true)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing attribute " + element.getAttribute() + " in " + entity));
  }

  private static StreamElement cloneAndUpdateAttribute(
      EntityDescriptor entity, StreamElement elem) {

    if (elem.hasSequentialId()) {
      return StreamElement.upsert(
          entity,
          getAttributeOfEntity(entity, elem),
          elem.getSequentialId(),
          elem.getKey(),
          elem.getAttribute(),
          elem.getStamp(),
          elem.getValue());
    }
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
    return toStoragePrefix(uri) + toKeyAttribute(key, attribute);
  }

  private static String toKeyAttribute(String key, String attribute) {
    return key + "#" + attribute;
  }

  private static CommitLogObserver.OnNextContext asOnNextContext(
      CommitLogObserver.OffsetCommitter committer, Offset offset) {
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
