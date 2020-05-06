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
package cz.o2.proxima.direct.storage;

import static cz.o2.proxima.direct.commitlog.ObserverUtils.asRepartitionContext;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
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
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.direct.randomaccess.RawOffset;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.direct.view.LocalCachedPartitionedView;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkSupplier;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/** InMemStorage for testing purposes. */
@Slf4j
public class InMemStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  private static final Partition PARTITION = () -> 0;
  private static final long IDLE_FLUSH_TIME = 500L;
  private static long BOUNDED_OUT_OF_ORDERNESS = 5000L;

  static class IntOffset implements Offset {

    private static final long serialVersionUID = 1L;

    @Getter final long offset;
    @Getter final long watermark;

    public IntOffset(long offset, long watermark) {
      this.offset = offset;
      this.watermark = watermark;
    }

    @Override
    public Partition getPartition() {
      return PARTITION;
    }

    @Override
    public String toString() {
      return "IntOffset(" + "offset=" + offset + ", watermark=" + watermark + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IntOffset) {
        IntOffset other = (IntOffset) obj;
        return other.offset == this.offset && other.watermark == this.watermark;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int) ((offset ^ watermark) % Integer.MAX_VALUE);
    }
  }

  @FunctionalInterface
  private interface InMemIngestWriter extends Serializable {
    void write(StreamElement data);
  }

  public final class Writer extends AbstractOnlineAttributeWriter {

    private Writer(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public void write(StreamElement data, CommitCallback statusCallback) {
      NavigableMap<Integer, InMemIngestWriter> writeObservers = getObservers(getUri());
      if (log.isDebugEnabled()) {
        synchronized (writeObservers) {
          log.debug(
              "Writing element {} to {} with {} observers", data, getUri(), writeObservers.size());
        }
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
      final List<InMemIngestWriter> observers;
      synchronized (writeObservers) {
        observers = Lists.newArrayList(writeObservers.values());
      }
      observers.forEach(
          o -> {
            o.write(data);
            log.debug("Passed element {} to {}", data, o);
          });
      statusCallback.commit(true, null);
    }

    @Override
    public void close() {
      // nop
    }
  }

  private class InMemCommitLogReader extends AbstractStorage implements CommitLogReader {

    private InMemCommitLogReader(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public List<Partition> getPartitions() {
      return Collections.singletonList(PARTITION);
    }

    @Override
    public ObserveHandle observe(String name, Position position, LogObserver observer) {

      return observe(name, position, false, observer);
    }

    private ObserveHandle observe(
        String name, Position position, boolean stopAtCurrent, LogObserver observer) {

      return observe(name, position, new IntOffset(0L, Long.MIN_VALUE), stopAtCurrent, observer);
    }

    private ObserveHandle observe(
        String name,
        Position position,
        IntOffset offset,
        boolean stopAtCurrent,
        LogObserver observer) {

      return doObserve(position, offset, stopAtCurrent, observer, name);
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

      return observeBulk(
          name, position, new IntOffset(0L, Long.MIN_VALUE), stopAtCurrent, observer);
    }

    private ObserveHandle observeBulk(
        String name,
        Position position,
        IntOffset offset,
        boolean stopAtCurrent,
        LogObserver observer) {

      return doObserve(position, offset, stopAtCurrent, observer, name);
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      return observeBulk(name, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {

      return doObserve(
          Position.OLDEST, ((IntOffset) Iterables.getOnlyElement(offsets)), false, observer, null);
    }

    private ObserveHandle doObserve(
        Position position,
        IntOffset offset,
        boolean stopAtCurrent,
        LogObserver observer,
        @Nullable String name) {

      log.debug(
          "Observing {} as {} from offset {} with position {} and stopAtCurrent {} using observer {}",
          getUri(),
          name,
          offset,
          position,
          stopAtCurrent,
          observer);

      int id = createConsumerId(stopAtCurrent);

      observer.onRepartition(asRepartitionContext(Arrays.asList(PARTITION)));
      AtomicReference<Thread> threadInterrupt = new AtomicReference<>();
      AtomicBoolean killSwitch = new AtomicBoolean();
      Supplier<IntOffset> offsetSupplier =
          flushBasedOnPosition(
              name, position, offset, id, stopAtCurrent, killSwitch, threadInterrupt, observer);

      return createHandle(id, observer, offsetSupplier, killSwitch, threadInterrupt);
    }

    private int createConsumerId(boolean stopAtCurrent) {
      final int id;
      if (!stopAtCurrent) {
        NavigableMap<Integer, InMemIngestWriter> uriObservers = getObservers(getUri());
        synchronized (uriObservers) {
          id = uriObservers.isEmpty() ? 0 : uriObservers.lastKey() + 1;
          // insert placeholder
          uriObservers.put(id, elem -> {});
        }
      } else {
        id = -1;
      }
      return id;
    }

    private ObserveHandle createHandle(
        int consumerId,
        LogObserver observer,
        Supplier<IntOffset> offsetTracker,
        AtomicBoolean killSwitch,
        AtomicReference<Thread> threadInterrupt) {

      return new ObserveHandle() {

        @Override
        public void close() {
          getObservers(getUri()).remove(consumerId);
          killSwitch.set(true);
          threadInterrupt.get().interrupt();
          observer.onCancelled();
        }

        @Override
        public List<Offset> getCommittedOffsets() {
          // no commits supported for now
          return Arrays.asList(new IntOffset(0, Long.MIN_VALUE));
        }

        @Override
        public void resetOffsets(List<Offset> offsets) {
          // nop
        }

        @Override
        public List<Offset> getCurrentOffsets() {
          return Arrays.asList(offsetTracker.get());
        }

        @Override
        public void waitUntilReady() {
          // nop
        }
      };
    }

    private Supplier<IntOffset> flushBasedOnPosition(
        @Nullable String name,
        Position position,
        IntOffset offset,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        AtomicReference<Thread> observeThread,
        LogObserver observer) {

      AtomicLong offsetTracker = new AtomicLong(offset.getOffset());
      WatermarkEstimator watermark =
          holder.getWatermarkEstimator(
              getUri(),
              offset.getWatermark(),
              MoreObjects.firstNonNull(name, "InMemConsumer@" + getUri() + ":" + consumerId));
      CountDownLatch latch = new CountDownLatch(1);
      observeThread.set(
          new Thread(
              () ->
                  handleFlushDataBaseOnPosition(
                      position,
                      offset,
                      consumerId,
                      stopAtCurrent,
                      killSwitch,
                      offsetTracker,
                      watermark,
                      latch,
                      observer)));
      observeThread.get().start();
      try {
        latch.await();
      } catch (InterruptedException ex) {
        log.warn("Interrupted.", ex);
      }
      return () -> new IntOffset(offsetTracker.get(), watermark.getWatermark());
    }

    private void handleFlushDataBaseOnPosition(
        Position position,
        IntOffset offset,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        AtomicLong offsetTracker,
        WatermarkEstimator watermark,
        CountDownLatch latch,
        LogObserver observer) {

      AtomicLong restartedOffset = new AtomicLong();
      AtomicReference<ScheduledFuture<?>> onIdleRef = new AtomicReference<>();

      Runnable onIdle =
          () -> {
            synchronized (observer) {
              watermark.onIdle();
              observer.onIdle(watermark::getWatermark);
              if (watermark.getWatermark() >= WatermarkEstimator.MAX_TIMESTAMP) {
                observer.onCompleted();
                getObservers(getUri()).remove(consumerId);
                onIdleRef.get().cancel(true);
                killSwitch.set(true);
              }
            }
          };
      ScheduledFuture<?> onIdleFuture =
          scheduler.scheduleAtFixedRate(
              onIdle, IDLE_FLUSH_TIME, IDLE_FLUSH_TIME, TimeUnit.MILLISECONDS);
      onIdleRef.set(onIdleFuture);
      BiConsumer<StreamElement, OffsetCommitter> consumer =
          (el, committer) -> {
            try {
              if (!killSwitch.get()) {
                el = cloneAndUpdateAttribute(getEntityDescriptor(), el);
                long off = offsetTracker.incrementAndGet();
                watermark.accumulate(el);
                long w = watermark.getWatermark();
                synchronized (observer) {
                  killSwitch.compareAndSet(
                      false,
                      !observer.onNext(el, asOnNextContext(committer, new IntOffset(off, w))));
                }
              }
            } catch (Exception ex) {
              synchronized (observer) {
                observer.onError(ex);
              }
            }
          };

      NavigableMap<Integer, InMemIngestWriter> uriObservers = getObservers(getUri());
      if (position == Position.OLDEST) {
        synchronized (getData()) {
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
                    if (restartedOffset.getAndIncrement() < offset.getOffset()) {
                      return;
                    }
                    String[] parts = e.getKey().substring(prefixLength).split("#");
                    String key = parts[0];
                    String attribute = parts[1];
                    AttributeDescriptor<?> desc =
                        getEntityDescriptor()
                            .findAttribute(attribute, true)
                            .orElseThrow(
                                () ->
                                    new IllegalArgumentException("Missing attribute " + attribute));
                    byte[] value = e.getValue().getSecond();
                    StreamElement element =
                        StreamElement.upsert(
                            getEntityDescriptor(),
                            desc,
                            UUID.randomUUID().toString(),
                            key,
                            attribute,
                            e.getValue().getFirst(),
                            value);
                    consumer.accept(
                        element,
                        (succ, exc) -> {
                          if (!succ && exc != null) {
                            throw new IllegalStateException("Error in observing old data", exc);
                          }
                        });
                  });
          if (!killSwitch.get()) {
            if (!stopAtCurrent) {
              uriObservers.put(consumerId, el -> consumer.accept(el, (succ, exc) -> {}));
            } else {
              observer.onCompleted();
              onIdleFuture.cancel(true);
            }
          }
        }
      } else {
        if (!stopAtCurrent) {
          uriObservers.put(consumerId, el -> consumer.accept(el, (succ, exc) -> {}));
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
  }

  private final class Reader extends AbstractStorage
      implements RandomAccessReader, BatchLogObservable {

    @Setter private Factory<Executor> executorFactory;
    private transient Executor executor;

    private Reader(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public <T> Optional<KeyValue<T>> get(
        String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

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

    private Optional<Pair<Long, byte[]>> getMapKey(String key, String attribute) {
      return Optional.ofNullable(getData().get(toMapKey(key, attribute)));
    }

    private String toMapKey(String key, String attribute) {
      return InMemStorage.toMapKey(getUri(), key, attribute);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset, long stamp, int limit, Consumer<KeyValue<?>> consumer) {

      scanWildcardPrefix(key, "", offset, stamp, limit, (Consumer) consumer);
    }

    @Override
    @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
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
                        (AttributeDescriptor) attr.get(),
                        key,
                        attribute,
                        new RawOffset(attribute),
                        attr.get().getValueSerializer().deserialize(e.getValue().getSecond()).get(),
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

    @Override
    public void listEntities(
        RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

      String off = offset == null ? "" : ((RawOffset) offset).getOffset();
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

    @Override
    public void close() {}

    @Override
    public RandomOffset fetchOffset(Listing type, String key) {
      return new RawOffset(key);
    }

    @Override
    public List<Partition> getPartitions(long startStamp, long endStamp) {
      return Arrays.asList(PARTITION);
    }

    @Override
    public void observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {

      log.debug(
          "Batch observing {} partitions {} for attributes {}", getUri(), partitions, attributes);
      Preconditions.checkArgument(
          partitions.size() == 1,
          "This observable works on single partition only, got " + partitions);
      String prefix = toStoragePrefix(getUri());
      int prefixLength = prefix.length();
      executor()
          .execute(
              () -> {
                try {
                  Map<String, Pair<Long, byte[]>> data = getData().tailMap(prefix);
                  synchronized (data) {
                    for (Map.Entry<String, Pair<Long, byte[]>> e : data.entrySet()) {
                      if (!e.getKey().startsWith(prefix)) {
                        break;
                      }
                      String k = e.getKey();
                      Pair<Long, byte[]> v = e.getValue();
                      String[] parts = k.substring(prefixLength).split("#");
                      String key = parts[0];
                      String attribute = parts[1];
                      boolean shouldContinue =
                          getEntityDescriptor()
                              .findAttribute(attribute, true)
                              .flatMap(
                                  desc ->
                                      attributes.contains(desc)
                                          ? Optional.of(desc)
                                          : Optional.empty())
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
                                          PARTITION))
                              .orElse(true);
                      if (!shouldContinue) {
                        break;
                      }
                    }
                  }
                  observer.onCompleted();
                } catch (Throwable err) {
                  observer.onError(err);
                }
              });
    }

    private Executor executor() {
      if (executor == null) {
        executor = executorFactory.apply();
      }
      return executor;
    }
  }

  /** Estimator of watermark. */
  public interface WatermarkEstimator extends Serializable, WatermarkSupplier {

    long MAX_TIMESTAMP = Long.MAX_VALUE - BOUNDED_OUT_OF_ORDERNESS;

    /** Update the watermark estimate with given element. */
    void accumulate(StreamElement element);

    /** Signal that all data is processed and reader is empty. */
    default void onIdle() {}
  }

  public static class DefaultWatermarkEstimator implements WatermarkEstimator {

    private long watermark;
    private long lastWatermarkWhenIdle = Long.MIN_VALUE;

    public DefaultWatermarkEstimator(long initialWatermark) {
      this.watermark = initialWatermark;
    }

    @Override
    public void accumulate(StreamElement element) {
      watermark = Math.max(watermark, element.getStamp() - BOUNDED_OUT_OF_ORDERNESS);
    }

    @Override
    public long getWatermark() {
      return watermark;
    }

    @Override
    public void onIdle() {
      watermark = Math.max(watermark, lastWatermarkWhenIdle + IDLE_FLUSH_TIME);
      lastWatermarkWhenIdle = getWatermark();
    }
  }

  @FunctionalInterface
  public interface WatermarkEstimatorFactory extends Serializable {
    WatermarkEstimator apply(long stamp, String consumer);
  }

  private static class DataHolder {
    final NavigableMap<String, Pair<Long, byte[]>> data;
    final Map<URI, NavigableMap<Integer, InMemIngestWriter>> observers;
    final Map<URI, WatermarkEstimatorFactory> watermarkEstimatorFactories;

    DataHolder() {
      this.data = Collections.synchronizedNavigableMap(new TreeMap<>());
      this.observers = Collections.synchronizedMap(new HashMap<>());
      this.watermarkEstimatorFactories = new ConcurrentHashMap<>();
    }

    WatermarkEstimator getWatermarkEstimator(
        URI uri, long initializationWatermark, String consumerName) {

      return Optional.ofNullable(watermarkEstimatorFactories.get(uri))
          .map(f -> f.apply(initializationWatermark, consumerName))
          .orElseGet(() -> new DefaultWatermarkEstimator(initializationWatermark));
    }

    void clear() {
      this.data.clear();
      this.observers.clear();
      this.watermarkEstimatorFactories.clear();
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
    holder.watermarkEstimatorFactories.put(uri, factory);
  }

  private static final DataHolder holder = new DataHolder();

  private static final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(4);

  public InMemStorage() {
    // this is hackish, but working as expected
    // that is - when we create new instance of the storage,
    // we want the storage to be empty
    // we should *never* be using two instances of InMemStorage
    // simultaneously, as that would imply we are working with
    // two repositories, which is not supported
    holder.clear();
    log.info("Created new empty {}", getClass().getName());
  }

  public NavigableMap<String, Pair<Long, byte[]>> getData() {
    return holder.data;
  }

  NavigableMap<Integer, InMemIngestWriter> getObservers(URI uri) {
    return Objects.requireNonNull(
        holder.observers.get(uri), () -> String.format("Missing observer for [%s]", uri));
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("inmem") ? Accept.ACCEPT : Accept.REJECT;
  }

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator op, EntityDescriptor entity, URI uri, Map<String, Object> cfg) {

    log.info("Creating accessor {} for URI {}", getClass(), uri);
    holder.observers.computeIfAbsent(
        uri, k -> Collections.synchronizedNavigableMap(new TreeMap<>()));
    final Writer writer = new Writer(entity, uri);
    final InMemCommitLogReader commitLogReader = new InMemCommitLogReader(entity, uri);
    final Reader reader = new Reader(entity, uri);
    final CachedView cachedView = new LocalCachedPartitionedView(entity, commitLogReader, writer);

    return new DataAccessor() {

      private static final long serialVersionUID = 1L;

      @Override
      public Optional<AttributeWriterBase> getWriter(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(writer);
      }

      @Override
      public Optional<CommitLogReader> getCommitLogReader(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(commitLogReader);
      }

      @Override
      public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(reader);
      }

      @Override
      public Optional<CachedView> getCachedView(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(cachedView);
      }

      @Override
      public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
        Objects.requireNonNull(context);
        reader.setExecutorFactory(asExecutorFactory(context));
        return Optional.of(reader);
      }
    };
  }

  private static Factory<Executor> asExecutorFactory(Context context) {
    return context::getExecutorService;
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
}
