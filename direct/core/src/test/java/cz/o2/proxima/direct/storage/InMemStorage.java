/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.ObserverUtils;
import static cz.o2.proxima.direct.commitlog.ObserverUtils.asRepartitionContext;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader.Listing;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.direct.randomaccess.RawOffset;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.Nullable;
import lombok.Getter;
import java.util.UUID;
import java.util.concurrent.Executor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.ObjectStreamException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * InMemStorage for testing purposes.
 */
@Slf4j
public class InMemStorage implements DataAccessorFactory {

  private static final Partition PARTITION = () -> 0;

  static class IntOffset implements Offset {

    @Getter
    final long offset;

    public IntOffset(long offset) {
      this.offset = offset;
    }

    @Override
    public Partition getPartition() {
      return PARTITION;
    }
  }

  @FunctionalInterface
  private interface InMemIngestWriter extends Serializable {
    void write(StreamElement data);
  }

  public static final class Writer
      extends AbstractOnlineAttributeWriter {

    private final NavigableMap<String, Pair<Long, byte[]>> data;
    private final Map<Integer, InMemIngestWriter> observers;

    private Writer(
        EntityDescriptor entityDesc, URI uri,
        NavigableMap<String, Pair<Long, byte[]>> data,
        Map<Integer, InMemIngestWriter> observers) {

      super(entityDesc, uri);
      this.data = data;
      this.observers = observers;
    }


    @Override
    public void write(StreamElement data, CommitCallback statusCallback) {
      if (log.isDebugEnabled()) {
        synchronized (observers) {
          log.debug(
              "Writing element {} to {} with {} observers",
              data, getUri(), observers.size());
        }
      }
      String attr = data.isDeleteWildcard()
          ? data.getAttributeDescriptor().toAttributePrefix()
          : data.getAttribute();
      this.data.compute(
          toMapKey(getUri(), data.getKey(), attr),
          (key, old) -> {
            if (old != null && old.getFirst() > data.getStamp()) {
              return old;
            }
            return Pair.of(data.getStamp(), data.getValue());
          });
      synchronized (observers) {
        observers.values().forEach(o -> o.write(data));
      }
      statusCallback.commit(true, null);
    }

    @Override
    public void close() {
      // nop
    }

  }

  private static class InMemCommitLogReader
      extends AbstractStorage
      implements CommitLogReader {

    private final NavigableMap<Integer, InMemIngestWriter> observers;
    private final NavigableMap<String, Pair<Long, byte[]>> data;

    private InMemCommitLogReader(
        EntityDescriptor entityDesc, URI uri,
        NavigableMap<String, Pair<Long, byte[]>> data,
        NavigableMap<Integer, InMemIngestWriter> observers) {

      super(entityDesc, uri);
      this.data = data;
      this.observers = observers;
    }

    @Override
    public void close() {

    }

    @Override
    public List<Partition> getPartitions() {
      return Collections.singletonList(PARTITION);
    }

    @Override
    public ObserveHandle observe(
        String name,
        Position position,
        LogObserver observer) {

      return observe(name, position, false, observer);
    }

    private ObserveHandle observe(
        String name,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      return observe(name, position, 0L, stopAtCurrent, observer);
    }

    private ObserveHandle observe(
        String name,
        Position position,
        long offset,
        boolean stopAtCurrent,
        LogObserver observer) {

      log.debug("Observing {} as {}", getUri(), name);

      return doObserve(position, offset, stopAtCurrent, observer);
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
        String name,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      return observeBulk(name, position, 0L, stopAtCurrent, observer);
    }

    private ObserveHandle observeBulk(
        String name,
        Position position,
        long offset,
        boolean stopAtCurrent,
        LogObserver observer) {


      return doObserve(position, offset, stopAtCurrent, observer);
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
    public ObserveHandle observeBulkOffsets(
        Collection<Offset> offsets, LogObserver observer) {

      return doObserve(Position.OLDEST,
          ((IntOffset) Iterables.getOnlyElement(offsets)).getOffset(),
          false,
          observer);
    }

    private ObserveHandle doObserve(
        Position position,
        long offset,
        boolean stopAtCurrent,
        LogObserver observer) {

      int id = createConsumerId(stopAtCurrent);

      observer.onRepartition(asRepartitionContext(Arrays.asList(PARTITION)));
      AtomicReference<Thread> threadInterrupt = new AtomicReference<>();
      AtomicBoolean killSwitch = new AtomicBoolean();
      AtomicLong offsetTracker = flushBasedOnPosition(
          position,
          offset,
          id,
          stopAtCurrent,
          killSwitch,
          threadInterrupt,
          observer);

      return createHandle(
          id, observer, offsetTracker, killSwitch, threadInterrupt);
    }


    private int createConsumerId(boolean stopAtCurrent) {
      final int id;
      if (!stopAtCurrent) {
        synchronized (observers) {
          id = observers.isEmpty() ? 0 : observers.lastKey() + 1;
          // insert placeholder
          observers.put(id, elem -> { });
        }
      } else {
        id = -1;
      }
      return id;
    }

    private ObserveHandle createHandle(
        int consumerId, LogObserver observer,
        AtomicLong offsetTracker,
        AtomicBoolean killSwitch,
        AtomicReference<Thread> threadInterrupt) {

      return new ObserveHandle() {

        @Override
        public void cancel() {
          observers.remove(consumerId);
          killSwitch.set(true);
          threadInterrupt.get().interrupt();
          observer.onCancelled();
        }

        @Override
        public List<Offset> getCommittedOffsets() {
          return Arrays.asList(new IntOffset(0));
        }

        @Override
        public void resetOffsets(List<Offset> offsets) {
          // nop
        }

        @Override
        public List<Offset> getCurrentOffsets() {
          return Arrays.asList(new IntOffset(offsetTracker.get()));
        }

        @Override
        public void waitUntilReady() throws InterruptedException {
          // nop
        }

      };
    }

    private AtomicLong flushBasedOnPosition(
        Position position,
        long offset,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        AtomicReference<Thread> observeThread,
        LogObserver observer) {

      AtomicLong offsetTracker = new AtomicLong(offset);
      CountDownLatch latch = new CountDownLatch(1);
      observeThread.set(new Thread(() -> handleFlushDataBaseOnPosition(
          position, offset, consumerId, stopAtCurrent, killSwitch,
          offsetTracker, latch, observer)));
      observeThread.get().start();
      try {
        latch.await();
      } catch (InterruptedException ex) {
        log.warn("Interrupted.", ex);
      }
      return offsetTracker;
    }

    private void handleFlushDataBaseOnPosition(
        Position position,
        long offset,
        int consumerId,
        boolean stopAtCurrent,
        AtomicBoolean killSwitch,
        AtomicLong offsetTracker,
        CountDownLatch latch,
        LogObserver observer) {


      AtomicLong restartedOffset = new AtomicLong();

      BiConsumer<StreamElement, OffsetCommitter> consumer = (el, committer) -> {
        try {
          if (!killSwitch.get()) {
            long off = offsetTracker.incrementAndGet();
            el = cloneAndUpdateAttribute(getEntityDescriptor(), el);
            killSwitch.compareAndSet(false,
                !observer.onNext(el, asOnNextContext(committer, new IntOffset(off))));
          }
        } catch (Exception ex) {
          observer.onError(ex);
        }
      };


      if (position == Position.OLDEST) {
        synchronized (data) {
          latch.countDown();
          String prefix = getUri().getPath() + "/";
          int prefixLength = prefix.length();
          data.entrySet()
              .stream()
              .filter(e -> e.getKey().startsWith(prefix))
              .sorted((a, b) ->
                  Long.compare(a.getValue().getFirst(), b.getValue().getFirst()))
              .forEachOrdered(e -> {
                if (restartedOffset.getAndIncrement() < offset) {
                  return;
                }
                String[] parts = e.getKey().substring(prefixLength).split("#");
                String key = parts[0];
                String attribute = parts[1];
                AttributeDescriptor<?> desc = getEntityDescriptor()
                    .findAttribute(attribute, true)
                    .orElseThrow(() -> new IllegalArgumentException(
                        "Missing attribute " + attribute));
                byte[] value = e.getValue().getSecond();
                StreamElement element = StreamElement.update(
                    getEntityDescriptor(), desc, UUID.randomUUID().toString(),
                    key, attribute, e.getValue().getFirst(), value);
                consumer.accept(element, (succ, exc) -> {
                  if (!succ) {
                    throw new IllegalStateException("Error in observing old data", exc);
                  }
                });
              });
          if (!killSwitch.get()) {
            if (!stopAtCurrent) {
              observers.put(
                  consumerId,
                  el -> consumer.accept(el, (succ, exc) -> { }));
            } else {
              observer.onCompleted();
            }
          }
        }
      } else {
        if (!stopAtCurrent) {
          observers.put(
              consumerId,
              el -> consumer.accept(el, (succ, exc) -> { }));
        } else {
          observer.onCompleted();
        }
        latch.countDown();
      }
    }

  }

  private static final class Reader
      extends AbstractStorage
      implements RandomAccessReader, BatchLogObservable {

    private final NavigableMap<String, Pair<Long, byte[]>> data;
    @Setter
    private Factory<Executor> executorFactory;
    private transient Executor executor;

    private Reader(
        EntityDescriptor entityDesc, URI uri,
        NavigableMap<String, Pair<Long, byte[]>> data) {

      super(entityDesc, uri);
      this.data = data;
    }

    @Override
    public <T> Optional<KeyValue<T>> get(
        String key,
        String attribute,
        AttributeDescriptor<T> desc,
        long stamp) {

      Optional<Pair<Long, byte[]>> wildcard
          = desc.isWildcard() && !attribute.equals(desc.toAttributePrefix())
              ? getMapKey(key, desc.toAttributePrefix())
              : Optional.empty();
      return getMapKey(key, attribute)
          .filter(p -> p.getSecond() != null)
          .filter(p -> !wildcard.isPresent() || wildcard.get().getFirst() < p.getFirst())
          .map(b -> {
            try {
              return KeyValue.of(
                  getEntityDescriptor(),
                  desc, key, attribute,
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
      return Optional.ofNullable(data.get(toMapKey(key, attribute)));
    }

    private String toMapKey(String key, String attribute) {
      return InMemStorage.toMapKey(getUri(), key, attribute);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset, long stamp,
        int limit, Consumer<KeyValue<?>> consumer) {

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
      for (Map.Entry<String, Pair<Long, byte[]>> e : data.tailMap(start).entrySet()) {
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
              Optional<Pair<Long, byte[]>> wildcard = attr.get().isWildcard()
                  ? getMapKey(key, attr.get().toAttributePrefix())
                  : Optional.empty();
              if (!wildcard.isPresent()
                  || wildcard.get().getFirst() < e.getValue().getFirst()) {

                consumer.accept(KeyValue.of(
                    getEntityDescriptor(),
                    (AttributeDescriptor) attr.get(),
                    key,
                    attribute,
                    new RawOffset(attribute),
                    attr.get().getValueSerializer().deserialize(
                        e.getValue().getSecond()).get(),
                    e.getValue().getSecond()));

                if (++count == limit) {
                  break;
                }
              }
            } else {
              log.warn(
                  "Unknown attribute {} in entity {}",
                  attribute, getEntityDescriptor());
            }
          } else {
            break;
          }
        }
      }
    }


    @Override
    public void listEntities(
        RandomOffset offset,
        int limit,
        Consumer<Pair<RandomOffset, String>> consumer) {

      String off = offset == null ? "" : ((RawOffset) offset).getOffset();
      for (String k : data.tailMap(off).keySet()) {
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
    public void close() {

    }

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

      Preconditions.checkArgument(
          partitions.size() == 1,
          "This observable works on single partition only, got " + partitions);
      int prefix = getUri().getPath().length() + 1;
      executor().execute(() -> {
        try {
          data.forEach((k, v) -> {
            String[] parts = k.substring(prefix).split("#");
            String key = parts[0];
            String attribute = parts[1];
            getEntityDescriptor().findAttribute(attribute, true)
                .flatMap(desc -> attributes.contains(desc)
                    ? Optional.of(desc) : Optional.empty())
                .ifPresent(desc -> {
                  observer.onNext(StreamElement.update(
                      getEntityDescriptor(), desc, UUID.randomUUID().toString(), key,
                      attribute, v.getFirst(), v.getSecond()));
                });
          });
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

  private static class InMemCachedView implements CachedView {

    private final RandomAccessReader reader;
    private final CommitLogReader commitLogReader;
    private final OnlineAttributeWriter writer;
    private BiConsumer<StreamElement, Pair<Long, Object>> updateCallback;

    InMemCachedView(
        RandomAccessReader reader,
        CommitLogReader commitLogReader,
        OnlineAttributeWriter writer) {

      this.reader = reader;
      this.commitLogReader = commitLogReader;
      this.writer = writer;
    }

    @Override
    public void assign(
        Collection<Partition> partitions,
        BiConsumer<StreamElement, Pair<Long, Object>> updateCallback) {

      this.updateCallback = updateCallback;
      if (updateCallback != null) {
        this.commitLogReader.observe(
            UUID.randomUUID().toString(),
            new LogObserver() {
              @Override
              public boolean onNext(
                  StreamElement ingest,
                  OnNextContext context) {

                cache(ingest);
                context.confirm();
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });
      }

    }

    @Override
    public Collection<Partition> getAssigned() {
      return Arrays.asList(PARTITION);
    }

    @Override
    public RandomOffset fetchOffset(Listing type, String key) {
      return reader.fetchOffset(type, key);
    }

    @Override
    public <T> Optional<KeyValue<T>> get(
        String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

      return reader.get(key, attribute, desc, stamp);
    }

    @Override
    public <T> void scanWildcard(
        String key, AttributeDescriptor<T> wildcard,
        RandomOffset offset,
        long stamp,
        int limit,
        Consumer<KeyValue<T>> consumer) {

      reader.scanWildcard(key, wildcard, offset, stamp, limit, consumer);
    }

    @Override
    public void listEntities(
        RandomOffset offset,
        int limit,
        Consumer<Pair<RandomOffset, String>> consumer) {

      reader.listEntities(offset, limit, consumer);
    }

    @Override
    public EntityDescriptor getEntityDescriptor() {
      return reader.getEntityDescriptor();
    }

    @Override
    public void close() {

    }

    @Override
    public void write(StreamElement data, CommitCallback statusCallback) {
      cache(data);
      writer.write(data, statusCallback);
      getAttributeOfEntity(getEntityDescriptor(), data);
    }

    @Override
    public URI getUri() {
      return writer.getUri();
    }

    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset, long stamp,
        int limit, Consumer<KeyValue<?>> consumer) {

      reader.scanWildcardAll(key, offset, stamp, limit, consumer);
    }

    @Override
    public void cache(StreamElement element) {
      updateCallback.accept(
          element,
          Pair.of(-1L, get(
              element.getKey(), element.getAttribute(),
              element.getAttributeDescriptor()).orElse(null)));
    }

    @Override
    public Collection<Partition> getPartitions() {
      return Arrays.asList(PARTITION);
    }

  }

  private static InMemStorage INSTANCE;

  @Getter
  private final NavigableMap<String, Pair<Long, byte[]>> data;

  private final Map<URI, NavigableMap<Integer, InMemIngestWriter>> observers;

  public InMemStorage() {
    this.data = Collections.synchronizedNavigableMap(new TreeMap<>());
    this.observers = Collections.synchronizedMap(new HashMap<>());
    if (INSTANCE == null) {
      // store the first created instance for deserialization purposes
      INSTANCE = this;
    }
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("inmem") ? Accept.ACCEPT : Accept.REJECT;
  }

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator op,
      EntityDescriptor entity,
      URI uri,
      Map<String, Object> cfg) {

    observers.computeIfAbsent(uri, k -> Collections.synchronizedNavigableMap(
        new TreeMap<>()));
    NavigableMap<Integer, InMemIngestWriter> uriObservers = observers.get(uri);
    Writer writer = new Writer(entity, uri, data, uriObservers);
    InMemCommitLogReader commitLogReader = new InMemCommitLogReader(
        entity, uri, data, uriObservers);
    Reader reader = new Reader(entity, uri, data);
    CachedView cachedView = new InMemCachedView(reader, commitLogReader, writer);

    return new DataAccessor() {
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
        reader.setExecutorFactory(() -> context.getExecutorService());
        return Optional.of(reader);
      }

    };
  }

  @SuppressWarnings("unchecked")
  private static <T> AttributeDescriptor<T> getAttributeOfEntity(
      EntityDescriptor entity,
      StreamElement ingest) {

    return (AttributeDescriptor<T>) entity
        .findAttribute(ingest.getAttribute(), true)
        .orElseThrow(() -> new IllegalStateException(
            "Missing attribute " + ingest.getAttribute() + " in " + entity));
  }

  private static StreamElement cloneAndUpdateAttribute(
      EntityDescriptor entity, StreamElement elem) {

    return StreamElement.update(
        entity,
        getAttributeOfEntity(entity, elem),
        elem.getUuid(),
        elem.getKey(),
        elem.getAttribute(),
        elem.getStamp(),
        elem.getValue());
  }

  private static String toMapKey(URI uri, String key, String attribute) {
    return uri.getPath() + "/" + key + "#" + attribute;
  }

  private static LogObserver.OnNextContext asOnNextContext(
      LogObserver.OffsetCommitter committer, Offset offset) {

    return ObserverUtils.asOnNextContext(
        committer, offset, () -> System.currentTimeMillis() - 100);
  }

  // disable deserialization
  private Object readResolve() throws ObjectStreamException {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    return this;
  }

}
