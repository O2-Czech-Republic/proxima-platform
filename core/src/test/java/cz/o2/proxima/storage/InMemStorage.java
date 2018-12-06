/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage;

import com.google.common.base.Preconditions;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.batch.BatchLogObserver;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.storage.randomaccess.RandomOffset;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.o2.proxima.view.input.DataSourceUtils;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.randomaccess.RawOffset;
import cz.o2.proxima.view.PartitionedCachedView;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * InMemStorage for testing purposes.
 */
@Slf4j
public class InMemStorage extends StorageDescriptor {

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
      implements CommitLogReader, PartitionedView {

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
      return Collections.singletonList(() -> 0);
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

      log.debug("Observing {} as {}", getUri(), name);
      try {
        flushBasedOnPosition(
            position,
            (el, committer) -> observer.onNext(el, committer::accept));
      } catch (InterruptedException ex) {
        log.warn("Interrupted while reading old data.", ex);
        Thread.currentThread().interrupt();
        stopAtCurrent = true;
      }
      final int id;
      if (!stopAtCurrent) {
        synchronized (observers) {
          id = observers.isEmpty() ? 0 : observers.lastKey() + 1;
          observers.put(id, elem -> {
            elem = cloneAndUpdateAttribute(getEntityDescriptor(), elem);
            try {
              observer.onNext(elem, (suc, err) -> { });
            } catch (Exception ex) {
              observer.onError(ex);
            }
          });
        }
      } else {
        observer.onCompleted();
        id = -1;
      }
      return new ObserveHandle() {

        @Override
        public void cancel() {
          observers.remove(id);
          observer.onCancelled();
        }

        @Override
        public List<Offset> getCommittedOffsets() {
          return Arrays.asList(() -> () -> 0);
        }

        @Override
        public void resetOffsets(List<Offset> offsets) {
          // nop
        }

        @Override
        public List<Offset> getCurrentOffsets() {
          return getCommittedOffsets();
        }

        @Override
        public void waitUntilReady() throws InterruptedException {
          // nop
        }

      };
    }

    @Override
    public <T> Dataset<T> observe(
        Flow flow,
        String name,
        PartitionedLogObserver<T> observer) {

      return observePartitions(flow, getPartitions(), observer);
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
    public <T> Dataset<T> observePartitions(
        Flow flow,
        Collection<Partition> partitions,
        PartitionedLogObserver<T> observer) {

      if (partitions.size() != 1 || partitions.stream().findFirst().get().getId() != 0) {
        throw new IllegalArgumentException(
            "This fake implementation has only single partition");
      }

      SynchronousQueue<T> queue = new SynchronousQueue<>();
      DataSourceUtils.Producer producer = () -> {
        Object lock = new Object();
        ObserveHandle handle = observe(
            "partitionedView-" + flow.getName(),
            new LogObserver() {

              @Override
              public boolean onNext(StreamElement ingest, OffsetCommitter confirm) {
                synchronized (lock) {
                  ingest = cloneAndUpdateAttribute(getEntityDescriptor(), ingest);
                  observer.onNext(ingest, confirm::commit, () -> 0, e -> {
                    try {
                      queue.put(e);
                    } catch (InterruptedException ex) {
                      Thread.currentThread().interrupt();
                    }
                  });
                }
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

            });
        synchronized (lock) {
          try {
            handle.waitUntilReady();
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
          observer.onRepartition(partitions);
        }

      };

      return flow.createInput(
          DataSourceUtils.fromPartitions(
              DataSourceUtils.fromBlockingQueue(queue, producer,
                  () -> new ArrayList<>(),
                  l -> { })));

    }

    @Override
    public ObserveHandle observeBulk(
        String name,
        Position position,
        boolean stopAtCurrent,
        BulkLogObserver observer) {

      try {
        flushBasedOnPosition(
            position,
            (el, committer) -> observer.onNext(el, () -> 0, committer::accept));
      } catch (InterruptedException ex) {
        log.warn("Interrupted while reading old data", ex);
        Thread.currentThread().interrupt();
        stopAtCurrent = true;
      }
      final int id;
      if (!stopAtCurrent) {
        synchronized (observers) {
          id = observers.isEmpty() ? 0 : observers.lastKey();
          observers.put(id, elem -> {
            elem = cloneAndUpdateAttribute(getEntityDescriptor(), elem);
            try {
              observer.onNext(elem, () -> 0, (suc, err) -> { });
            } catch (Exception ex) {
              observer.onError(ex);
            }
          });
        }
      } else {
        id = -1;
        observer.onCompleted();
      }
      return new ObserveHandle() {
        @Override
        public void cancel() {
          observers.remove(id);
          observer.onCancelled();
        }

        @Override
        public List<Offset> getCommittedOffsets() {
          return Arrays.asList(() -> () -> 0);
        }

        @Override
        public void resetOffsets(List<Offset> offsets) {
          // nop
        }

        @Override
        public List<Offset> getCurrentOffsets() {
          return getCommittedOffsets();
        }

        @Override
        public void waitUntilReady() throws InterruptedException {
          // nop
        }

      };
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        BulkLogObserver observer) {

      return observeBulk(name, position, stopAtCurrent, observer);
    }

    @Override
    public ObserveHandle observeBulkOffsets(
        Collection<Offset> offsets, BulkLogObserver observer) {

      return observeBulkPartitions(
          offsets.stream().map(Offset::getPartition).collect(Collectors.toList()),
          Position.NEWEST,
          observer);
    }

    private void flushBasedOnPosition(
        Position position,
        BiConsumer<StreamElement, BiConsumer<Boolean, Throwable>> consumer)
        throws InterruptedException {

      if (position == Position.OLDEST) {
        synchronized (data) {
          String prefix = getUri().getPath() + "/";
          int prefixLength = prefix.length();
          CountDownLatch latch = new CountDownLatch(
              (int) data
                  .entrySet()
                  .stream()
                  .filter(e -> e.getKey().startsWith(prefix))
                  .count());
          data.entrySet()
              .stream()
              .filter(e -> e.getKey().startsWith(prefix))
              .sorted((a, b) ->
                  Long.compare(a.getValue().getFirst(), b.getValue().getFirst()))
              .forEach(e -> {
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
                  latch.countDown();
                });
              });
          latch.await();
        }
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
      return Arrays.asList(() -> 0);
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

  private static class CachedView implements PartitionedCachedView {

    private final RandomAccessReader reader;
    private final CommitLogReader commitLogReader;
    private final OnlineAttributeWriter writer;
    private BiConsumer<StreamElement, Pair<Long, Object>> updateCallback;

    CachedView(
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
                  LogObserver.OffsetCommitter committer) {

                cache(ingest);
                committer.confirm();
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
      return Arrays.asList(() -> 0);
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

  }

  @Getter
  private final NavigableMap<String, Pair<Long, byte[]>> data;

  private final Map<URI, NavigableMap<Integer, InMemIngestWriter>> observers;

  public InMemStorage() {
    super(Arrays.asList("inmem"));
    this.data = Collections.synchronizedNavigableMap(new TreeMap<>());
    this.observers = new ConcurrentHashMap<>();
  }

  @Override
  public DataAccessor getAccessor(
      EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    observers.computeIfAbsent(uri, k -> Collections.synchronizedNavigableMap(
        new TreeMap<>()));
    NavigableMap<Integer, InMemIngestWriter> uriObservers = observers.get(uri);
    Writer writer = new Writer(entityDesc, uri, data, uriObservers);
    InMemCommitLogReader commitLogReader = new InMemCommitLogReader(
        entityDesc, uri, data, uriObservers);
    Reader reader = new Reader(entityDesc, uri, data);
    CachedView cachedView = new CachedView(reader, commitLogReader, writer);

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
      public Optional<PartitionedView> getPartitionedView(Context context) {
        Objects.requireNonNull(context);
        return Optional.of(commitLogReader);
      }

      @Override
      public Optional<PartitionedCachedView> getCachedView(Context context) {
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

}
