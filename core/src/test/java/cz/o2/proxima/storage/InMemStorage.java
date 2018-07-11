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

import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
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
              data, getURI(), observers.size());
        }
      }
      if (data.isDeleteWildcard()) {
        String prefix = getURI().getPath() + "/" + data.getKey()
            + "#" + data.getAttributeDescriptor().toAttributePrefix();
        for (Map.Entry<String, Pair<Long, byte[]>> e
            : this.data.tailMap(prefix).entrySet()) {
          if (!e.getKey().startsWith(prefix)) {
            break;
          }
          if (e.getValue().getFirst() < data.getStamp()) {
            String attr = e.getKey().substring(prefix.lastIndexOf('#') + 1);
            write(StreamElement.delete(
                data.getEntityDescriptor(), data.getAttributeDescriptor(),
                data.getUuid(), data.getKey(), attr, data.getStamp()),
                (succ, exc) -> { });
          }
        }
      } else {
        this.data.compute(
            getURI().getPath() + "/" + data.getKey() + "#" + data.getAttribute(),
            (key, old) -> {
              if (old != null && old.getFirst() > data.getStamp()) {
                return old;
              }
              return Pair.of(data.getStamp(), data.getValue());
            });
      }
      synchronized (observers) {
        observers.values().forEach(o -> o.write(data));
      }
      statusCallback.commit(true, null);
    }
  }

  private static class InMemCommitLogReader
      extends AbstractStorage
      implements CommitLogReader, PartitionedView {

    private final NavigableMap<Integer, InMemIngestWriter> observers;

    private InMemCommitLogReader(
        EntityDescriptor entityDesc, URI uri,
        NavigableMap<Integer, InMemIngestWriter> observers) {

      super(entityDesc, uri);
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

      log.debug("Observing {} as {}", getURI(), name);
      logAndFixPosition(position);
      final int id;
      if (!stopAtCurrent) {
        synchronized (observers) {
          id = observers.isEmpty() ? 0 : observers.lastKey() + 1;
          observers.put(id, elem -> {
            validateAttributeInEntity(getEntityDescriptor(), elem);
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
        BulkLogObserver observer) {

      logAndFixPosition(position);
      final int id;
      if (!stopAtCurrent) {
        synchronized (observers) {
          id = observers.isEmpty() ? 0 : observers.lastKey();
          observers.put(id, elem -> {
            validateAttributeInEntity(getEntityDescriptor(), elem);
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
        observer.onRepartition(partitions);
        observe("partitionedView-" + flow.getName(), new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, OffsetCommitter confirm) {
            validateAttributeInEntity(getEntityDescriptor(), ingest);
            observer.onNext(ingest, confirm::commit, () -> 0, e -> {
              try {
                queue.put(e);
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
              }
            });
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }

        });
      };

      return flow.createInput(
          DataSourceUtils.fromPartitions(
              DataSourceUtils.fromBlockingQueue(queue, producer,
                  () -> new ArrayList<>(),
                  l -> { })));

    }

    @Override
    public <T> Dataset<T> observe(
        Flow flow,
        String name,
        PartitionedLogObserver<T> observer) {

      return observePartitions(flow, getPartitions(), observer);
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

    private Position logAndFixPosition(Position position) {
      if (position != Position.NEWEST) {
        log.warn(
            "InMemStorage cannot observe data other than NEWEST, got {}, fixing.",
            position);
      }
      return Position.NEWEST;
    }

  }

  private static final class Reader
      extends AbstractStorage
      implements RandomAccessReader {

    private final NavigableMap<String, Pair<Long, byte[]>> data;

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
        AttributeDescriptor<T> desc) {

      String mapKey = getURI().getPath() + "/" + key + "#" + attribute;
      return Optional.ofNullable(data.get(mapKey))
          .filter(p -> p.getSecond() != null)
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

    @SuppressWarnings("unchecked")
    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset,
        int limit, Consumer<KeyValue<?>> consumer) {

      scanWildcardPrefix(key, "", offset, limit, (Consumer) consumer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void scanWildcard(
        String key,
        AttributeDescriptor<T> wildcard,
        @Nullable RandomOffset offset,
        int limit,
        Consumer<KeyValue<T>> consumer) {

      String prefix = wildcard.toAttributePrefix();
      scanWildcardPrefix(key, prefix, offset, limit, (Consumer) consumer);
    }

    @SuppressWarnings("unchecked")
    private void scanWildcardPrefix(
        String key,
        String prefix,
        @Nullable RandomOffset offset,
        int limit,
        Consumer<KeyValue<Object>> consumer) {

      String off = offset == null ? "" : ((RawOffset) offset).getOffset();
      String start = getURI().getPath() + "/" + key + "#" + prefix;
      int count = 0;
      for (Map.Entry<String, Pair<Long, byte[]>> e : data.tailMap(start).entrySet()) {
        if (e.getKey().startsWith(start)) {
          int hash = e.getKey().lastIndexOf("#");
          String attribute = e.getKey().substring(hash + 1);
          if (attribute.equals(off) || e.getValue().getSecond() == null) {
            continue;
          }
          Optional<AttributeDescriptor<Object>> attr;
          attr = getEntityDescriptor().findAttribute(attribute, true);
          if (attr.isPresent()) {
            consumer.accept(KeyValue.of(
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
        String key, String attribute, AttributeDescriptor<T> desc) {

      return reader.get(key, attribute, desc);
    }

    @Override
    public <T> void scanWildcard(
        String key, AttributeDescriptor<T> wildcard,
        RandomOffset offset,
        int limit,
        Consumer<KeyValue<T>> consumer) {

      reader.scanWildcard(key, wildcard, offset, limit, consumer);
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
      validateAttributeInEntity(getEntityDescriptor(), data);
    }

    @Override
    public URI getURI() {
      return writer.getURI();
    }

    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset,
        int limit, Consumer<KeyValue<?>> consumer) {

      reader.scanWildcardAll(key, offset, limit, consumer);
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
        entityDesc, uri, uriObservers);
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

    };
  }

  // validate that entity contains required attribute
  private static void validateAttributeInEntity(
      EntityDescriptor entity,
      StreamElement ingest) {

    entity
        .findAttribute(ingest.getAttribute(), true)
        .orElseThrow(() -> new IllegalStateException(
            "Entity " + entity + " is missing attribute "
                + ingest.getAttribute()));
  }



}
