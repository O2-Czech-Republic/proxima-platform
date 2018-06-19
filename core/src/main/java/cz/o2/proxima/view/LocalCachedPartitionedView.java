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
package cz.o2.proxima.view;

import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomOffset;
import cz.o2.proxima.storage.randomaccess.RawOffset;
import cz.o2.proxima.util.Pair;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A transformation view from {@link PartitionedView} to {@link PartitionedCachedView}.
 */
@Slf4j
public class LocalCachedPartitionedView implements PartitionedCachedView {

  private final KafkaLogReader reader;
  private final EntityDescriptor entity;
  private final Factory<ExecutorService> executorFactory;

  /**
   * Writer to persist data to.
   */
  private final OnlineAttributeWriter writer;

  /**
   * Cache for data in memory.
   * Entity key -&gt; Attribute -&gt; (timestamp, value)
   */
  private final Map<String, NavigableMap<String, Pair<Long, Object>>> cache =
      Collections.synchronizedMap(new HashMap<>());

  /**
   * Handle of the observation thread (if any running).
   */
  private final AtomicReference<ObserveHandle> handle = new AtomicReference<>();

  private BiConsumer<StreamElement, Pair<Long, Object>> updateCallback = (e, old) -> { };

  private transient ExecutorService executor;

  public LocalCachedPartitionedView(
      EntityDescriptor entity, CommitLogReader reader, OnlineAttributeWriter writer) {

    this.reader = reader;
    this.entity = entity;
    this.writer = writer;
  }

  @SuppressWarnings("unchecked")
  private void onCache(StreamElement ingest, boolean overwrite) throws Exception {

    final Optional<Object> parsed = ingest.isDelete()
        ? Optional.empty()
        : ingest.getParsed();
    if (ingest.isDelete() || parsed.isPresent()) {
      final String attrName;
      if (ingest.isDeleteWildcard()) {
        attrName = ingest.getAttributeDescriptor().toAttributePrefix();
      } else {
        attrName = ingest.getAttribute();
      }
      AtomicBoolean updated = new AtomicBoolean();
      AtomicReference<Pair<Long, Object>> oldVal = new AtomicReference<>();
      cache.compute(ingest.getKey(), (key, m) -> {
        if (m == null) {
          m = new TreeMap<>();
        }
        final Map<String, Pair<Long, Object>> attrMap = m;
        m.compute(attrName, (k, c) -> {
          boolean wildcardDeleted = false;
          if (ingest.getAttributeDescriptor().isWildcard()) {
            Pair<Long, Object> wildcardRec;
            wildcardRec = attrMap.get(ingest.getAttributeDescriptor().toAttributePrefix());
            if (wildcardRec != null) {
              c = c == null
                  ? wildcardRec
                  : c.getFirst() < wildcardRec.getFirst()
                      ? wildcardRec : c;
            }
          }
          if (c == null || c.getFirst() < ingest.getStamp()
              || overwrite && c.getFirst() == ingest.getStamp()) {
            log.debug("Caching update {}", ingest);
            oldVal.set(c);
            updated.set(true);
            return Pair.of(
                ingest.getStamp(),
                ingest.isDelete() ? null : parsed.get());
          }
          log.debug("Ignoring old arrival ingest {}, current {}", ingest, c);
          return c;
        });
        return m;
      });
      if (updated.get()) {
        updateCallback.accept(ingest, oldVal.get());
      }
    }
  }


  @Override
  public void assign(
      Collection<Partition> partitions,
      BiConsumer<StreamElement, Pair<Long, Object>> updateCallback) {

    close();
    this.updateCallback = Objects.requireNonNull(updateCallback);
    if (executor == null) {
      executor = executorFactory.apply();
    }
    CountDownLatch latch = new CountDownLatch(1);
    AtomicLong prefetchedCount = new AtomicLong();

    BulkLogObserver prefetchObserver = new BulkLogObserver() {
      @Override
      public boolean onNext(
          StreamElement ingest,
          Partition partition,
          BulkLogObserver.OffsetCommitter committer) {

        try {
          prefetchedCount.incrementAndGet();
          onCache(ingest, true /* use tha latest value stored in the topic */);
          committer.confirm();
          return true;
        } catch (Throwable ex) {
          committer.fail(ex);
          return false;
        }
      }

      @Override
      public boolean onError(Throwable error) {
        log.error("Failed to prefetch data", error);
        assign(partitions);
        return false;
      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }

    };
    KafkaLogObserver observer = new KafkaLogObserver() {

      @Override
      public boolean onNext(
          StreamElement ingest,
          KafkaLogObserver.ConfirmCallback confirm,
          Partition partition) {

        try {
          onCache(
              ingest,
              false /* don't overwrite already stored data at the same stamp */);
          confirm.apply(true, null);
          return true;
        } catch (Throwable err) {
          confirm.apply(false, err);
          return false;
        }
      }

      @Override
      public boolean onError(Throwable error) {
        log.error("Error in caching data. Restarting consumption", error);
        assign(partitions);
        return false;
      }

      @Override
      public void onRepartition(Collection<Partition> assigned) {
        // should not happen
      }

    };
    try {
      // prefetch the data
      log.info(
          "Starting prefetching old topic data for partitions {} with preUpdate {}",
          partitions.stream().map(Partition::getId).collect(Collectors.toList()),
          updateCallback);
      ObserveHandle h = reader.processConsumerBulk(
          null, partitions.stream()
              .map(p -> new TopicOffset(p.getId(), -1L)).collect(Collectors.toList()),
          Position.OLDEST, true, false, prefetchObserver, executor);
      latch.await();
      log.info(
          "Finished prefetching of data after {} records. Starting consumption of updates.",
          prefetchedCount.get());
      List<Offset> offsets = h.getCommittedOffsets();
      // continue the processing
      handle.set(reader.processConsumer(
          null, offsets,
          Position.CURRENT, false, false, observer, executor));
      handle.get().waitUntilReady();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Collection<Partition> getAssigned() {
    if (handle.get() != null) {
      return handle.get().getCommittedOffsets()
          .stream().map(Offset::getPartition)
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  @Override
  public RandomOffset fetchOffset(Listing type, String key) {
    return new RawOffset(key);
  }

  @Override
  public <T> Optional<KeyValue<T>> get(
      String key,
      String attribute,
      AttributeDescriptor<T> desc) {

    Optional<NavigableMap<String, Pair<Long, Object>>> keyMap;
    keyMap = Optional.ofNullable(cache.get(key));
    if (keyMap.isPresent()) {
      NavigableMap<String, Pair<Long, Object>> m = keyMap.get();
      return Optional.ofNullable(m.get(attribute))
          .flatMap(p -> {
            if (desc.isWildcard()) {
              // verify if we don't have later wildcard record
              Pair<Long, Object> wildcard = m.get(desc.toAttributePrefix());
              if (wildcard == null || wildcard.getFirst() < p.getFirst()) {
                return Optional.ofNullable(toKv(key, attribute, p));
              }
              return Optional.ofNullable(toKv(key, attribute, wildcard));
            }
            return Optional.ofNullable(toKv(key, attribute, p));
          });
    }
    return Optional.empty();
  }

  @Override
  public void scanWildcardAll(
      String key, RandomOffset offset,
      int limit, Consumer<KeyValue<?>> consumer) {

    String off = offset == null ? "" : ((RawOffset) offset).getOffset();
    scanWildcardPrefix(key, "", off, limit, consumer);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      RandomOffset offset,
      int limit,
      Consumer<KeyValue<T>> consumer) {

    String off = offset == null
        ? wildcard.toAttributePrefix()
        : ((RawOffset) offset).getOffset();
    scanWildcardPrefix(
        key, wildcard.toAttributePrefix(), off, limit, (Consumer) consumer);
  }

  @SuppressWarnings("unchecked")
  private void scanWildcardPrefix(
      String key,
      String prefix,
      String offset,
      int limit,
      Consumer<KeyValue<?>> consumer) {

    NavigableMap<String, Pair<Long, Object>> m = cache.get(key);
    log.debug(
        "Scanning prefix {} of key {} with map.size {}",
        prefix, key, m == null ? -1 : m.size());
    if (m != null) {
      String prefixName = null;
      Pair<Long, Object> prefixRecord = null;
      SortedMap<String, Pair<Long, Object>> tail = m.tailMap(offset);
      for (Map.Entry<String, Pair<Long, Object>> e : tail.entrySet()) {
        if (e.getKey().compareTo(offset) <= 0) {
          continue;
        }
        if (limit == 0 || !e.getKey().startsWith(prefix)) {
          break;
        }
        log.trace("Scanned entry {} with prefix '{}'", e, prefix);
        if (e.getKey().length() > prefix.length()) {
          Optional<AttributeDescriptor<Object>> attr;
          attr = getEntityDescriptor().findAttribute(e.getKey());
          if (attr.isPresent()) {
            if (attr.get().isWildcard()) {
              if (prefixName == null || !attr.get().getName().startsWith(prefixName)) {
                // need to fetch new prefixName
                prefixName = attr.get().toAttributePrefix();
                prefixRecord = m.get(prefixName);
                log.trace("Fetched prefixRecord {} for attr", prefixRecord, attr);
              }
            }
            KeyValue kv = toKv(
                key, e.getKey(),
                prefixRecord == null || prefixRecord.getFirst() < e.getValue().getFirst()
                    ? e.getValue()
                    : prefixRecord);
            if (kv != null) {
              limit--;
              consumer.accept(kv);
            }
            log.trace("Scanned KeyValue {} for key {}", kv, key);
          } else {
            log.warn("Unknown attribute {} in {}", e.getKey(), getEntityDescriptor());
          }
        }
      }
    }
  }

  @Override
  public void listEntities(
      RandomOffset offset,
      int limit,
      Consumer<Pair<RandomOffset, String>> consumer) {

    throw new UnsupportedOperationException(
        "Don't use this view for listing entities");
  }

  @Override
  public void close() {
    Optional.ofNullable(handle.getAndSet(null)).ifPresent(ObserveHandle::cancel);
    cache.clear();
  }

  @SuppressWarnings("unchecked")
  private @Nullable <T> KeyValue<T> toKv(
      String key, String attribute, @Nullable Pair<Long, Object> p) {

    Optional<AttributeDescriptor<Object>> attrDesc = entity.findAttribute(attribute);
    if (attrDesc.isPresent()) {
      return toKv(key, attribute, attrDesc.get(), p);
    }
    log.warn("Missing attribute {} in entity {}", attribute, entity);
    return null;
  }

  @SuppressWarnings("unchecked")
  private @Nullable <T> KeyValue<T> toKv(
      String key, String attribute,
      AttributeDescriptor<?> attr, @Nullable Pair<Long, Object> p) {

    if (p == null || p.getSecond() == null) {
      return null;
    }
    return (KeyValue) KeyValue.of(
        entity, (AttributeDescriptor) attr, key, attribute,
        new RawOffset(attribute), (T) p.getSecond(), null,
        p.getFirst());
  }

  @Override
  public EntityDescriptor getEntityDescriptor() {
    return entity;
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    try {
      onCache(data, true);
      writer.write(data, statusCallback);
    } catch (Exception ex) {
      statusCallback.commit(false, ex);
    }
  }

  @Override
  public URI getURI() {
    return reader.getURI();
  }

}
