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
package cz.o2.proxima.storage.kafka;

import cz.o2.proxima.functional.Consumer;
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
import cz.o2.proxima.view.PartitionedCachedView;
import cz.o2.proxima.view.PartitionedView;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A transformation view from {@link PartitionedView} to {@link PartitionedCachedView}.
 */
@Slf4j
class LocalCachedPartitionedView implements PartitionedCachedView {

  private final KafkaLogReader reader;
  private final EntityDescriptor entity;
  private final ExecutorService executor;

  /**
   * Writer to persist data to.
   */
  private final OnlineAttributeWriter writer;

  /**
   * Cache for data in memory.
   * Entity key -> Attribute -> (timestamp, value)
   */
  private final Map<String, NavigableMap<String, Pair<Long, Object>>> cache =
      Collections.synchronizedMap(new HashMap<>());

  /**
   * Handle of the observation thread (if any running).
   */
  private final AtomicReference<ObserveHandle> handle = new AtomicReference<>();

  public LocalCachedPartitionedView(
      KafkaLogReader reader, OnlineAttributeWriter writer,
      ExecutorService executor) {

    this.reader = reader;
    this.entity = reader.getEntityDescriptor();
    this.executor = executor;
    this.writer = writer;
  }

  private boolean onCache(
      StreamElement ingest, CommitCallback confirm) {

    Optional<Object> parsed = ingest.getParsed();
    try {
      if (parsed.isPresent()) {
        cache.compute(ingest.getKey(), (key, m) -> {
          if (m == null) {
            m = new TreeMap<>();
          }
          m.compute(ingest.getAttribute(), (k, c) -> {
            if (c == null || c.getFirst() < ingest.getStamp()) {
              return Pair.of(ingest.getStamp(), parsed.get());
            }
            log.debug("Ignoring old arrival ingest {}, current {}", ingest, c);
            return c;
          });
          return m;
        });
      }
      confirm.commit(true, null);
      return true;
    } catch (Throwable err) {
      confirm.commit(false, err);
      return false;
    }
  }


  @Override
  public void assign(Collection<Partition> partitions) {
    close();
    CountDownLatch latch = new CountDownLatch(1);

    BulkLogObserver prefetchObserver = new BulkLogObserver() {
      @Override
      public boolean onNext(
          StreamElement ingest,
          Partition partition,
          BulkLogObserver.OffsetCommitter committer) {

        return onCache(ingest, committer::commit);
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

        return onCache(ingest, confirm::apply);
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
      ObserveHandle h = reader.processConsumerBulk(
          null, partitions.stream()
              .map(p -> new TopicOffset(p.getId(), -1L)).collect(Collectors.toList()),
          Position.OLDEST, true, false, prefetchObserver, executor);
      latch.await();
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

    return Optional.ofNullable(cache.get(key))
        .flatMap(m -> Optional.ofNullable(toKv(key, attribute, m.get(attribute))));
  }

  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      RandomOffset offset,
      int limit,
      Consumer<KeyValue<T>> consumer) {

    String off = offset == null ? "" : ((RawOffset) offset).getOffset();
    NavigableMap<String, Pair<Long, Object>> m = cache.get(key);
    String prefix = wildcard.toAttributePrefix();
    if (m != null) {
      SortedMap<String, Pair<Long, Object>> tail = m.tailMap(off);
      for (Map.Entry<String, Pair<Long, Object>> e : tail.entrySet()) {
        if (e.getKey().compareTo(off) > 0) {
          if (limit-- > 0 && e.getKey().startsWith(prefix)) {
            consumer.accept(toKv(key, e.getKey(), e.getValue()));
          } else {
            break;
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
    handle.getAndUpdate(h -> {
      if (h != null) {
        h.cancel();
      }
      return null;
    });
    cache.clear();
  }

  @SuppressWarnings("unchecked")
  private @Nullable <T> KeyValue<T> toKv(
      String key, String attribute, @Nullable Pair<Long, Object> o) {

    if (o == null || o.getSecond() == null) {
      return null;
    }
    Optional<AttributeDescriptor<Object>> attrDesc = entity.findAttribute(attribute);
    if (attrDesc.isPresent()) {
      return KeyValue.of(
          entity, (AttributeDescriptor) attrDesc.get(), key, attribute,
          new RawOffset(attribute), (T) o.getSecond(), null);
    }
    log.warn("Missing attribute {} in entity {}", attribute, entity);
    return null;
  }

  @Override
  public EntityDescriptor getEntityDescriptor() {
    return entity;
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    try {
      onCache(data, (succ, exc) -> {
        if (!succ) {
          throw new RuntimeException(exc);
        }
      });
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
