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
package cz.o2.proxima.repository;

import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.batch.BatchLogObserver;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.storage.randomaccess.RandomOffset;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.view.PartitionedCachedView;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy attribute family applying transformations of attributes
 * to and from private space to public space.
 */
@Slf4j
class AttributeFamilyProxyDescriptor extends AttributeFamilyDescriptor {

  AttributeFamilyProxyDescriptor(
      AttributeProxyDescriptorImpl<?> targetAttribute,
      AttributeFamilyDescriptor targetFamilyRead,
      AttributeFamilyDescriptor targetFamilyWrite) {

    super(
        "proxy::" + targetAttribute.toAttributePrefix(false) + "::" + targetFamilyRead.getName(),
        targetFamilyRead.getType(),
        Arrays.asList(targetAttribute), getWriter(targetAttribute, targetFamilyWrite),
        getCommitLogReader(targetAttribute, targetFamilyRead),
        getBatchObservable(targetAttribute, targetFamilyRead),
        getRandomAccess(targetAttribute, targetFamilyRead),
        getPartitionedView(targetAttribute, targetFamilyRead),
        getPartitionedCachedView(targetAttribute, targetFamilyRead),
        targetFamilyRead.getType() == StorageType.PRIMARY
            ? targetFamilyRead.getAccess()
            : AccessType.or(targetFamilyRead.getAccess(), AccessType.from("read-only")),
        targetFamilyRead.getFilter(),
        null);
  }

  private static OnlineAttributeWriter getWriter(
      AttributeProxyDescriptorImpl<?> targetAttribute,
      AttributeFamilyDescriptor targetFamily) {

    Optional<AttributeWriterBase> w = targetFamily.getWriter();
    if (!w.isPresent() || !(w.get() instanceof OnlineAttributeWriter)) {
      return null;
    }
    OnlineAttributeWriter writer = w.get().online();
    final URI uri = getProxyURI(writer.getURI(), targetFamily);
    return new OnlineAttributeWriter() {

      @Override
      public void rollback() {
        writer.rollback();
      }

      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        writer.write(
            transformToRaw(data, targetAttribute),
            statusCallback);
      }

      @Override
      public URI getURI() {
        return uri;
      }

    };
  }

  private static CommitLogReader getCommitLogReader(
      AttributeProxyDescriptorImpl<?> targetAttribute,
      AttributeFamilyDescriptor targetFamily) {

    Optional<CommitLogReader> target = targetFamily.getCommitLogReader();
    if (!target.isPresent()) {
      return null;
    }
    CommitLogReader reader = target.get();
    return new CommitLogReader() {

      @Override
      public URI getURI() {
        return reader.getURI();
      }

      @Override
      public List<Partition> getPartitions() {
        return reader.getPartitions();
      }

      @Override
      public ObserveHandle observe(
          String name,
          Position position, LogObserver observer) {

        return reader.observe(
            name, position, wrapTransformed(targetAttribute, observer));
      }

      @Override
      public ObserveHandle observePartitions(
          String name,
          Collection<Partition> partitions, Position position,
          boolean stopAtCurrent, LogObserver observer) {

        return reader.observePartitions(
            name, partitions, position, stopAtCurrent,
            wrapTransformed(targetAttribute, observer));
      }

      @Override
      public ObserveHandle observePartitions(
          Collection<Partition> partitions, Position position,
          boolean stopAtCurrent, LogObserver observer) {

        return reader.observePartitions(
            partitions, position, stopAtCurrent,
            wrapTransformed(targetAttribute, observer));
      }

      @Override
      public ObserveHandle observeBulk(
          String name, Position position, boolean stopAtCurrent,
          BulkLogObserver observer) {

        return reader.observeBulk(
            name, position, stopAtCurrent, wrapTransformed(targetAttribute, observer));
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public ObserveHandle observeBulkPartitions(
          Collection<Partition> partitions,
          Position position,
          boolean stopAtCurrent,
          BulkLogObserver observer) {

        return reader.observeBulkPartitions(
            partitions, position, stopAtCurrent, wrapTransformed(targetAttribute, observer));
      }

      @Override
      public ObserveHandle observeBulkPartitions(
          String name,
          Collection<Partition> partitions,
          Position position,
          boolean stopAtCurrent,
          BulkLogObserver observer) {

        return reader.observeBulkPartitions(
            name, partitions, position, stopAtCurrent, wrapTransformed(targetAttribute, observer));
      }

      @Override
      public ObserveHandle observeBulkOffsets(
          Collection<Offset> offsets, BulkLogObserver observer) {

        return reader.observeBulkOffsets(
            offsets, wrapTransformed(targetAttribute, observer));
      }

    };
  }


  private static BatchLogObservable getBatchObservable(
      AttributeProxyDescriptorImpl targetDesc,
      AttributeFamilyDescriptor targetFamily) {

    Optional<BatchLogObservable> target = targetFamily.getBatchObservable();
    if (!target.isPresent()) {
      return null;
    }
    BatchLogObservable reader = target.get();
    return new BatchLogObservable() {

      @Override
      public List<Partition> getPartitions(long startStamp, long endStamp) {
        return reader.getPartitions(startStamp, endStamp);
      }

      @Override
      public void observe(
          List<Partition> partitions,
          List<AttributeDescriptor<?>> attributes,
          BatchLogObserver observer) {

        reader.observe(
            partitions, attributes, wrapTransformed(targetDesc, observer));
      }

    };
  }

  @SuppressWarnings("unchecked")
  private static RandomAccessReader getRandomAccess(
      AttributeProxyDescriptorImpl targetAttribute,
      AttributeFamilyDescriptor targetFamily) {

    Optional<RandomAccessReader> target = targetFamily.getRandomAccessReader();
    if (!target.isPresent()) {
      return null;
    }
    RandomAccessReader reader = target.get();
    return new RandomAccessReader() {

      @Override
      public RandomOffset fetchOffset(
          RandomAccessReader.Listing type, String key) {

        if (type == Listing.ATTRIBUTE) {
          return reader.fetchOffset(
              type, targetAttribute.getReadTransform().fromProxy(key));
        }
        return reader.fetchOffset(type, key);
      }

      @Override
      public <T> Optional<KeyValue<T>> get(
          String key, String attribute, AttributeDescriptor<T> desc) {

        ProxyTransform transform = targetAttribute.getReadTransform();
        return reader.get(key, transform.fromProxy(attribute), desc)
            .map(kv -> transformToProxy(kv, targetAttribute));
      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> void scanWildcard(
          String key, AttributeDescriptor<T> wildcard,
          RandomOffset offset, int limit, Consumer<KeyValue<T>> consumer) {

        if (!targetAttribute.isWildcard()) {
          throw new IllegalArgumentException(
              "Proxy target is not wildcard attribute!");
        }
        reader.scanWildcard(key, targetAttribute.getReadTarget(), offset, limit,
            kv -> consumer.accept((KeyValue) transformToProxy(kv, targetAttribute)));
      }

      @Override
      public void scanWildcardAll(
          String key, RandomOffset offset,
          int limit, Consumer<KeyValue<?>> consumer) {

        reader.scanWildcardAll(
            key, offset, limit,
            kv -> consumer.accept(transformToProxy(kv, targetAttribute)));
      }

      @Override
      public void listEntities(
          RandomOffset offset, int limit,
          Consumer<Pair<RandomOffset, String>> consumer) {

        reader.listEntities(offset, limit, consumer);
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public EntityDescriptor getEntityDescriptor() {
        return reader.getEntityDescriptor();
      }

    };
  }

  @SuppressWarnings("unchecked")
  private static PartitionedView getPartitionedView(
      AttributeProxyDescriptorImpl targetAttribute,
      AttributeFamilyDescriptor targetFamily) {

    Optional<PartitionedView> target = targetFamily.getPartitionedView();
    if (!target.isPresent()) {
      return null;
    }
    PartitionedView view = target.get();
    return new PartitionedView() {

      @Override
      public List<Partition> getPartitions() {
        return view.getPartitions();
      }

      @Override
      public <T> Dataset<T> observePartitions(
          Flow flow,
          Collection<Partition> partitions,
          PartitionedLogObserver<T> observer) {

        return view.observePartitions(flow, partitions, wrapTransformed(
            targetAttribute, observer));
      }

      @Override
      public <T> Dataset<T> observe(
          Flow flow, String name, PartitionedLogObserver<T> observer) {

        return view.observe(flow, name, wrapTransformed(
            targetAttribute, observer));
      }

      @Override
      public EntityDescriptor getEntityDescriptor() {
        return view.getEntityDescriptor();
      }

    };
  }

  private static <T> PartitionedCachedView getPartitionedCachedView(
      AttributeProxyDescriptorImpl<T> targetAttribute,
      AttributeFamilyDescriptor targetFamily) {

    Optional<PartitionedCachedView> target = targetFamily.getPartitionedCachedView();
    if (!target.isPresent()) {
      return null;
    }
    PartitionedCachedView view = target.get();
    final URI uri = getProxyURI(view.getURI(), targetFamily);

    return new PartitionedCachedView() {

      @Override
      public void assign(
          Collection<Partition> partitions,
          BiConsumer<StreamElement, Pair<Long, Object>> updateCallback) {

        view.assign(partitions, updateCallback);
      }

      @Override
      public Collection<Partition> getAssigned() {
        return view.getAssigned();
      }

      @Override
      public RandomOffset fetchOffset(
          RandomAccessReader.Listing type, String key) {

        if (type == Listing.ATTRIBUTE) {
          return view.fetchOffset(
              type, targetAttribute.getReadTransform().fromProxy(key));
        }
        return view.fetchOffset(type, key);
      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> Optional<KeyValue<T>> get(
          String key, String attribute, AttributeDescriptor<T> desc) {

        ProxyTransform transform = targetAttribute.getReadTransform();
        return view.get(key, transform.fromProxy(attribute), desc)
            .map(kv -> (KeyValue) transformToProxy(kv, targetAttribute));
      }

      @Override
      public void scanWildcardAll(
          String key, RandomOffset offset, int limit,
          Consumer<KeyValue<?>> consumer) {

        view.scanWildcardAll(key, offset, limit, kv -> {
          consumer.accept(transformToProxy(kv, targetAttribute));
        });

      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> void scanWildcard(
          String key, AttributeDescriptor<T> wildcard,
          RandomOffset offset, int limit, Consumer<KeyValue<T>> consumer) {

        if (!targetAttribute.isWildcard()) {
          throw new IllegalArgumentException(
              "Proxy target is not wildcard attribute!");
        }
        view.scanWildcard(key, targetAttribute.getReadTarget(), offset, limit,
            kv -> consumer.accept((KeyValue) transformToProxy(kv, targetAttribute)));
      }

      @Override
      public void listEntities(
          RandomOffset offset, int limit,
          Consumer<Pair<RandomOffset, String>> consumer) {

        view.listEntities(offset, limit, consumer);
      }

      @Override
      public void close() throws IOException {
        view.close();
      }

      @Override
      public EntityDescriptor getEntityDescriptor() {
        return view.getEntityDescriptor();
      }

      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        view.write(transformToRaw(data, targetAttribute), statusCallback);
      }

      @Override
      public URI getURI() {
        return uri;
      }

    };
  }

  @SuppressWarnings("unchecked")
  private static LogObserver wrapTransformed(
      AttributeProxyDescriptorImpl proxy,
      LogObserver observer) {

    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, LogObserver.OffsetCommitter confirm) {
        return observer.onNext(
            transformToProxy(ingest, proxy), confirm);
      }

      @Override
      public void onCompleted() {
        observer.onCompleted();
      }

      @Override
      public void onCancelled() {
        observer.onCancelled();
      }

      @Override
      public boolean onError(Throwable error) {
        return observer.onError(error);
      }

    };
  }

  static BulkLogObserver wrapTransformed(
      AttributeProxyDescriptorImpl desc,
      BulkLogObserver observer) {

    return new BulkLogObserver() {

      @Override
      public void onCompleted() {
        observer.onCompleted();
      }

      @Override
      public boolean onError(Throwable error) {
        return observer.onError(error);
      }

      @Override
      public boolean onNext(
          StreamElement ingest,
          Partition partition,
          BulkLogObserver.OffsetCommitter confirm) {

        return observer.onNext(
            transformToProxy(ingest, desc), partition, confirm);
      }

      @Override
      public void onRestart(List<Offset> offsets) {
        observer.onRestart(offsets);
      }

      @Override
      public void onCancelled() {
        observer.onCancelled();
      }

    };
  }

  static BatchLogObserver wrapTransformed(
      AttributeProxyDescriptorImpl desc,
      BatchLogObserver observer) {

    return new BatchLogObserver() {

      @Override
      public boolean onNext(
          StreamElement ingest,
          Partition partition) {

        return observer.onNext(transformToProxy(ingest, desc), partition);
      }

      @Override
      public void onCompleted() {
        observer.onCompleted();
      }

      @Override
      public void onError(Throwable error) {
        observer.onError(error);
      }

    };
  }

  private static <T> PartitionedLogObserver<T> wrapTransformed(
      AttributeProxyDescriptorImpl<T> target, PartitionedLogObserver<T> observer) {

    return new PartitionedLogObserver<T>() {

      @Override
      public void onRepartition(Collection<Partition> assigned) {
        observer.onRepartition(assigned);
      }

      @Override
      public boolean onNext(
          StreamElement ingest,
          PartitionedLogObserver.ConfirmCallback confirm,
          Partition partition,
          Consumer<T> collector) {

        return observer.onNext(transformToProxy(ingest, target),
            confirm, partition, collector);
      }

      @Override
      public void onCompleted() {
        observer.onCompleted();
      }

      @Override
      public boolean onError(Throwable error) {
        return observer.onError(error);
      }

    };
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToRaw(
      StreamElement data,
      AttributeProxyDescriptorImpl targetDesc) {

    return transform(data,
        targetDesc.getWriteTarget(),
        targetDesc.getWriteTransform()::fromProxy);
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToProxy(
      StreamElement data,
      AttributeProxyDescriptorImpl targetDesc) {

    return transform(data, targetDesc, targetDesc.getReadTransform()::toProxy);
  }

  @SuppressWarnings("unchecked")
  private static <T> KeyValue<T> transformToProxy(
      KeyValue<T> kv,
      AttributeProxyDescriptorImpl targetDesc) {

    return KeyValue.of(
        kv.getEntityDescriptor(),
        targetDesc, kv.getKey(),
        targetDesc.getReadTransform().toProxy(kv.getAttribute()),
        kv.getOffset(), kv.getValueBytes());
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transform(
      StreamElement data,
      AttributeDescriptor target,
      Function<String, String> transform) {

    if (data.isDelete()) {
      if (data.isDeleteWildcard()) {
        return StreamElement.deleteWildcard(
            data.getEntityDescriptor(),
            target, data.getUuid(), data.getKey(), data.getStamp());
      } else {
        return StreamElement.delete(
            data.getEntityDescriptor(), target,
            data.getUuid(), data.getKey(),
            transform.apply(data.getAttribute()),
            data.getStamp());
      }
    }
    return StreamElement.update(data.getEntityDescriptor(),
        target, data.getUuid(), data.getKey(),
        transform.apply(data.getAttribute()),
        data.getStamp(), data.getValue());
  }

  private static URI getProxyURI(
      URI uri, AttributeFamilyDescriptor targetFamily) {
    try {
      return new URI(String.format(
          "proxy-%s.%s", targetFamily.getName(), uri.toString())
          .replace("_", "-"));
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

}
