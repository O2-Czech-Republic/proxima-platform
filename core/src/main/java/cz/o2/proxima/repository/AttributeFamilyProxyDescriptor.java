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
import cz.o2.proxima.storage.commitlog.Cancellable;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Proxy attribute family applying transformations of attributes
 * to and from private space to public space.
 */
class AttributeFamilyProxyDescriptor extends AttributeFamilyDescriptor {

  AttributeFamilyProxyDescriptor(
      AttributeProxyDescriptorImpl<?> targetAttribute,
      AttributeFamilyDescriptor targetFamily) {
    super(
        "proxy::" + targetAttribute.getName() + "::" + targetFamily.getName(),
        targetFamily.getType(),
        Arrays.asList(targetAttribute), getWriter(targetAttribute, targetFamily),
        getCommitLogReader(targetAttribute, targetFamily),
        getBatchObservable(targetAttribute, targetFamily),
        getRandomAccess(targetAttribute, targetFamily),
        getPartitionedView(targetAttribute, targetFamily),
        targetFamily.getType() == StorageType.PRIMARY
            ? targetFamily.getAccess()
            : AccessType.or(targetFamily.getAccess(), AccessType.from("read-only")),
        targetFamily.getFilter());
  }

  private static OnlineAttributeWriter getWriter(
      AttributeProxyDescriptorImpl<?> targetAttribute,
      AttributeFamilyDescriptor targetFamily) {

    Optional<AttributeWriterBase> w = targetFamily.getWriter();
    if (!w.isPresent() || !(w.get() instanceof OnlineAttributeWriter)) {
      return null;
    }
    OnlineAttributeWriter writer = w.get().online();
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
        return writer.getURI();
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
      public Cancellable observe(
          String name,
          CommitLogReader.Position position, LogObserver observer) {

        return reader.observe(
            name, position, wrapTransformed(targetAttribute, observer));
      }

      @Override
      public Cancellable observePartitions(
          Collection<Partition> partitions, CommitLogReader.Position position,
          boolean stopAtCurrent, LogObserver observer) {

        return reader.observePartitions(
            partitions, position, stopAtCurrent,
            wrapTransformed(targetAttribute, observer));
      }

      @Override
      public Cancellable observeBulk(
          String name, CommitLogReader.Position position,
          BulkLogObserver observer) {

        return reader.observeBulk(name, position, wrapTransformed(observer));
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

    };
  }


  private static BatchLogObservable getBatchObservable(
      AttributeProxyDescriptorImpl<?> targetAttribute,
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

        reader.observe(partitions, attributes, wrapTransformed(observer));
      }

    };
  }

  private static RandomAccessReader getRandomAccess(
      AttributeProxyDescriptorImpl<?> targetAttribute,
      AttributeFamilyDescriptor targetFamily) {

    Optional<RandomAccessReader> target = targetFamily.getRandomAccessReader();
    if (!target.isPresent()) {
      return null;
    }
    RandomAccessReader reader = target.get();
    return new RandomAccessReader() {

      @Override
      public RandomAccessReader.Offset fetchOffset(
          RandomAccessReader.Listing type, String key) {

        if (type == Listing.ATTRIBUTE) {
          return reader.fetchOffset(
              type, targetAttribute.getTransform().fromProxy(key));
        }
        return reader.fetchOffset(type, key);
      }

      @Override
      public Optional<KeyValue<?>> get(
          String key, String attribute, AttributeDescriptor<?> desc) {

        ProxyTransform transform = targetAttribute.getTransform();
        return reader.get(key, transform.fromProxy(attribute), desc)
            .map(kv -> transformToProxy(kv, targetAttribute));
      }

      @Override
      public void scanWildcard(
          String key, AttributeDescriptor<?> wildcard,
          RandomAccessReader.Offset offset, int limit, Consumer<KeyValue<?>> consumer) {

        if (!targetAttribute.isWildcard()) {
          throw new IllegalArgumentException(
              "Proxy target is not wildcard attribute!");
        }
        reader.scanWildcard(key, targetAttribute.getTarget(), offset, limit, kv -> {
          consumer.accept(transformToProxy(kv, targetAttribute));
        });
      }

      @Override
      public void listEntities(
          RandomAccessReader.Offset offset, int limit,
          Consumer<Pair<RandomAccessReader.Offset, String>> consumer) {

      }

      @Override
      public void close() throws IOException {
      }

    };
  }

  private static PartitionedView getPartitionedView(
      AttributeProxyDescriptorImpl<?> targetAttribute,
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

    };
  }

  private static LogObserver wrapTransformed(
      AttributeProxyDescriptorImpl<?> proxy,
      LogObserver observer) {

    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, LogObserver.ConfirmCallback confirm) {
        return observer.onNext(
            transformToProxy(ingest, proxy), confirm);
      }

      @Override
      public boolean onNext(
          StreamElement ingest,
          Partition partition,
          LogObserver.ConfirmCallback confirm) {

        return observer.onNext(
            transformToProxy(ingest, proxy),
            partition, confirm);
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

      @Override
      public void close() throws Exception {
        observer.close();
      }

    };
  }

  static BulkLogObserver wrapTransformed(BulkLogObserver observer) {
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
          BulkLogObserver.BulkCommitter confirm) {

        return observer.onNext(ingest, partition, confirm);
      }

      @Override
      public void onRestart() {
        observer.onRestart();
      }

      @Override
      public void onCancelled() {
        observer.onCancelled();
      }

      @Override
      public void close() throws Exception {
        observer.close();
      }

    };
  }


  static BatchLogObserver wrapTransformed(BatchLogObserver observer) {
    return new BatchLogObserver() {

      @Override
      public boolean onNext(
          StreamElement ingest,
          Partition partition) {

        return observer.onNext(ingest, partition);
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
      AttributeProxyDescriptorImpl<?> target, PartitionedLogObserver<T> observer) {

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
          PartitionedLogObserver.Consumer<T> collector) {

        return observer.onNext(transformToProxy(ingest, target),
            confirm, partition, collector);
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

  private static StreamElement transformToRaw(
      StreamElement data,
      AttributeProxyDescriptorImpl<?> targetDesc) {

    return transform(data,
        targetDesc.getTarget(),
        targetDesc.getTransform()::fromProxy);
  }

  private static StreamElement transformToProxy(
      StreamElement data,
      AttributeProxyDescriptorImpl<?> targetDesc) {

    return transform(data, targetDesc, targetDesc.getTransform()::toProxy);
  }

  private static KeyValue<?> transformToProxy(
      KeyValue<?> kv,
      AttributeProxyDescriptorImpl<?> targetDesc) {

    return KeyValue.of(
        kv.getEntityDescriptor(),
        targetDesc, kv.getKey(),
        targetDesc.getTransform().toProxy(kv.getAttribute()),
        kv.getOffset(), kv.getValueBytes());
  }

  private static StreamElement transform(
      StreamElement data,
      AttributeDescriptor<?> target,
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




}
