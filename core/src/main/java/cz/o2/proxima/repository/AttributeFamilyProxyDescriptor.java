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

import com.google.common.base.Preconditions;
import cz.o2.proxima.transform.ProxyTransform;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryFunction;
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
import cz.o2.proxima.storage.randomaccess.RawOffset;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.view.LocalCachedPartitionedView;
import cz.o2.proxima.view.PartitionedCachedView;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy attribute family applying transformations of attributes
 * to and from private space to public space.
 */
@Slf4j
class AttributeFamilyProxyDescriptor extends AttributeFamilyDescriptor {

  static class AttrLookup implements Serializable {

    @Getter
    private final List<AttributeProxyDescriptorImpl<?>> attrs;
    private final Map<String, AttributeProxyDescriptorImpl<?>> proxyNameToDesc;
    private final Map<String, List<AttributeProxyDescriptorImpl<?>>> readNameToDesc;
    private final String familyName;

    AttrLookup(
        String familyName,
        List<AttributeProxyDescriptorImpl<?>> attrs) {

      this.familyName = familyName;
      this.attrs = attrs;
      proxyNameToDesc = attrs
          .stream()
          .collect(Collectors.toMap(AttributeDescriptor::getName, Function.identity()));
      readNameToDesc = attrs
          .stream()
          .map(a -> Pair.of(a.getReadTarget().getName(), a))
          .collect(Collectors.groupingBy(
              Pair::getFirst,
              Collectors.mapping(Pair::getSecond, Collectors.toList())));
    }

    List<AttributeProxyDescriptorImpl<?>> lookupRead(String name) {
      List<AttributeProxyDescriptorImpl<?>> read = readNameToDesc.get(name);
      if (read == null) {
        log.debug(
            "Fallbacking to lookup of proxy attribute with name {} in family {}. ",
            name, familyName);
        try {
          return Arrays.asList(lookupProxy(name));
        } catch (Exception ex) {
          log.warn(
              "Error during lookup of {} in family {}."
                  + "This might indicate serious problem.",
              name, familyName, ex);
          return Collections.emptyList();
        }
      }
      return read;
    }

    AttributeProxyDescriptorImpl<?> lookupProxy(String name) {
      return lookup(proxyNameToDesc, name);
    }

    private <T> T lookup(Map<String, T> map, String name) {
      T result = map.get(name);
      if (result != null) {
        return result;
      }
      int index = name.lastIndexOf('$');
      if (index >= 0 && index < name.length() - 1) {
        String truncated = name.substring(index + 1);
        log.warn("Truncating name {} to {}", name, truncated);
        return lookup(map, truncated);
      }
      throw new IllegalStateException("Missing name " + name + " in " + map);
    }
  }

  static AttributeFamilyDescriptor of(
      EntityDescriptor entity,
      List<AttributeProxyDescriptorImpl<?>> attrs,
      AttributeFamilyDescriptor targetFamilyRead,
      AttributeFamilyDescriptor targetFamilyWrite) {

    return new AttributeFamilyProxyDescriptor(
        entity,
        new AttrLookup(getFamilyName(targetFamilyRead, targetFamilyWrite), attrs),
        targetFamilyRead, targetFamilyWrite);
  }


  @Getter
  private final AttributeFamilyDescriptor targetFamilyRead;

  @Getter
  private final AttributeFamilyDescriptor targetFamilyWrite;

  @SuppressWarnings("unchecked")
  private AttributeFamilyProxyDescriptor(
      EntityDescriptor entity,
      AttrLookup lookup,
      AttributeFamilyDescriptor targetFamilyRead,
      AttributeFamilyDescriptor targetFamilyWrite) {

    super(
        getFamilyName(targetFamilyRead, targetFamilyWrite),
        targetFamilyWrite.getType() == targetFamilyRead.getType()
            ? targetFamilyRead.getType()
            : StorageType.REPLICA,
        (Collection) lookup.getAttrs(),
        getWriter(lookup, targetFamilyWrite),
        getCommitLogReader(lookup, targetFamilyRead),
        getBatchObservable(lookup, targetFamilyRead),
        getRandomAccess(lookup, targetFamilyRead),
        getPartitionedView(lookup, targetFamilyRead),
        getPartitionedCachedView(entity, lookup, targetFamilyRead, targetFamilyWrite),
        targetFamilyWrite.getType()
            == StorageType.PRIMARY && targetFamilyRead.getType() == StorageType.PRIMARY
                ? targetFamilyRead.getAccess()
                : AccessType.or(
                    targetFamilyRead.getAccess(), AccessType.from("read-only")),
        targetFamilyRead.getFilter(),
        null);

    this.targetFamilyRead = targetFamilyRead;
    this.targetFamilyWrite = targetFamilyWrite;
  }

  private static String getFamilyName(
      AttributeFamilyDescriptor targetFamilyRead,
      AttributeFamilyDescriptor targetFamilyWrite) {

    return "proxy::" + targetFamilyRead.getName() + "::" + targetFamilyWrite.getName();
  }

  private static OnlineAttributeWriter getWriter(
      AttrLookup lookup,
      AttributeFamilyDescriptor targetFamily) {

    Optional<AttributeWriterBase> w = targetFamily.getWriter();
    if (!w.isPresent() || !(w.get() instanceof OnlineAttributeWriter)) {
      return null;
    }
    OnlineAttributeWriter writer = w.get().online();
    final URI uri = getProxyUri(writer.getUri(), targetFamily);
    return new OnlineAttributeWriter() {

      @Override
      public void rollback() {
        writer.rollback();
      }

      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        AttributeProxyDescriptorImpl<?> target = lookup.lookupProxy(
            data.getAttributeDescriptor().getName());

        log.debug(
            "proxying write of {} to target {} using writer {}",
            data, target, writer.getUri());

        writer.write(
                transformToRaw(data, target),
                statusCallback);
      }

      @Override
      public URI getUri() {
        return uri;
      }

      @Override
      public void close() {
        writer.close();
      }

    };
  }

  private static CommitLogReader getCommitLogReader(
      AttrLookup lookup,
      AttributeFamilyDescriptor targetFamily) {

    Optional<CommitLogReader> target = targetFamily.getCommitLogReader();
    if (!target.isPresent()) {
      return null;
    }
    CommitLogReader reader = target.get();
    return new CommitLogReader() {

      @Override
      public URI getUri() {
        return reader.getUri();
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
            name, position, wrapTransformed(lookup, observer));
      }

      @Override
      public ObserveHandle observePartitions(
          String name,
          Collection<Partition> partitions, Position position,
          boolean stopAtCurrent, LogObserver observer) {

        return reader.observePartitions(
            name, partitions, position, stopAtCurrent,
            wrapTransformed(lookup, observer));
      }

      @Override
      public ObserveHandle observePartitions(
          Collection<Partition> partitions, Position position,
          boolean stopAtCurrent, LogObserver observer) {

        return reader.observePartitions(
            partitions, position, stopAtCurrent,
            wrapTransformed(lookup, observer));
      }

      @Override
      public ObserveHandle observeBulk(
          String name, Position position, boolean stopAtCurrent,
          BulkLogObserver observer) {

        return reader.observeBulk(
            name, position, stopAtCurrent, wrapTransformed(lookup, observer));
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
            partitions, position, stopAtCurrent,
            wrapTransformed(lookup, observer));
      }

      @Override
      public ObserveHandle observeBulkPartitions(
          String name,
          Collection<Partition> partitions,
          Position position,
          boolean stopAtCurrent,
          BulkLogObserver observer) {

        return reader.observeBulkPartitions(
            name, partitions, position, stopAtCurrent,
            wrapTransformed(lookup, observer));
      }

      @Override
      public ObserveHandle observeBulkOffsets(
          Collection<Offset> offsets, BulkLogObserver observer) {

        return reader.observeBulkOffsets(
            offsets, wrapTransformed(lookup, observer));
      }

    };
  }


  private static BatchLogObservable getBatchObservable(
      AttrLookup lookup,
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
            partitions, attributes.stream()
                .map(a -> lookup.lookupProxy(a.getName()))
                .collect(Collectors.toList()),
            wrapTransformed(lookup, observer));
      }

    };
  }

  @SuppressWarnings("unchecked")
  private static RandomAccessReader getRandomAccess(
      AttrLookup lookup,
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

        if (type == Listing.ATTRIBUTE && !key.isEmpty()) {
          return reader.fetchOffset(
              type,
              lookup.lookupProxy(toAttrName(key))
                  .getReadTransform().fromProxy(key));
        }
        return reader.fetchOffset(type, key);
      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> Optional<KeyValue<T>> get(
          String key, String attribute, AttributeDescriptor<T> desc,
          long stamp) {

        AttributeProxyDescriptorImpl<T> targetAttribute;
        targetAttribute = (AttributeProxyDescriptorImpl<T>) lookup.lookupProxy(
            desc.getName());
        ProxyTransform transform = targetAttribute.getReadTransform();
        return reader.get(
            key, transform.fromProxy(attribute), targetAttribute.getReadTarget(), stamp)
            .map(kv -> transformToProxy(kv, targetAttribute));
      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> void scanWildcard(
          String key, AttributeDescriptor<T> wildcard,
          RandomOffset offset, long stamp, int limit, Consumer<KeyValue<T>> consumer) {

        AttributeProxyDescriptorImpl<?> targetAttribute = lookup.lookupProxy(
            wildcard.getName());
        if (!targetAttribute.isWildcard()) {
          throw new IllegalArgumentException(
              "Proxy target is not wildcard attribute!");
        }
        Preconditions.checkArgument(
            offset == null || offset instanceof RawOffset,
            "Scanning through proxy can be done with RawOffests only, got %s",
            offset);
        reader.scanWildcard(
            key, targetAttribute.getReadTarget(),
            offset,
            stamp,
            limit,
            kv -> consumer.accept((KeyValue) transformToProxy(kv, targetAttribute)));
      }

      @Override
      public void scanWildcardAll(
          String key, RandomOffset offset, long stamp,
          int limit, Consumer<KeyValue<?>> consumer) {

        reader.scanWildcardAll(
            key, offset, stamp, limit,
            kv -> lookup.lookupRead(kv.getAttrDescriptor().getName())
                  .stream()
                  .forEach(attr -> consumer.accept(transformToProxy(kv, attr))));
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
      AttrLookup lookup,
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

        return view.observePartitions(
            flow, partitions,
            wrapTransformed(lookup, observer));
      }

      @Override
      public <T> Dataset<T> observe(
          Flow flow, String name, PartitionedLogObserver<T> observer) {

        return view.observe(
            flow, name,
            wrapTransformed(lookup, observer));
      }

      @Override
      public EntityDescriptor getEntityDescriptor() {
        return view.getEntityDescriptor();
      }

    };
  }

  private static PartitionedCachedView getPartitionedCachedView(
      EntityDescriptor entity,
      AttrLookup lookup,
      AttributeFamilyDescriptor targetFamilyRead,
      AttributeFamilyDescriptor targetFamilyWrite) {

    return new LocalCachedPartitionedView(
        entity,
        getCommitLogReader(lookup, targetFamilyRead),
        getWriter(lookup, targetFamilyWrite));
  }

  @SuppressWarnings("unchecked")
  private static LogObserver wrapTransformed(
      AttrLookup lookup,
      LogObserver observer) {

    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter confirm) {
        try {
          return lookup.lookupRead(ingest.getAttributeDescriptor().getName())
              .stream()
              .map(attr -> observer.onNext(transformToProxy(ingest, attr), confirm))
              .filter(c -> !c)
              .findFirst()
              .orElse(true);
        } catch (Exception ex) {
          log.error("Failed to transform ingest {}", ingest, ex);
          confirm.fail(ex);
          return false;
        }
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
      AttrLookup lookup,
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
          OffsetCommitter confirm) {

        try {
          return lookup.lookupRead(ingest.getAttributeDescriptor().getName())
              .stream()
              .map(attr -> observer.onNext(
                  transformToProxy(ingest, attr), partition, confirm))
              .filter(c -> !c)
              .findFirst()
              .orElse(true);
        } catch (Exception ex) {
          log.error("Failed to transform ingest {}", ingest, ex);
          confirm.fail(ex);
          return true;
        }
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
      AttrLookup lookup,
      BatchLogObserver observer) {

    return new BatchLogObserver() {

      @Override
      public boolean onNext(
          StreamElement ingest,
          Partition partition) {

        try {
          return lookup.lookupRead(ingest.getAttributeDescriptor().getName())
              .stream()
              .map(attr -> observer.onNext(transformToProxy(ingest, attr), partition))
              .filter(c -> !c)
              .findFirst()
              .orElse(true);
        } catch (Exception ex) {
          log.error("Failed to transform ingest {}", ingest, ex);
          return true;
        }
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
      AttrLookup lookup,
      PartitionedLogObserver<T> observer) {

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

        try {
          return lookup.lookupRead(ingest.getAttributeDescriptor().getName())
              .stream()
              .map(attr -> observer.onNext(
                  transformToProxy(ingest, attr),
                  confirm, partition, collector))
              .filter(c -> !c)
              .findFirst()
              .orElse(true);
        } catch (Exception ex) {
          log.error("Failed to transform ingest {}", ingest, ex);
          return true;
        }
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
  private static StreamElement transformToRawRead(
      StreamElement data,
      AttributeProxyDescriptorImpl targetReadDesc) {

    return transform(data,
        targetReadDesc.getReadTarget(),
        targetReadDesc.getReadTransform()::fromProxy);
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToProxy(
      StreamElement data,
      AttributeProxyDescriptorImpl<?> targetDesc) {

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
        kv.getOffset(), kv.getValue(), kv.getValueBytes(), kv.getStamp());
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transform(
      StreamElement data,
      AttributeDescriptor target,
      UnaryFunction<String, String> transform) {

    if (data.isDelete()) {
      if (data.isDeleteWildcard()) {
        return StreamElement.deleteWildcard(
            data.getEntityDescriptor(),
            target, data.getUuid(), data.getKey(),
            transform.apply(data.getAttribute()),
            data.getStamp());
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

  private static URI getProxyUri(
      URI uri, AttributeFamilyDescriptor targetFamily) {
    try {
      return new URI(String.format(
          "proxy-%s.%s", targetFamily.getName(), uri.toString())
          .replace("_", "-"));
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String toAttrName(String key) {
    int index = key.indexOf('.');
    if (index > 0) {
      return key.substring(0, index) + ".*";
    }
    return key;
  }

  @Override
  boolean isProxy() {
    return true;
  }

}
