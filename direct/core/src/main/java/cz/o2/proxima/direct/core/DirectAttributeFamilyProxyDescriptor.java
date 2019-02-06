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
package cz.o2.proxima.direct.core;

import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.direct.randomaccess.RawOffset;
import cz.o2.proxima.transform.ProxyTransform;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.direct.view.LocalCachedPartitionedView;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.AttributeProxyDescriptor;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
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
import cz.o2.proxima.direct.view.CachedView;

/**
 * Attribute family proxy descriptor for direct operator.
 */
@Slf4j
public class DirectAttributeFamilyProxyDescriptor
    extends DirectAttributeFamilyDescriptor {

  static DirectAttributeFamilyProxyDescriptor of(
      Context context,
      AttributeFamilyProxyDescriptor proxy) {

    return new DirectAttributeFamilyProxyDescriptor(
        context, proxy, new AttrLookup(proxy));
  }

  static class AttrLookup implements Serializable {

    @Getter
    private final List<AttributeProxyDescriptor<?>> attrs;
    private final Map<String, AttributeProxyDescriptor<?>> proxyNameToDesc;
    private final Map<String, List<AttributeProxyDescriptor<?>>> readNameToDesc;
    private final String familyName;

    AttrLookup(
        AttributeFamilyProxyDescriptor proxy) {

      this.familyName = proxy.getName();
      this.attrs = proxy.getAttributes()
          .stream()
          .map(AttributeDescriptor::asProxy)
          .collect(Collectors.toList());

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

    List<AttributeProxyDescriptor<?>> lookupRead(String name) {
      List<AttributeProxyDescriptor<?>> read = readNameToDesc.get(name);
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

    AttributeProxyDescriptor<?> lookupProxy(String name) {
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

  DirectAttributeFamilyProxyDescriptor(
      Context context,
      AttributeFamilyProxyDescriptor desc,
      AttrLookup lookup) {

    super(
        desc,
        getWriter(lookup, context, desc),
        getCommitLogReader(lookup, context, desc),
        getBatchObservable(lookup, context, desc),
        getRandomAccess(lookup, context, desc),
        getPartitionedCachedView(lookup, context, desc));
  }


  private static Optional<AttributeWriterBase> getWriter(
      AttrLookup lookup,
      Context context,
      AttributeFamilyProxyDescriptor desc) {

    URI uri = desc.getStorageUri();
    Optional<AttributeWriterBase> w = context.resolve(
        desc.getTargetFamilyWrite())
        .flatMap(DirectAttributeFamilyDescriptor::getWriter);

    if (!w.isPresent() || !(w.get() instanceof OnlineAttributeWriter)) {
      return Optional.empty();
    }
    OnlineAttributeWriter writer = w.get().online();
    return Optional.of(new OnlineAttributeWriter() {

      @Override
      public void rollback() {
        writer.rollback();
      }

      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        AttributeProxyDescriptor<?> target = lookup.lookupProxy(
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

    });
  }

  private static Optional<CommitLogReader> getCommitLogReader(
      AttrLookup lookup,
      Context context,
      AttributeFamilyProxyDescriptor desc) {

    return context
        .resolve(desc.getTargetFamilyRead())
        .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
        .map(reader -> new CommitLogReader() {

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
              LogObserver observer) {

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
              LogObserver observer) {

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
              LogObserver observer) {

            return reader.observeBulkPartitions(
                name, partitions, position, stopAtCurrent,
                wrapTransformed(lookup, observer));
          }

          @Override
          public ObserveHandle observeBulkOffsets(
              Collection<Offset> offsets, LogObserver observer) {

            return reader.observeBulkOffsets(
                offsets, wrapTransformed(lookup, observer));
          }

        });
  }


  private static Optional<BatchLogObservable> getBatchObservable(
      AttrLookup lookup,
      Context context,
      AttributeFamilyProxyDescriptor desc) {

    return context
        .resolve(desc.getTargetFamilyRead())
        .flatMap(DirectAttributeFamilyDescriptor::getBatchObservable)
        .map(reader -> new BatchLogObservable() {

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

        });
  }

  @SuppressWarnings("unchecked")
  private static Optional<RandomAccessReader> getRandomAccess(
      AttrLookup lookup,
      Context context,
      AttributeFamilyProxyDescriptor desc) {

    return context
        .resolve(desc.getTargetFamilyRead())
        .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
        .map(reader -> new RandomAccessReader() {

          @Override
          public RandomOffset fetchOffset(
              RandomAccessReader.Listing type, String key) {

            if (type == RandomAccessReader.Listing.ATTRIBUTE && !key.isEmpty()) {
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

            AttributeProxyDescriptor<T> targetAttribute;
            targetAttribute = (AttributeProxyDescriptor<T>) lookup.lookupProxy(
                desc.getName());
            ProxyTransform transform = targetAttribute.getReadTransform();
            return reader.get(
                key, transform
                    .fromProxy(attribute), targetAttribute.getReadTarget(), stamp)
                .map(kv -> transformToProxy(kv, targetAttribute));
          }

          @SuppressWarnings("unchecked")
          @Override
          public <T> void scanWildcard(
              String key, AttributeDescriptor<T> wildcard,
              RandomOffset offset, long stamp, int limit,
              Consumer<KeyValue<T>> consumer) {

            AttributeProxyDescriptor<?> targetAttribute = lookup.lookupProxy(
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

        });
  }

  private static Optional<CachedView> getPartitionedCachedView(
      AttrLookup lookup,
      Context context,
      AttributeFamilyProxyDescriptor desc) {

    if (desc.getTargetFamilyRead().getAccess().canReadCommitLog()
        && !desc.getTargetFamilyWrite().getAccess().isReadonly()) {

      return Optional.of(new LocalCachedPartitionedView(
          desc.getTargetFamilyRead().getEntity(),
          getCommitLogReader(lookup, context, desc)
              .orElseThrow(() -> new IllegalArgumentException(
                  "Missing commit log reader for " + desc.getTargetFamilyRead())),
          (OnlineAttributeWriter) getWriter(lookup, context, desc)
              .orElseThrow(() -> new IllegalArgumentException(
                  "Missing writer for " + desc.getTargetFamilyWrite()))));
    }
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  private static LogObserver wrapTransformed(
      AttrLookup lookup,
      LogObserver observer) {

    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        try {
          return lookup.lookupRead(ingest.getAttributeDescriptor().getName())
              .stream()
              .map(attr -> observer.onNext(transformToProxy(ingest, attr), context))
              .filter(c -> !c)
              .findFirst()
              .orElse(true);
        } catch (Exception ex) {
          log.error("Failed to transform ingest {}", ingest, ex);
          context.fail(ex);
          return false;
        }
      }

      @Override
      public void onRepartition(OnRepartitionContext context) {
        observer.onRepartition(context);
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
      public boolean onError(Throwable error) {
        return observer.onError(error);
      }

    };
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToRaw(
      StreamElement data,
      AttributeProxyDescriptor targetDesc) {

    return transform(data,
        targetDesc.getWriteTarget(),
        targetDesc.getWriteTransform()::fromProxy);
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToRawRead(
      StreamElement data,
      AttributeProxyDescriptor targetReadDesc) {

    return transform(data,
        targetReadDesc.getReadTarget(),
        targetReadDesc.getReadTransform()::fromProxy);
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToProxy(
      StreamElement data,
      AttributeProxyDescriptor targetDesc) {

    return transform(data, targetDesc, targetDesc.getReadTransform()::toProxy);
  }

  @SuppressWarnings("unchecked")
  private static <T> KeyValue<T> transformToProxy(
      KeyValue<T> kv,
      AttributeProxyDescriptor targetDesc) {

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

  private static String toAttrName(String key) {
    int index = key.indexOf('.');
    if (index > 0) {
      return key.substring(0, index) + ".*";
    }
    return key;
  }

}
