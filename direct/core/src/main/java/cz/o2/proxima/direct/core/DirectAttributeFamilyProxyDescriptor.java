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
package cz.o2.proxima.direct.core;

import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.direct.randomaccess.RawOffset;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.direct.view.LocalCachedPartitionedView;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.AttributeProxyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.transform.ProxyTransform;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Attribute family proxy descriptor for direct operator. */
@Slf4j
public class DirectAttributeFamilyProxyDescriptor extends DirectAttributeFamilyDescriptor {

  private static final long serialVersionUID = 1L;

  static DirectAttributeFamilyProxyDescriptor of(
      Repository repo, Context context, AttributeFamilyProxyDescriptor proxy) {

    return new DirectAttributeFamilyProxyDescriptor(repo, context, proxy, new AttrLookup(proxy));
  }

  static class AttrLookup implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private final List<AttributeProxyDescriptor<?>> attrs;
    private final Map<String, AttributeProxyDescriptor<?>> proxyNameToDesc;
    private final Map<String, List<AttributeProxyDescriptor<?>>> readNameToDesc;
    private final String familyName;

    @SuppressWarnings("unchecked")
    AttrLookup(AttributeFamilyProxyDescriptor proxy) {

      this.familyName = proxy.getName();
      this.attrs =
          proxy
              .getAttributes()
              .stream()
              .map(AttributeDescriptor::asProxy)
              .collect(Collectors.toList());

      proxyNameToDesc =
          attrs
              .stream()
              .collect(Collectors.toMap(AttributeDescriptor::getName, Function.identity()));
      readNameToDesc =
          (Map)
              attrs
                  .stream()
                  .map(a -> Pair.of(a.getReadTarget().getName(), a))
                  .collect(
                      Collectors.groupingBy(
                          Pair::getFirst,
                          Collectors.mapping(Pair::getSecond, Collectors.toList())));
    }

    List<AttributeProxyDescriptor<?>> lookupRead(String name) {
      List<AttributeProxyDescriptor<?>> read = readNameToDesc.get(name);
      if (read == null) {
        log.debug(
            "Fallbacking to lookup of proxy attribute with name {} in family {}. ",
            name,
            familyName);
        try {
          return Collections.singletonList(lookupProxy(name));
        } catch (Exception ex) {
          log.warn(
              "Error during lookup of {} in family {}." + "This might indicate serious problem.",
              name,
              familyName,
              ex);
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

  private static Optional<CommitLogReader> getCommitLogReader(
      AttrLookup lookup, Context context, AttributeFamilyProxyDescriptor desc) {

    return context
        .resolve(desc.getTargetFamilyRead())
        .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
        .map(reader -> new ProxyCommitLogReader(reader, lookup));
  }

  DirectAttributeFamilyProxyDescriptor(
      Repository repo, Context context, AttributeFamilyProxyDescriptor desc, AttrLookup lookup) {

    super(
        repo,
        desc,
        getWriter(lookup, context, desc),
        getCommitLogReader(lookup, context, desc),
        getBatchReader(lookup, context, desc),
        getRandomAccess(lookup, context, desc),
        getPartitionedCachedView(lookup, context, desc));
  }

  private static Optional<AttributeWriterBase> getWriter(
      AttrLookup lookup, Context context, AttributeFamilyProxyDescriptor desc) {

    URI uri = desc.getStorageUri();
    Optional<AttributeWriterBase> w =
        context
            .resolve(desc.getTargetFamilyWrite())
            .flatMap(DirectAttributeFamilyDescriptor::getWriter);

    if (!w.isPresent() || !(w.get() instanceof OnlineAttributeWriter)) {
      return Optional.empty();
    }
    OnlineAttributeWriter writer = w.get().online();
    return Optional.of(new ProxyOnlineAttributeWriter(writer, lookup, uri));
  }

  private static Optional<BatchLogReader> getBatchReader(
      AttrLookup lookup, Context context, AttributeFamilyProxyDescriptor desc) {

    return context
        .resolve(desc.getTargetFamilyRead())
        .flatMap(DirectAttributeFamilyDescriptor::getBatchReader)
        .map(reader -> new ProxyBatchLogReader(reader, lookup));
  }

  @SuppressWarnings("unchecked")
  private static Optional<RandomAccessReader> getRandomAccess(
      AttrLookup lookup, Context context, AttributeFamilyProxyDescriptor desc) {

    return context
        .resolve(desc.getTargetFamilyRead())
        .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader)
        .map(reader -> new ProxyRandomAccessReader(reader, lookup));
  }

  private static Optional<CachedView> getPartitionedCachedView(
      AttrLookup lookup, Context context, AttributeFamilyProxyDescriptor desc) {

    if (desc.getTargetFamilyRead().getAccess().canReadCommitLog()
        && !desc.getTargetFamilyWrite().getAccess().isReadonly()
        && desc.getTargetFamilyRead().getAccess().canCreateCachedView()) {

      Optional<CommitLogReader> maybeReader = getCommitLogReader(lookup, context, desc);
      Optional<OnlineAttributeWriter> maybeWriter =
          getWriter(lookup, context, desc).map(AttributeWriterBase::online);
      if (maybeReader.isPresent() && maybeWriter.isPresent()) {
        return Optional.of(
            new LocalCachedPartitionedView(
                desc.getTargetFamilyRead().getEntity(), maybeReader.get(), maybeWriter.get()));
      }
    }
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  private static LogObserver wrapTransformed(AttrLookup lookup, LogObserver observer) {

    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        try {
          return lookup
              .lookupRead(ingest.getAttributeDescriptor().getName())
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
      public void onIdle(OnIdleContext context) {
        observer.onIdle(context);
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

  static BatchLogObserver wrapTransformed(AttrLookup lookup, BatchLogObserver observer) {

    return new BatchLogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {

        try {
          return lookup
              .lookupRead(ingest.getAttributeDescriptor().getName())
              .stream()
              .map(attr -> observer.onNext(transformToProxy(ingest, attr), context))
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

      @Override
      public void onCancelled() {
        observer.onCancelled();
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToRaw(
      StreamElement data, AttributeProxyDescriptor<?> targetDesc) {

    return transform(
        data,
        targetDesc.getWriteTarget(),
        targetDesc.getWriteTransform().asElementWise()::fromProxy);
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToRawRead(
      StreamElement data, AttributeProxyDescriptor<?> targetReadDesc) {

    return transform(
        data,
        targetReadDesc.getReadTarget(),
        targetReadDesc.getReadTransform().asElementWise()::fromProxy);
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transformToProxy(
      StreamElement data, AttributeProxyDescriptor<?> targetDesc) {

    return transform(data, targetDesc, targetDesc.getReadTransform().asElementWise()::toProxy);
  }

  @SuppressWarnings("unchecked")
  private static <T> KeyValue<T> transformKvToProxy(
      KeyValue<T> kv, AttributeProxyDescriptor<T> targetDesc) {

    return KeyValue.of(
        kv.getEntityDescriptor(),
        targetDesc,
        kv.getKey(),
        targetDesc.getReadTransform().asElementWise().toProxy(kv.getAttribute()),
        kv.getOffset(),
        kv.getParsedRequired(),
        kv.getValue(),
        kv.getStamp());
  }

  @SuppressWarnings("unchecked")
  private static StreamElement transform(
      StreamElement data, AttributeDescriptor<?> target, UnaryFunction<String, String> transform) {

    if (data.isDeleteWildcard()) {
      return StreamElement.deleteWildcard(
          data.getEntityDescriptor(),
          target,
          data.getUuid(),
          data.getKey(),
          transform.apply(data.getAttribute()),
          data.getStamp());
    } else if (data.isDelete()) {
      return StreamElement.delete(
          data.getEntityDescriptor(),
          target,
          data.getUuid(),
          data.getKey(),
          transform.apply(data.getAttribute()),
          data.getStamp());
    }
    return StreamElement.upsert(
        data.getEntityDescriptor(),
        target,
        data.getUuid(),
        data.getKey(),
        transform.apply(data.getAttribute()),
        data.getStamp(),
        data.getValue());
  }

  private static String toAttrName(String key) {
    int index = key.indexOf('.');
    if (index > 0) {
      return key.substring(0, index) + ".*";
    }
    return key;
  }

  private static class ProxyOnlineAttributeWriter implements OnlineAttributeWriter {

    private final OnlineAttributeWriter writer;
    private final AttrLookup lookup;
    private final URI uri;

    public ProxyOnlineAttributeWriter(OnlineAttributeWriter writer, AttrLookup lookup, URI uri) {
      this.writer = writer;
      this.lookup = lookup;
      this.uri = uri;
    }

    @Override
    public void rollback() {
      writer.rollback();
    }

    @Override
    public void write(StreamElement data, CommitCallback statusCallback) {
      AttributeProxyDescriptor<?> target =
          lookup.lookupProxy(data.getAttributeDescriptor().getName());

      log.debug("proxying write of {} to target {} using writer {}", data, target, writer.getUri());

      writer.write(transformToRaw(data, target), statusCallback);
    }

    @Override
    public Factory<?> asFactory() {
      final Factory<?> writerFactory = writer.asFactory();
      final AttrLookup lookup = this.lookup;
      final URI uri = this.uri;
      return repo -> new ProxyOnlineAttributeWriter(writerFactory.apply(repo), lookup, uri);
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public void close() {
      writer.close();
    }
  }

  private static class ProxyCommitLogReader implements CommitLogReader {

    private final CommitLogReader reader;
    private final AttrLookup lookup;

    public ProxyCommitLogReader(CommitLogReader reader, AttrLookup lookup) {
      this.reader = reader;
      this.lookup = lookup;
    }

    @Override
    public URI getUri() {
      return reader.getUri();
    }

    @Override
    public List<Partition> getPartitions() {
      return reader.getPartitions();
    }

    @Override
    public ObserveHandle observe(String name, Position position, LogObserver observer) {

      return reader.observe(name, position, wrapTransformed(lookup, observer));
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      return reader.observePartitions(
          name, partitions, position, stopAtCurrent, wrapTransformed(lookup, observer));
    }

    @Override
    public ObserveHandle observePartitions(
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      return reader.observePartitions(
          partitions, position, stopAtCurrent, wrapTransformed(lookup, observer));
    }

    @Override
    public ObserveHandle observeBulk(
        String name, Position position, boolean stopAtCurrent, LogObserver observer) {

      return reader.observeBulk(name, position, stopAtCurrent, wrapTransformed(lookup, observer));
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      return reader.observeBulkPartitions(
          partitions, position, stopAtCurrent, wrapTransformed(lookup, observer));
    }

    @Override
    public ObserveHandle observeBulkPartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      return reader.observeBulkPartitions(
          name, partitions, position, stopAtCurrent, wrapTransformed(lookup, observer));
    }

    @Override
    public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {

      return reader.observeBulkOffsets(offsets, wrapTransformed(lookup, observer));
    }

    @Override
    public Factory<?> asFactory() {
      final Factory<?> readerFactory = reader.asFactory();
      final AttrLookup lookup = this.lookup;
      return repo -> new ProxyCommitLogReader(readerFactory.apply(repo), lookup);
    }
  }

  private static class ProxyRandomAccessReader implements RandomAccessReader {

    private final RandomAccessReader reader;
    private final AttrLookup lookup;

    public ProxyRandomAccessReader(RandomAccessReader reader, AttrLookup lookup) {
      this.reader = reader;
      this.lookup = lookup;
    }

    @Override
    public RandomOffset fetchOffset(Listing type, String key) {

      if (type == Listing.ATTRIBUTE && !key.isEmpty()) {
        return reader.fetchOffset(
            type,
            lookup.lookupProxy(toAttrName(key)).getReadTransform().asElementWise().fromProxy(key));
      }
      return reader.fetchOffset(type, key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<KeyValue<T>> get(
        String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

      AttributeProxyDescriptor<T> targetAttribute;
      targetAttribute = (AttributeProxyDescriptor<T>) lookup.lookupProxy(desc.getName());
      ProxyTransform transform = targetAttribute.getReadTransform();
      return reader
          .get(
              key,
              transform.asElementWise().fromProxy(attribute),
              targetAttribute.getReadTarget(),
              stamp)
          .map(kv -> transformKvToProxy(kv, targetAttribute));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void scanWildcard(
        String key,
        AttributeDescriptor<T> wildcard,
        RandomOffset offset,
        long stamp,
        int limit,
        Consumer<KeyValue<T>> consumer) {

      AttributeProxyDescriptor<?> targetAttribute = lookup.lookupProxy(wildcard.getName());
      if (!targetAttribute.isWildcard()) {
        throw new IllegalArgumentException("Proxy target is not wildcard attribute!");
      }
      Preconditions.checkArgument(
          offset == null || offset instanceof RawOffset,
          "Scanning through proxy can be done with RawOffests only, got %s",
          offset);
      reader.scanWildcard(
          key,
          targetAttribute.getReadTarget(),
          offset,
          stamp,
          limit,
          kv -> consumer.accept(transformKvToProxy((KeyValue) kv, targetAttribute)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset, long stamp, int limit, Consumer<KeyValue<?>> consumer) {

      reader.scanWildcardAll(
          key,
          offset,
          stamp,
          limit,
          kv ->
              lookup
                  .lookupRead(kv.getAttributeDescriptor().getName())
                  .forEach(attr -> consumer.accept(transformKvToProxy((KeyValue) kv, attr))));
    }

    @Override
    public void listEntities(
        RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

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

    @Override
    public Factory<?> asFactory() {
      final Factory<?> readerFactory = reader.asFactory();
      final AttrLookup lookup = this.lookup;
      return repo -> new ProxyRandomAccessReader(readerFactory.apply(repo), lookup);
    }
  }

  private static class ProxyBatchLogReader implements BatchLogReader {

    private final BatchLogReader reader;
    private final AttrLookup lookup;

    public ProxyBatchLogReader(BatchLogReader reader, AttrLookup lookup) {
      this.reader = reader;
      this.lookup = lookup;
    }

    @Override
    public List<Partition> getPartitions(long startStamp, long endStamp) {
      return reader.getPartitions(startStamp, endStamp);
    }

    @Override
    public cz.o2.proxima.direct.batch.ObserveHandle observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {

      return reader.observe(
          partitions,
          attributes
              .stream()
              .map(a -> lookup.lookupProxy(a.getName()))
              .collect(Collectors.toList()),
          wrapTransformed(lookup, observer));
    }

    @Override
    public Factory<?> asFactory() {
      final Factory<?> readerFactory = reader.asFactory();
      final AttrLookup lookup = this.lookup;
      return repo -> new ProxyBatchLogReader(readerFactory.apply(repo), lookup);
    }
  }
}
