/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.storage;

import com.google.auto.service.AutoService;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.AbstractBulkAttributeWriter;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogObservers;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.direct.core.batch.TerminationContext;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Storage acting as a bulk in memory storage. */
@Slf4j
@AutoService(DataAccessorFactory.class)
public class InMemBulkStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  private static class Writer extends AbstractBulkAttributeWriter {

    int writtenSinceLastCommit = 0;
    CommitCallback toCommit = null;
    long lastWriteWatermark = Long.MIN_VALUE;

    public Writer(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public void write(StreamElement data, long watermark, CommitCallback statusCallback) {
      // store the data, commit after each 10 elements
      log.debug("Writing {} into {}", data, getUri());
      InMemBulkStorage.data.put(
          getUri().getPath() + "/" + data.getKey() + "#" + data.getAttribute(), data);
      lastWriteWatermark = watermark;
      toCommit = statusCallback;
      if (++writtenSinceLastCommit >= 10) {
        commit();
      }
    }

    @Override
    public void updateWatermark(long watermark) {
      if (toCommit != null && lastWriteWatermark + 3600000L < watermark) {
        commit();
      }
    }

    @Override
    public BulkAttributeWriter.Factory<?> asFactory() {
      final EntityDescriptor entity = getEntityDescriptor();
      final URI uri = getUri();
      return repo -> new Writer(entity, uri);
    }

    void commit() {
      Optional.ofNullable(toCommit).ifPresent(c -> c.commit(true, null));
      writtenSinceLastCommit = 0;
      toCommit = null;
    }

    @Override
    public void rollback() {
      // nop
    }

    @Override
    public void close() {
      // nop
    }
  }

  private static class BatchReader extends AbstractStorage implements BatchLogReader {

    private final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory;
    private transient ExecutorService executor;

    private BatchReader(
        EntityDescriptor entityDesc,
        URI uri,
        cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory) {

      super(entityDesc, uri);
      this.executorFactory = executorFactory;
    }

    @Override
    public List<Partition> getPartitions(long startStamp, long endStamp) {
      return Collections.singletonList(() -> 0);
    }

    @Override
    public ObserveHandle observe(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer) {

      TerminationContext terminationContext = new TerminationContext(observer);
      observeInternal(partitions, attributes, observer, terminationContext);
      return terminationContext;
    }

    private void observeInternal(
        List<Partition> partitions,
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer,
        TerminationContext terminationContext) {

      Preconditions.checkArgument(
          partitions.size() == 1, "This reader works on single partition only, got " + partitions);
      String prefix = getUri().getPath();
      executor()
          .submit(
              () -> {
                try {
                  for (Map.Entry<String, StreamElement> e : InMemBulkStorage.data.entrySet()) {
                    if (!processRecord(attributes, observer, terminationContext, prefix, e)) {
                      break;
                    }
                  }
                  terminationContext.finished();
                } catch (Throwable err) {
                  terminationContext.handleErrorCaught(
                      err,
                      () -> observeInternal(partitions, attributes, observer, terminationContext));
                }
              });
    }

    private boolean processRecord(
        List<AttributeDescriptor<?>> attributes,
        BatchLogObserver observer,
        TerminationContext terminationContext,
        String prefix,
        Map.Entry<String, StreamElement> e) {

      if (terminationContext.isCancelled()) {
        return false;
      }
      if (!e.getKey().startsWith(prefix)) {
        return false;
      }
      String k = e.getKey();
      StreamElement v = e.getValue();
      String[] parts = k.substring(prefix.length()).split("#");
      String key = parts[0];
      String attribute = parts[1];
      return getEntityDescriptor()
          .findAttribute(attribute, true)
          .filter(attributes::contains)
          .map(desc -> observer.onNext(v, BatchLogObservers.defaultContext(Partition.of(0))))
          .orElse(true);
    }

    @Override
    public Factory<?> asFactory() {
      final EntityDescriptor entity = getEntityDescriptor();
      final URI uri = getUri();
      final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory =
          this.executorFactory;
      return repo -> new BatchReader(entity, uri, executorFactory);
    }

    private ExecutorService executor() {
      if (executor == null) {
        executor = executorFactory.apply();
      }
      return executor;
    }
  }

  private class InMemBulkAccessor implements DataAccessor {

    private static final long serialVersionUID = 1L;

    private final EntityDescriptor entityDesc;
    @Getter private final URI uri;

    InMemBulkAccessor(EntityDescriptor entityDesc, URI uri) {
      this.entityDesc = entityDesc;
      this.uri = uri;
    }

    @Override
    public Optional<AttributeWriterBase> getWriter(Context context) {
      return Optional.of(new Writer(entityDesc, uri));
    }

    @Override
    public Optional<BatchLogReader> getBatchLogReader(Context context) {
      return Optional.of(new BatchReader(entityDesc, uri, context::getExecutorService));
    }
  }

  private static final NavigableMap<String, StreamElement> data =
      Collections.synchronizedNavigableMap(new TreeMap<>());

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("inmem-bulk") ? Accept.ACCEPT : Accept.REJECT;
  }

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator operator, AttributeFamilyDescriptor familyDescriptor) {
    return new InMemBulkAccessor(familyDescriptor.getEntity(), familyDescriptor.getStorageUri());
  }

  public NavigableMap<String, StreamElement> getData(String prefix) {
    return Collections.unmodifiableNavigableMap(
        data.subMap(prefix, true, prefix + Character.MAX_VALUE, false));
  }
}
