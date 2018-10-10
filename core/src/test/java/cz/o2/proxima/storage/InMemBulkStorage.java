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
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.batch.BatchLogObserver;
import cz.o2.proxima.util.Pair;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import lombok.Getter;

/**
 * Storage acting as a bulk in memory storage.
 */
public class InMemBulkStorage extends StorageDescriptor {

  private class Writer extends AbstractBulkAttributeWriter {

    int writtenSinceLastCommit = 0;

    public Writer(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public void write(StreamElement data, CommitCallback statusCallback) {
      // store the data, commit after each 10 elements
      InMemBulkStorage.this.data.put(
          getUri().getPath() + "/" + data.getKey() + "#" + data.getAttribute(),
          Pair.of(data.getStamp(), data.getValue()));
      if (++writtenSinceLastCommit >= 10) {
        statusCallback.commit(true, null);
        writtenSinceLastCommit = 0;
      }
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

  private class BatchObservable extends AbstractStorage implements BatchLogObservable {

    private final Factory<ExecutorService> executorFactory;
    private transient ExecutorService executor;

    private BatchObservable(
        EntityDescriptor entityDesc,
        URI uri,
        Factory<ExecutorService> executorFactory) {

      super(entityDesc, uri);
      this.executorFactory = executorFactory;
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
          InMemBulkStorage.this.data.forEach((k, v) -> {
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

  private class InMemBulkAccessor implements DataAccessor {

    private final EntityDescriptor entityDesc;
    private final URI uri;

    InMemBulkAccessor(EntityDescriptor entityDesc, URI uri) {
      this.entityDesc = entityDesc;
      this.uri = uri;
    }

    @Override
    public Optional<AttributeWriterBase> getWriter(Context context) {
      return Optional.of(new Writer(entityDesc, uri));
    }

    @Override
    public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
      return Optional.of(new BatchObservable(
          entityDesc, uri, () -> context.getExecutorService()));
    }

  }

  @Getter
  private final NavigableMap<String, Pair<Long, byte[]>> data = new TreeMap<>();

  public InMemBulkStorage() {
    super(Collections.singletonList("inmem-bulk"));
  }

  @Override
  public DataAccessor getAccessor(
      EntityDescriptor entityDesc, URI uri,
      Map<String, Object> cfg) {

    return new InMemBulkAccessor(entityDesc, uri);
  }

}
