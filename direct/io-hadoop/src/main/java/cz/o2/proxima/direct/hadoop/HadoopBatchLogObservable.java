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
package cz.o2.proxima.direct.hadoop;

import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Observable of data stored in {@code SequenceFiles} in HDFS. */
@Slf4j
public class HadoopBatchLogObservable implements BatchLogObservable {

  @Getter private final HadoopDataAccessor accessor;
  private final Context context;
  private final Executor executor;

  public HadoopBatchLogObservable(HadoopDataAccessor accessor, Context context) {
    this.accessor = accessor;
    this.context = context;
    this.executor = context.getExecutorService();
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> partitions = new ArrayList<>();
    HadoopFileSystem fs = accessor.getHadoopFs();
    long batchProcessSize = accessor.getBatchProcessSize();
    @SuppressWarnings("unchecked")
    Stream<HadoopPath> paths = (Stream) fs.list(startStamp, endStamp);
    AtomicReference<HadoopPartition> current = new AtomicReference<>();
    paths
        .filter(p -> !p.isTmpPath())
        .forEach(
            p -> {
              if (current.get() == null) {
                current.set(new HadoopPartition((partitions.size())));
                partitions.add(current.get());
              }
              final HadoopPartition part = current.get();
              part.add(p);
              if (part.size() > batchProcessSize) {
                current.set(null);
              }
            });
    return partitions;
  }

  @Override
  public void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    executor.execute(
        () -> {
          boolean run = true;
          try {
            for (Iterator<Partition> it = partitions.iterator(); run && it.hasNext(); ) {
              HadoopPartition p = (HadoopPartition) it.next();
              for (HadoopPath path : p.getPaths()) {
                if (!processPath(observer, p, path)) {
                  run = false;
                  break;
                }
              }
            }
            observer.onCompleted();
          } catch (Throwable ex) {
            log.warn("Failed to observe partitions {}", partitions, ex);
            if (observer.onError(ex)) {
              log.info("Restaring processing by request");
              observe(partitions, attributes, observer);
            }
          }
        });
  }

  @Override
  public Factory<?> asFactory() {
    final HadoopDataAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new HadoopBatchLogObservable(accessor, context);
  }

  private boolean processPath(BatchLogObserver observer, HadoopPartition p, HadoopPath path) {
    try {
      try (Reader reader = accessor.getFormat().openReader(path, accessor.getEntityDesc())) {
        for (StreamElement elem : reader) {
          if (!observer.onNext(elem, p)) {
            return false;
          }
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to read file " + p, ex);
    }
    return true;
  }
}
