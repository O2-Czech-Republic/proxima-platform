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
package cz.o2.proxima.direct.io.hadoop;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogObservers;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.direct.core.batch.Offset;
import cz.o2.proxima.direct.core.batch.TerminationContext;
import cz.o2.proxima.direct.io.bulkfs.Reader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Reader of data stored in {@code SequenceFiles} in HDFS. */
@Slf4j
public class HadoopBatchLogReader implements BatchLogReader {

  @Getter private final HadoopDataAccessor accessor;
  private final Context context;
  private final ExecutorService executor;

  public HadoopBatchLogReader(HadoopDataAccessor accessor, Context context) {
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

    executor.submit(
        () -> {
          try {
            outer:
            for (Iterator<Partition> it = partitions.iterator(); it.hasNext(); ) {
              HadoopPartition p = (HadoopPartition) it.next();
              for (HadoopPath path : p.getPaths()) {
                if (!processPath(observer, p.getMinTimestamp(), p, path, terminationContext)) {
                  break outer;
                }
              }
            }
            terminationContext.finished();
          } catch (Throwable ex) {
            terminationContext.handleErrorCaught(
                ex,
                () -> {
                  log.info("Restarting processing by request");
                  observeInternal(partitions, attributes, observer, terminationContext);
                });
          }
        });
  }

  @Override
  public Factory<?> asFactory() {
    final HadoopDataAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new HadoopBatchLogReader(accessor, context);
  }

  private boolean processPath(
      BatchLogObserver observer,
      long watermark,
      HadoopPartition partition,
      HadoopPath path,
      TerminationContext terminationContext) {

    try {
      try (Reader reader = accessor.getFormat().openReader(path, accessor.getEntityDesc())) {
        long elementIndex = 0;
        final Iterator<StreamElement> iterator = reader.iterator();
        while (iterator.hasNext()) {
          final StreamElement element = iterator.next();
          final Offset offset = Offset.of(partition, elementIndex++, !iterator.hasNext());
          if (terminationContext.isCancelled()
              || !observer.onNext(
                  element, BatchLogObservers.withWatermark(partition, offset, watermark))) {
            return false;
          }
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to read file " + partition, ex);
    }
    return true;
  }
}
