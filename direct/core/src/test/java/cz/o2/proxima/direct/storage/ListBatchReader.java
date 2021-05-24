/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogObservers;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.direct.batch.TerminationContext;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.Watermarks;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A {@link BatchLogReader} implementaion backed by {@link List}. */
public class ListBatchReader implements BatchLogReader, Serializable {

  /**
   * Create a {@link BatchLogReader} from given data in single partition.
   *
   * @param context the {@link Context}
   * @param data the data in the reader
   * @return the reader
   */
  public static ListBatchReader of(Context context, List<StreamElement> data) {
    return ofPartitioned(context, Collections.singletonList(data));
  }

  /**
   * Create a {@link BatchLogReader} from given data in given partitions.
   *
   * @param context the {@link Context}
   * @param partitions partitioned data contained in the reader
   * @return the reader
   */
  @SafeVarargs
  public static ListBatchReader ofPartitioned(Context context, List<StreamElement>... partitions) {
    return ofPartitioned(context, Arrays.asList(partitions));
  }

  /**
   * Create a {@link BatchLogReader} from given data in given partitions.
   *
   * @param context the {@link Context}
   * @param partitions partitioned data contained in the reader
   * @return the reader
   */
  public static ListBatchReader ofPartitioned(
      Context context, List<List<StreamElement>> partitions) {

    return new ListBatchReader(context, partitions);
  }

  private final Context context;
  private final List<List<StreamElement>> data;

  private ListBatchReader(Context context, List<List<StreamElement>> partitions) {
    this.context = Objects.requireNonNull(context);
    this.data = partitions.stream().map(Lists::newArrayList).collect(Collectors.toList());
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    return IntStream.range(0, data.size()).mapToObj(Partition::of).collect(Collectors.toList());
  }

  @Override
  public ObserveHandle observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    Preconditions.checkArgument(partitions != null);
    Preconditions.checkArgument(attributes != null);
    Preconditions.checkArgument(observer != null);

    Set<AttributeDescriptor<?>> attrSet = new HashSet<>(attributes);
    TerminationContext terminationContext = new TerminationContext(observer);
    context
        .getExecutorService()
        .submit(
            () -> {
              terminationContext.setRunningThread();
              for (int i = 0; i < partitions.size() && !terminationContext.isCancelled(); i++) {
                long elementIndex = 0;
                final Partition partition = partitions.get(i);
                final Iterator<StreamElement> iterator = data.get(partition.getId()).iterator();
                while (!Thread.currentThread().isInterrupted() && iterator.hasNext()) {
                  final StreamElement element = iterator.next();
                  final BatchLogObserver.Offset offset =
                      new BatchLogObserver.SimpleOffset(
                          partition, elementIndex++, !iterator.hasNext());
                  if (attrSet.contains(element.getAttributeDescriptor())
                      && !observer.onNext(
                          element,
                          BatchLogObservers.withWatermark(
                              partition, offset, Watermarks.MIN_WATERMARK))) {
                    terminationContext.cancel();
                    break;
                  }
                }
              }
              terminationContext.finished();
            });
    return terminationContext.asObserveHandle();
  }

  @Override
  public Factory<?> asFactory() {
    return (Factory<ListBatchReader>) input -> this;
  }
}
