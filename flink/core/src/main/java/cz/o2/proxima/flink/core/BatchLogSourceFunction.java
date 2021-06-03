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
package cz.o2.proxima.flink.core;

import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.Offset;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.flink.core.batch.OffsetTrackingBatchLogReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;

@Slf4j
public class BatchLogSourceFunction<OutputT>
    extends AbstractLogSourceFunction<
        BatchLogReader,
        BatchLogSourceFunction.LogObserver<OutputT>,
        Offset,
        BatchLogObserver.OnNextContext,
        OutputT> {

  static class LogObserver<OutputT>
      extends AbstractSourceLogObserver<Offset, BatchLogObserver.OnNextContext, OutputT>
      implements BatchLogObserver {

    LogObserver(
        SourceContext<OutputT> sourceContext,
        ResultExtractor<OutputT> resultExtractor,
        Set<Partition> skipFirstElementFromEachPartition) {
      super(sourceContext, resultExtractor, skipFirstElementFromEachPartition);
    }

    @Override
    void markOffsetAsConsumed(BatchLogObserver.OnNextContext context) {
      final OffsetTrackingBatchLogReader.OffsetCommitter committer =
          (OffsetTrackingBatchLogReader.OffsetCommitter) context;
      committer.markOffsetAsConsumed();
    }
  }

  public BatchLogSourceFunction(
      RepositoryFactory repositoryFactory,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ResultExtractor<OutputT> resultExtractor) {
    super(repositoryFactory, attributeDescriptors, resultExtractor);
  }

  @Override
  BatchLogReader createLogReader(List<AttributeDescriptor<?>> attributeDescriptors) {
    final BatchLogReader batchLogReader =
        getRepositoryFactory()
            .apply()
            .getOrCreateOperator(DirectDataOperator.class)
            .getBatchLogReader(attributeDescriptors)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to find batch log reader for [%s].", attributeDescriptors)));
    return OffsetTrackingBatchLogReader.of(batchLogReader);
  }

  @Override
  List<Partition> getPartitions(BatchLogReader reader) {
    return reader.getPartitions();
  }

  @Override
  Partition getOffsetPartition(Offset offset) {
    return offset.getPartition();
  }

  @Override
  LogObserver<OutputT> createObserver(
      SourceContext<OutputT> sourceContext,
      ResultExtractor<OutputT> resultExtractor,
      Set<Partition> skipFirstElement) {
    return new LogObserver<>(sourceContext, resultExtractor, skipFirstElement);
  }

  @Override
  ObserveHandle<Offset> observe(
      BatchLogReader reader,
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributeDescriptors,
      LogObserver<OutputT> observer) {
    final OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle delegate =
        (OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle)
            reader.observe(partitions, attributeDescriptors, wrapSourceObserver(observer));
    return new ObserveHandle<Offset>() {

      @Override
      public List<Offset> getCurrentOffsets() {
        // Filter out finished partitions, as we don't need them for restoring the state.
        return delegate
            .getCurrentOffsets()
            .stream()
            .filter(offset -> !offset.isLast())
            .collect(Collectors.toList());
      }

      @Override
      public void close() {
        delegate.close();
      }
    };
  }

  @Override
  ObserveHandle<Offset> observeOffsets(
      BatchLogReader reader,
      List<Offset> offsets,
      List<AttributeDescriptor<?>> attributeDescriptors,
      LogObserver<OutputT> observer) {
    final OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle delegate =
        (OffsetTrackingBatchLogReader.OffsetTrackingObserveHandle)
            reader.observeOffsets(offsets, attributeDescriptors, wrapSourceObserver(observer));
    return new ObserveHandle<Offset>() {

      @Override
      public List<Offset> getCurrentOffsets() {
        // Filter out finished partitions, as we don't need them for restoring the state.
        return delegate
            .getCurrentOffsets()
            .stream()
            .filter(offset -> !offset.isLast())
            .collect(Collectors.toList());
      }

      @Override
      public void close() {
        delegate.close();
      }
    };
  }

  @Override
  Set<Partition> getSkipFirstElement(List<Offset> offsets) {
    return offsets
        .stream()
        .filter(offset -> offset.getElementIndex() >= 0)
        .map(Offset::getPartition)
        .collect(Collectors.toSet());
  }

  /**
   * Allow tests to wrap the source observer, in order to place a barrier for deterministically
   * acquiring the checkpoint lock.
   *
   * @param sourceObserver Source observer to wrap.
   * @return Wrapped observer.
   */
  @VisibleForTesting
  BatchLogObserver wrapSourceObserver(BatchLogObserver sourceObserver) {
    return sourceObserver;
  }
}
