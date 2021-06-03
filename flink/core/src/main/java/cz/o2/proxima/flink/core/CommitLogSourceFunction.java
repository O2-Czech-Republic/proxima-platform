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

import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.watermark.Watermark;

@Slf4j
public class CommitLogSourceFunction<OutputT>
    extends AbstractLogSourceFunction<
        CommitLogReader,
        CommitLogSourceFunction.LogObserver<OutputT>,
        Offset,
        CommitLogObserver.OnNextContext,
        OutputT> {

  static class LogObserver<OutputT>
      extends AbstractSourceLogObserver<Offset, CommitLogObserver.OnNextContext, OutputT>
      implements CommitLogObserver {

    LogObserver(
        SourceContext<OutputT> sourceContext,
        ResultExtractor<OutputT> resultExtractor,
        Set<Partition> skipFirstElementFromEachPartition) {
      super(sourceContext, resultExtractor, skipFirstElementFromEachPartition);
    }

    @Override
    public void onIdle(OnIdleContext context) {
      if (context.getWatermark() > watermark) {
        watermark = context.getWatermark();
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.emitWatermark(new Watermark(watermark));
        }
      }
    }

    @Override
    void markOffsetAsConsumed(CommitLogObserver.OnNextContext context) {
      // No-op.
    }
  }

  public CommitLogSourceFunction(
      RepositoryFactory repositoryFactory,
      List<AttributeDescriptor<?>> attributeDescriptors,
      ResultExtractor<OutputT> resultExtractor) {
    super(repositoryFactory, attributeDescriptors, resultExtractor);
  }

  @Override
  CommitLogReader createLogReader(List<AttributeDescriptor<?>> attributeDescriptors) {
    return getRepositoryFactory()
        .apply()
        .getOrCreateOperator(DirectDataOperator.class)
        .getCommitLogReader(attributeDescriptors)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Unable to find commit log reader for [%s].", attributeDescriptors)));
  }

  @Override
  List<Partition> getPartitions(CommitLogReader reader) {
    return reader.getPartitions();
  }

  @Override
  Partition getOffsetPartition(Offset offset) {
    return offset.getPartition();
  }

  @Override
  Set<Partition> getSkipFirstElement(List<Offset> offsets) {
    return offsets.stream().map(Offset::getPartition).collect(Collectors.toSet());
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
      CommitLogReader reader,
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributeDescriptors,
      LogObserver<OutputT> observer) {
    final cz.o2.proxima.direct.commitlog.ObserveHandle delegate =
        reader.observeBulkPartitions(partitions, Position.OLDEST, false, observer);
    return new ObserveHandle<Offset>() {

      @Override
      public List<Offset> getCurrentOffsets() {
        return delegate.getCurrentOffsets();
      }

      @Override
      public void close() {
        delegate.close();
      }
    };
  }

  @Override
  ObserveHandle<Offset> observeOffsets(
      CommitLogReader reader,
      List<Offset> offsets,
      List<AttributeDescriptor<?>> attributeDescriptors,
      LogObserver<OutputT> observer) {
    final cz.o2.proxima.direct.commitlog.ObserveHandle delegate =
        reader.observeBulkOffsets(offsets, false, observer);
    return new ObserveHandle<Offset>() {

      @Override
      public List<Offset> getCurrentOffsets() {
        return delegate.getCurrentOffsets();
      }

      @Override
      public void close() {
        delegate.close();
      }
    };
  }
}
