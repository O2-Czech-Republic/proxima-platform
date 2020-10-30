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
package cz.o2.proxima.direct.blob;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogObservers;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.direct.batch.TerminationContext;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** {@link BatchLogReader} for blob storages. */
@Slf4j
public abstract class BlobLogReader<BlobT extends BlobBase, BlobPathT extends BlobPath<BlobT>>
    implements BatchLogReader {

  private static class BulkStoragePartition<BlobT extends BlobBase> implements Partition {

    private static final long serialVersionUID = 1L;

    @Getter private final List<BlobT> blobs = new ArrayList<>();
    private final int id;
    private long minStamp;
    private long maxStamp;
    private long size;

    BulkStoragePartition(int id, long minStamp, long maxStamp) {
      this.id = id;
      this.minStamp = minStamp;
      this.maxStamp = maxStamp;
    }

    void add(BlobT b, long minStamp, long maxStamp) {
      blobs.add(b);
      size += getSize(b);
      this.minStamp = Math.min(this.minStamp, minStamp);
      this.maxStamp = Math.max(this.maxStamp, maxStamp);
    }

    private long getSize(BlobT b) {
      return b.getSize();
    }

    @Override
    public int getId() {
      return id;
    }

    @Override
    public boolean isBounded() {
      return true;
    }

    @Override
    public long size() {
      return size;
    }

    public int getNumBlobs() {
      return blobs.size();
    }

    @Override
    public long getMinTimestamp() {
      return minStamp;
    }

    @Override
    public long getMaxTimestamp() {
      return maxStamp;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(BulkStoragePartition.class)
          .add("id", getId())
          .add("size", size())
          .add("minTimestamp", getMinTimestamp())
          .add("maxTimestamp", getMaxTimestamp())
          .add("blobs.size()", blobs.size())
          .toString();
    }
  }

  @FunctionalInterface
  public interface ThrowingRunnable extends Serializable {
    void run() throws Exception;
  }

  private final EntityDescriptor entity;
  private final FileSystem fs;
  private final FileFormat fileFormat;
  private final NamingConvention namingConvention;
  private final long partitionMinSize;
  private final int partitionMaxNumBlobs;
  private final long partitionMaxTimeSpan;
  private final ExecutorService executor;
  @Getter private final BlobStorageAccessor accessor;
  @Getter private final Context context;

  protected BlobLogReader(BlobStorageAccessor accessor, Context context) {
    this.entity = accessor.getEntityDescriptor();
    this.fs = accessor.getTargetFileSystem();
    this.fileFormat = accessor.getFileFormat();
    this.namingConvention = accessor.getNamingConvention();
    this.partitionMinSize = accessor.getPartitionMinSize();
    this.partitionMaxNumBlobs = accessor.getPartitionMaxNumBlobs();
    this.partitionMaxTimeSpan = accessor.getPartitionMaxTimeSpanMs();
    this.executor = context.getExecutorService();
    this.context = context;
    this.accessor = accessor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> ret = new ArrayList<>();
    AtomicInteger id = new AtomicInteger();
    @Nullable BulkStoragePartition<BlobT> current = null;
    Stream<Path> paths = fs.list(startStamp, endStamp).sorted();
    for (Iterator<Path> it = paths.iterator(); it.hasNext(); ) {
      current =
          considerBlobForPartitionInclusion(((BlobPathT) it.next()).getBlob(), id, current, ret);
    }
    if (current != null) {
      ret.add(current);
    }
    return ret;
  }

  @Nullable
  private BulkStoragePartition<BlobT> considerBlobForPartitionInclusion(
      BlobT b,
      AtomicInteger partitionId,
      @Nullable BulkStoragePartition<BlobT> currentPartition,
      List<Partition> resultingPartitions) {

    log.trace("Considering blob {} for partition inclusion", b.getName());
    Pair<Long, Long> minMaxStamp = namingConvention.parseMinMaxTimestamp(b.getName());
    BulkStoragePartition<BlobT> res = currentPartition;
    if (partitionMaxTimeSpan > 0
        && currentPartition != null
        && Math.max(
                minMaxStamp.getSecond() - currentPartition.getMinTimestamp(),
                currentPartition.getMaxTimestamp() - minMaxStamp.getFirst())
            > partitionMaxTimeSpan) {
      // close current partition
      log.debug(
          "Closing partition {} due to max time span {} reached",
          currentPartition,
          partitionMaxTimeSpan);
      resultingPartitions.add(currentPartition);
      res = null;
    }
    if (res == null) {
      res =
          new BulkStoragePartition<>(
              partitionId.getAndIncrement(), minMaxStamp.getFirst(), minMaxStamp.getSecond());
    }
    res.add(b, minMaxStamp.getFirst(), minMaxStamp.getSecond());
    log.trace("Blob {} added to partition {}", b.getName(), res);
    if (res.size() >= partitionMinSize || res.getNumBlobs() >= partitionMaxNumBlobs) {
      resultingPartitions.add(res);
      return null;
    }
    return res;
  }

  @Override
  public ObserveHandle observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    TerminationContext terminationContext = new TerminationContext(observer);
    observeInternal(partitions, attributes, observer, terminationContext);
    return terminationContext.asObserveHandle();
  }

  private void observeInternal(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer,
      TerminationContext terminationContext) {

    Preconditions.checkArgument(
        partitions.stream().map(Partition::getId).distinct().count() == partitions.size(),
        "Passed partitions must be unique, got partitions %s",
        partitions);
    executor.submit(
        () -> {
          terminationContext.setRunningThread();
          try {
            Set<AttributeDescriptor<?>> attrs = new HashSet<>(attributes);
            AtomicBoolean stopProcessing = new AtomicBoolean(false);
            Iterator<Partition> iterator = partitions.iterator();
            while (iterator.hasNext()
                && !terminationContext.isCancelled()
                && !stopProcessing.get()) {
              Partition p = iterator.next();
              processSinglePartition(p, attrs, terminationContext, stopProcessing, observer);
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

  private void processSinglePartition(
      Partition partition,
      Set<AttributeDescriptor<?>> attrs,
      TerminationContext terminationContext,
      AtomicBoolean stopProcessing,
      BatchLogObserver observer) {

    @SuppressWarnings("unchecked")
    BulkStoragePartition<BlobT> part = (BulkStoragePartition<BlobT>) partition;
    for (BlobT blob : part.getBlobs()) {
      if (terminationContext.isCancelled() || stopProcessing.get()) {
        break;
      }
      try {
        runHandlingErrors(
            blob,
            () -> {
              log.info("Starting to observe {} from partition {}", blob, partition);
              try (Reader reader = fileFormat.openReader(createPath(blob), entity)) {
                for (StreamElement e : reader) {
                  if (stopProcessing.get() || terminationContext.isCancelled()) {
                    break;
                  }
                  if (attrs.contains(e.getAttributeDescriptor())) {
                    boolean cont =
                        observer.onNext(
                            e,
                            BatchLogObservers.withWatermarkSupplier(
                                partition, partition::getMinTimestamp));

                    if (!cont) {
                      stopProcessing.set(true);
                      break;
                    }
                  }
                }
              }
            });
      } catch (Exception ex) {
        throw new IllegalStateException(String.format("Failed to read from %s", blob), ex);
      }
    }
  }

  protected abstract void runHandlingErrors(BlobT blob, ThrowingRunnable runnable) throws Exception;

  protected abstract BlobPath<BlobT> createPath(BlobT blob);
}
