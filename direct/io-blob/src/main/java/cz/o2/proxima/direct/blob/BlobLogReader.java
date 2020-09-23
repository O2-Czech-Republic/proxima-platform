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
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** {@link BatchLogReader} for blob storages. */
@Slf4j
public abstract class BlobLogReader<BlobT extends BlobBase, BlobPathT extends BlobPath<BlobT>>
    implements BatchLogReader {

  private static final long serialVersionUID = 1L;

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
  private final Executor executor;
  @Getter private final BlobStorageAccessor accessor;
  @Getter private final Context context;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public BlobLogReader(BlobStorageAccessor accessor, Context context) {
    this.entity = accessor.getEntityDescriptor();
    this.fs = accessor.getTargetFileSystem();
    this.fileFormat = accessor.getFileFormat();
    this.namingConvention = accessor.getNamingConvention();
    this.partitionMinSize = accessor.getPartitionMinSize();
    this.partitionMaxNumBlobs = accessor.getPartitionMaxNumBlobs();
    this.executor = context.getExecutorService();
    this.context = context;
    this.accessor = accessor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> ret = new ArrayList<>();
    AtomicInteger id = new AtomicInteger();
    AtomicReference<BulkStoragePartition<BlobT>> current = new AtomicReference<>();
    Stream<Path> paths = fs.list(startStamp, endStamp);
    paths.forEach(
        blob -> considerBlobForPartitionInclusion(((BlobPathT) blob).getBlob(), id, current, ret));
    if (current.get() != null) {
      ret.add(current.get());
    }
    return ret;
  }

  private void considerBlobForPartitionInclusion(
      BlobT b,
      AtomicInteger partitionId,
      AtomicReference<BulkStoragePartition<BlobT>> currentPartition,
      List<Partition> resultingPartitions) {

    log.trace("Considering blob {} for partition inclusion", b.getName());
    Pair<Long, Long> minMaxStamp = namingConvention.parseMinMaxTimestamp(b.getName());
    if (currentPartition.get() == null) {
      currentPartition.set(
          new BulkStoragePartition<>(
              partitionId.getAndIncrement(), minMaxStamp.getFirst(), minMaxStamp.getSecond()));
    }
    currentPartition.get().add(b, minMaxStamp.getFirst(), minMaxStamp.getSecond());
    log.trace("Blob {} added to partition {}", b.getName(), currentPartition.get());
    if (currentPartition.get().size() >= partitionMinSize
        || currentPartition.get().getNumBlobs() >= partitionMaxNumBlobs) {
      resultingPartitions.add(currentPartition.getAndSet(null));
    }
  }

  @Override
  public void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    Preconditions.checkArgument(
        partitions.stream().map(Partition::getId).distinct().count() == partitions.size(),
        "Passed partitions must be unique, got partitions %s",
        partitions);
    executor.execute(
        () -> {
          try {
            Set<AttributeDescriptor<?>> attrs = new HashSet<>(attributes);

            partitions
                .stream()
                .sorted(Comparator.comparing(Partition::getMinTimestamp))
                .forEach(
                    p -> {
                      @SuppressWarnings("unchecked")
                      BulkStoragePartition<BlobT> part = (BulkStoragePartition<BlobT>) p;
                      part.getBlobs()
                          .forEach(
                              blob -> {
                                try {
                                  runHandlingErrors(
                                      blob,
                                      () -> {
                                        log.info(
                                            "Starting to observe {} from partition {}", blob, p);
                                        try (Reader reader =
                                            fileFormat.openReader(createPath(blob), entity)) {
                                          reader.forEach(
                                              e -> {
                                                if (attrs.contains(e.getAttributeDescriptor())) {
                                                  observer.onNext(
                                                      e,
                                                      BatchLogObservers.withWatermarkSupplier(
                                                          p, p::getMinTimestamp));
                                                }
                                              });
                                        }
                                      });
                                } catch (Exception ex) {
                                  throw new IllegalStateException(
                                      String.format("Failed to read from %s", blob), ex);
                                }
                              });
                    });
            observer.onCompleted();
          } catch (Exception ex) {
            log.error("Failed to observe partitions {}", partitions, ex);
            if (observer.onError(ex)) {
              log.info("Restarting processing by request");
              observe(partitions, attributes, observer);
            }
          }
        });
  }

  protected abstract void runHandlingErrors(BlobT blob, ThrowingRunnable runnable) throws Exception;

  protected abstract BlobPath<BlobT> createPath(BlobT blob);
}
