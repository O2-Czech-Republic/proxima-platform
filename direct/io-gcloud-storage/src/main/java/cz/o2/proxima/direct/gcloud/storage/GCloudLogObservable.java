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
package cz.o2.proxima.direct.gcloud.storage;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.Lists;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** {@link BatchLogObservable} for gcloud storage. */
@Slf4j
public class GCloudLogObservable extends GCloudClient implements BatchLogObservable, FileSystem {

  private static class GCloudStoragePartition implements Partition {

    @Getter private final List<Blob> blobs = new ArrayList<>();
    private final int id;
    private long minStamp;
    private long maxStamp;
    private long size;

    GCloudStoragePartition(int id, long minStamp, long maxStamp) {
      this.id = id;
      this.minStamp = minStamp;
      this.maxStamp = maxStamp;
    }

    void add(Blob b, long minStamp, long maxStamp) {
      blobs.add(b);
      size += b.getSize();
      this.minStamp = Math.min(this.minStamp, minStamp);
      this.maxStamp = Math.max(this.maxStamp, maxStamp);
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
  }

  private final FileFormat fileFormat;
  private final NamingConvention namingConvetion;
  private final long partitionMinSize;
  private final int partitionMaxNumBlobs;
  private final Factory<Executor> executorFactory;
  @Nullable private transient Executor executor = null;
  private long backoff = 100;

  public GCloudLogObservable(
      EntityDescriptor entityDesc, GCloudStorageAccessor accessor, Context context) {

    super(entityDesc, accessor.getUri(), accessor.getCfg());
    this.fileFormat = accessor.getFileFormat();
    this.namingConvetion = accessor.getNamingConvention();
    this.partitionMinSize = accessor.getPartitionMinSize();
    this.partitionMaxNumBlobs = accessor.getPartitionMaxNumBlobs();
    this.executorFactory = context::getExecutorService;
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> ret = new ArrayList<>();
    AtomicInteger id = new AtomicInteger();
    AtomicReference<GCloudStoragePartition> current = new AtomicReference<>();
    Stream<Path> paths = list(startStamp, endStamp);
    paths.forEach(
        blob -> considerBlobForPartitionInclusion(((BlobPath) blob).getBlob(), id, current, ret));
    if (current.get() != null) {
      ret.add(current.get());
    }
    return ret;
  }

  private List<Blob> getBlobsInRange(long startStamp, long endStamp) {
    List<Blob> ret = new ArrayList<>();
    Collection<String> prefixes = namingConvetion.prefixesOf(startStamp, endStamp);
    prefixes.forEach(
        prefix -> {
          Page<Blob> p = client().list(this.bucket, BlobListOption.prefix(prefix));
          List<Blob> sorted = Lists.newArrayList(p.iterateAll());
          sorted.sort(Comparator.comparing(Blob::getName));
          for (Blob blob : sorted) {
            if (namingConvetion.isInRange(blob.getName(), startStamp, endStamp)) {
              ret.add(blob);
            }
          }
        });
    log.debug("Parsed partitions {} for startStamp {}, endStamp {}", ret, startStamp, endStamp);
    return ret;
  }

  private void considerBlobForPartitionInclusion(
      Blob b,
      AtomicInteger partitionId,
      AtomicReference<GCloudStoragePartition> currentPartition,
      List<Partition> resultingPartitions) {

    log.trace("Considering blob {} for partition inclusion", b.getName());
    Pair<Long, Long> minMaxStamp = namingConvetion.parseMinMaxTimestamp(b.getName());
    if (currentPartition.get() == null) {
      currentPartition.set(
          new GCloudStoragePartition(
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

    executor()
        .execute(
            () -> {
              try {
                Set<AttributeDescriptor<?>> attrs = attributes.stream().collect(Collectors.toSet());

                partitions.forEach(
                    p -> {
                      GCloudStoragePartition part = (GCloudStoragePartition) p;
                      part.getBlobs()
                          .forEach(
                              blob -> {
                                while (true) {
                                  final String name = blob.getName();
                                  log.debug("Starting to observe partition {}", p);
                                  try (Reader reader =
                                      fileFormat.openReader(
                                          BlobPath.of(blob), getEntityDescriptor())) {
                                    reader.forEach(
                                        e -> {
                                          if (attrs.contains(e.getAttributeDescriptor())) {
                                            observer.onNext(e, p);
                                          }
                                        });
                                    backoff = 100;
                                  } catch (GoogleJsonResponseException ex) {
                                    if (handleResponseException(ex, blob)) {
                                      continue;
                                    }
                                  } catch (IOException ex) {
                                    handleGeneralException(ex, blob);
                                  }
                                  break;
                                }
                              });
                    });
                observer.onCompleted();
              } catch (Exception ex) {
                log.warn("Failed to observe partitions {}", partitions, ex);
                if (observer.onError(ex)) {
                  log.info("Restaring processing by request");
                  observe(partitions, attributes, observer);
                }
              }
            });
  }

  @Override
  public Stream<Path> list(long minTs, long maxTs) {
    return getBlobsInRange(minTs, maxTs).stream().map(BlobPath::of);
  }

  @Override
  public Path newPath(long ts) {
    return BlobPath.of(createBlob(namingConvetion.nameOf(ts)));
  }

  private void handleGeneralException(Exception ex, Blob blob) {
    log.warn("Exception while consuming blob {}", blob);
    throw new RuntimeException(ex);
  }

  private boolean handleResponseException(GoogleJsonResponseException ex, Blob blob) {
    switch (ex.getStatusCode()) {
      case 404:
        log.warn(
            "Received 404: {} on getting {}. Skipping gone object.", ex.getStatusMessage(), blob);
        break;
      case 429:
        log.warn(
            "Received 429: {} on getting {}. Backoff {}.", ex.getStatusMessage(), blob, backoff);
        ExceptionUtils.unchecked(() -> Thread.sleep(backoff));
        backoff *= 2;
        return true;
      default:
        handleGeneralException(ex, blob);
    }
    return false;
  }

  private Executor executor() {
    if (executor == null) {
      executor = executorFactory.apply();
    }
    return executor;
  }
}
