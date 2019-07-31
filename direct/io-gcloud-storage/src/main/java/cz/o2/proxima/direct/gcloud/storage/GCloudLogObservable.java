/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.internal.shaded.com.google.common.collect.Lists;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * {@link BatchLogObservable} for gcloud storage.
 */
@Slf4j
public class GCloudLogObservable
    extends GCloudClient
    implements BatchLogObservable {

  private static final Pattern BLOB_NAME_PATTERN = Pattern.compile(
      ".*/?[^-/]+-([0-9]+)_([0-9]+)[^/]*\\.blob[^/]*$");

  private static class GCloudStoragePartition implements Partition {

    @Getter
    private final List<Blob> blobs = new ArrayList<>();
    private final int id;
    private long minStamp;
    private long maxStamp;

    GCloudStoragePartition(int id, long minStamp, long maxStamp) {
      this.id = id;
      this.minStamp = minStamp;
      this.maxStamp = maxStamp;
    }

    void add(Blob b, long minStamp, long maxStamp) {
      blobs.add(b);
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
      return blobs.stream()
          .map(Blob::getSize)
          .reduce((a, b) -> a + b)
          .orElse(0L);
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

  private final long partitionMinSize;
  private final int partitionMaxNumBlobs;
  private final Factory<Executor> executorFactory;
  @Nullable
  private transient Executor executor = null;
  private long backoff = 100;

  public GCloudLogObservable(
      EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg,
      Factory<Executor> executorFactory) {

    super(entityDesc, uri, cfg);
    this.partitionMinSize = Optional.ofNullable(cfg.get("partition.size"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(100 * 1024 * 1024L);
    this.partitionMaxNumBlobs = Optional.ofNullable(cfg.get("partition.max-blobs"))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(1000);
    this.executorFactory = executorFactory;
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> ret = new ArrayList<>();
    Set<String> prefixes = convertStampsToPrefixes(this.path, startStamp, endStamp);
    AtomicInteger id = new AtomicInteger();
    AtomicReference<GCloudStoragePartition> current = new AtomicReference<>();
    prefixes.forEach(prefix -> {
      Page<Blob> p = client().list(this.bucket, BlobListOption.prefix(prefix));
      List<Blob> sorted = Lists.newArrayList(p.iterateAll());
      sorted.sort(Comparator.comparing(Blob::getName));
      for (Blob blob : sorted) {
        considerBlobForPartitionInclusion(
            startStamp, endStamp, blob, id, current, ret);
      }
    });
    if (current.get() != null) {
      ret.add(current.get());
    }
    log.debug(
        "Parsed partitions {} for startStamp {}, endStamp {}",
        ret, startStamp, endStamp);
    return ret;
  }

  private void considerBlobForPartitionInclusion(
      long startStamp, long endStamp, Blob b,
      AtomicInteger partitionId,
      AtomicReference<GCloudStoragePartition> currentPartition,
      List<Partition> resultingPartitions) {

    log.trace("Considering blob {} for partition inclusion", b.getName());
    Pair<Long, Long> minMaxStamp = parseMinMaxStamp(b.getName());
    if (isInRange(minMaxStamp, startStamp, endStamp)) {
      if (currentPartition.get() == null) {
        currentPartition.set(
            new GCloudStoragePartition(
                partitionId.getAndIncrement(),
                minMaxStamp.getFirst(),
                minMaxStamp.getSecond()));
      }
      currentPartition.get().add(b, minMaxStamp.getFirst(), minMaxStamp.getSecond());
      log.trace("Blob {} added to partition {}", b.getName(), currentPartition.get());
      if (currentPartition.get().size() >= partitionMinSize
          || currentPartition.get().getNumBlobs() >= partitionMaxNumBlobs) {
        resultingPartitions.add(currentPartition.getAndSet(null));
      }
    } else {
      log.trace(
          "Blob {} is not in range {} - {}", b.getName(), startStamp, endStamp);
    }
  }

  @VisibleForTesting
  static Set<String> convertStampsToPrefixes(
      String basePath, long startStamp, long endStamp) {

    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy/MM");
    // use TreeSet, so that prefixes are sorted, which will yield
    // partitions roughly sorted by timestamp
    Set<String> prefixes = new TreeSet<>();
    // remove trailing slashes
    while (basePath.endsWith("/")) {
      basePath = basePath.substring(0, basePath.length() - 1);
    }
    // and add exactly one
    basePath += "/";
    long t = startStamp;
    if (startStamp > Long.MIN_VALUE && endStamp < Long.MAX_VALUE) {
      LocalDateTime time = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(t), ZoneId.ofOffset("UTC", ZoneOffset.UTC));
      LocalDateTime end = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(endStamp), ZoneId.ofOffset("UTC", ZoneOffset.UTC));
      while (time.isBefore(end)) {
        prefixes.add(basePath + format.format(time));
        time = time.plusMonths(1);
      }
      prefixes.add(
          basePath + format.format(LocalDateTime.ofInstant(
              Instant.ofEpochMilli(endStamp), ZoneId.ofOffset("UTC", ZoneOffset.UTC))));
    } else {
      prefixes.add(basePath);
    }
    return prefixes;
  }

  @VisibleForTesting
  static @Nullable Pair<Long, Long> parseMinMaxStamp(String name) {
    Matcher matcher = BLOB_NAME_PATTERN.matcher(name);
    if (matcher.matches()) {
      long min = Long.parseLong(matcher.group(1));
      long max = Long.parseLong(matcher.group(2));
      return Pair.of(min, max);
    }
    log.warn("Skipping unparseable name {}", name);
    return null;
  }

  @VisibleForTesting
  static boolean isInRange(Pair<Long, Long> minMaxStamp, long startStamp, long endStamp) {
    return minMaxStamp != null
        && minMaxStamp.getFirst() <= endStamp
        && minMaxStamp.getSecond() >= startStamp;
  }

  @Override
  public void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    executor().execute(() -> {
      try {
        Set<AttributeDescriptor<?>> attrs = attributes
            .stream()
            .collect(Collectors.toSet());

        partitions.forEach(p -> {
          GCloudStoragePartition part = (GCloudStoragePartition) p;
          part.getBlobs().forEach(blob -> {
            for (;;) {
              final String name = blob.getName();
              log.debug("Starting to observe partition {}", p);
              try (InputStream s = Channels.newInputStream(blob.reader());
                  BinaryBlob.Reader reader = BinaryBlob.reader(
                      getEntityDescriptor(), name, s)) {

                reader.forEach(e -> {
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

  private void handleGeneralException(Exception ex, Blob blob) {
    log.warn("Exception while consuming blob {}", blob);
    throw new RuntimeException(ex);
  }

  private boolean handleResponseException(GoogleJsonResponseException ex, Blob blob) {
    switch (ex.getStatusCode()) {
      case 404:
        log.warn(
            "Received 404: {} on getting {}. Skipping gone object.",
            ex.getStatusMessage(), blob);
        break;
      case 429:
        log.warn("Received 429: {} on getting {}. Backoff {}.",
            ex.getStatusMessage(), blob, backoff);
        ExceptionUtils.unchecked(() -> Thread.currentThread().sleep(backoff));
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
