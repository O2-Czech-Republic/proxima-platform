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
package cz.o2.proxima.gcloud.storage;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.batch.BatchLogObserver;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    GCloudStoragePartition(int id) {
      this.id = id;
    }

    void add(Blob b) {
      blobs.add(b);
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

  }

  private final long partitionMinSize;
  private final Factory<Executor> executorFactory;
  @Nullable
  private transient Executor executor = null;

  public GCloudLogObservable(
      EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg,
      Factory<Executor> executorFactory) {

    super(entityDesc, uri, cfg);
    this.partitionMinSize = Optional.ofNullable(cfg.get("partition.size"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(100 * 1024 * 1024L);
    this.executorFactory = executorFactory;
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> ret = new ArrayList<>();
    Set<String> prefixes = convertStampsToPrefixes(startStamp, endStamp);
    AtomicInteger id = new AtomicInteger();
    AtomicReference<GCloudStoragePartition> current = new AtomicReference<>();
    prefixes.forEach(prefix -> {
      Page<Blob> p = client().list(this.bucket, BlobListOption.prefix(prefix));
      for (Blob b : p.iterateAll()) {
        if (isInRange(b.getName(), startStamp, endStamp)) {
          if (current.get() == null) {
            current.set(new GCloudStoragePartition(id.getAndIncrement()));
          }
          current.get().add(b);
          if (current.get().size() >= partitionMinSize) {
            ret.add(current.getAndSet(null));
          }
        }
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

  private Set<String> convertStampsToPrefixes(long startStamp, long endStamp) {
    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy/MM");
    Set<String> prefixes = new HashSet<>();
    long t = startStamp;
    if (startStamp > Long.MIN_VALUE && endStamp < Long.MAX_VALUE) {
      LocalDateTime time = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(t), ZoneId.ofOffset("UTC", ZoneOffset.UTC));
      LocalDateTime end = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(endStamp), ZoneId.ofOffset("UTC", ZoneOffset.UTC));
      while (time.isBefore(end)) {
        prefixes.add(this.path + format.format(time));
        time = time.plusMonths(1);
      }
      prefixes.add(format.format(LocalDateTime.ofInstant(
          Instant.ofEpochMilli(endStamp), ZoneId.ofOffset("UTC", ZoneOffset.UTC))));
    } else {
      prefixes.add(this.path);
    }
    return prefixes;
  }

  @VisibleForTesting
  static boolean isInRange(String name, long startStamp, long endStamp) {
    Matcher matcher = BLOB_NAME_PATTERN.matcher(name);
    if (matcher.matches()) {
      long min = Long.parseLong(matcher.group(1));
      long max = Long.parseLong(matcher.group(2));
      return max >= startStamp && min <= endStamp;
    }
    log.warn("Skipping unparseable name {}", name);
    return false;
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
            final String name = blob.getName();
            try (InputStream s = Channels.newInputStream(blob.reader());
                BinaryBlob.Reader reader = BinaryBlob.reader(
                    getEntityDescriptor(), name, s)) {

              reader.forEach(e -> {
                if (attrs.contains(e.getAttributeDescriptor())) {
                  observer.onNext(e);
                }
              });
            } catch (IOException ex) {
              log.warn("Exception while consuming blob {}", blob);
              throw new RuntimeException(ex);
            }
          });
        });
        observer.onCompleted();
      } catch (Exception ex) {
        observer.onError(ex);
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
