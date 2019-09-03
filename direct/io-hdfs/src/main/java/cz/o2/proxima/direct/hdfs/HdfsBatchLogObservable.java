/**
 * Copyright 2017-${Year} O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

/** Observable of data stored in {@code SequenceFiles} in HDFS. */
@Slf4j
public class HdfsBatchLogObservable implements BatchLogObservable {

  private final EntityDescriptor entityDesc;
  private final URI uri;

  @SuppressWarnings("squid:S1948")
  private final Map<String, Object> cfg;

  private final long batchProcessSize;

  private final Context context;
  private transient Executor executor;

  public HdfsBatchLogObservable(
      EntityDescriptor entityDesc,
      URI uri,
      Map<String, Object> cfg,
      Context context,
      long batchProcessSize) {

    this.entityDesc = entityDesc;
    this.cfg = cfg;
    this.uri = uri;
    this.context = context;
    this.batchProcessSize = batchProcessSize;
  }

  @Override
  @SuppressWarnings("squid:S00112")
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> partitions = new ArrayList<>();
    try {
      RemoteIterator<LocatedFileStatus> it;
      it = HdfsDataAccessor.getFs(uri, cfg).listFiles(new Path(uri.toString()), true);
      HdfsPartition current = new HdfsPartition(partitions.size());
      while (it.hasNext()) {
        LocatedFileStatus file = it.next();
        if (file.isFile()) {
          Map.Entry<Long, Long> minMaxStamp = getMinMaxStamp(file.getPath().getName());
          long min = minMaxStamp.getKey();
          long max = minMaxStamp.getValue();
          if (max >= startStamp && min <= endStamp) {
            current.add(file);
          }
          if (current.size() > batchProcessSize) {
            partitions.add(current);
            current = new HdfsPartition(partitions.size());
          }
        }
      }
      if (current.size() > 0) {
        partitions.add(current);
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to retrieve partitions", ex);
    }
    return partitions;
  }

  @Override
  @SuppressWarnings({"unchecked", "squid:S1181"})
  public void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    if (executor == null) {
      executor = context.getExecutorService();
    }

    executor.execute(
        () -> {
          boolean run = true;
          try {
            for (Iterator<Partition> it = partitions.iterator(); run && it.hasNext(); ) {
              HdfsPartition p = (HdfsPartition) it.next();
              for (URI f : p.getFiles()) {
                processFile(observer, p, new Path(f));
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

  @SuppressWarnings("squid:S00112")
  private void processFile(BatchLogObserver observer, HdfsPartition p, Path f) {
    try {
      if (!f.getParent().getName().equals(".tmp")) {
        long element = 0L;
        BytesWritable key = new BytesWritable();
        TimestampedNullableBytesWritable value = new TimestampedNullableBytesWritable();
        try (SequenceFile.Reader reader =
            new SequenceFile.Reader(
                HdfsDataAccessor.toHadoopConf(cfg), SequenceFile.Reader.file(f))) {

          while (reader.next(key, value)) {
            observer.onNext(toStreamElement(f, element++, key, value), p);
          }
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to read file " + f, ex);
    }
  }

  private StreamElement toStreamElement(
      Path file, long number, BytesWritable key, TimestampedNullableBytesWritable value) {

    String strKey = new String(key.copyBytes());
    String[] split = strKey.split("#", 2);
    if (split.length != 2) {
      throw new IllegalArgumentException("Invalid input in key bytes " + strKey);
    }
    String rawKey = split[0];
    String attribute = split[1];

    AttributeDescriptor attributeDesc;
    attributeDesc =
        entityDesc
            .findAttribute(attribute)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Attribute "
                            + attribute
                            + " does not exist in entity "
                            + entityDesc.getName()));
    String uuid = file + ":" + number;
    if (value.hasValue()) {
      return StreamElement.update(
          entityDesc, attributeDesc, uuid, rawKey, attribute, value.getStamp(), value.getValue());
    }
    return StreamElement.delete(
        entityDesc, attributeDesc, uuid, rawKey, attribute, value.getStamp());
  }

  @VisibleForTesting
  static Map.Entry<Long, Long> getMinMaxStamp(String name) {
    Matcher matched = HdfsDataAccessor.PART_FILE_PARSER.matcher(name);
    if (matched.find()) {
      return Maps.immutableEntry(Long.valueOf(matched.group(1)), Long.valueOf(matched.group(2)));
    }
    return Maps.immutableEntry(-1L, -1L);
  }
}
