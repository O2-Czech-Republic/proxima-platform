/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.storage.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractBulkAttributeWriter;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.batch.BatchLogObserver;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attribute writer to HDFS as {@code SequenceFiles}.
 */
public class HdfsDataAccessor
    extends AbstractBulkAttributeWriter
    implements BatchLogObservable, DataAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsDataAccessor.class);

  public static final String HDFS_MIN_ELEMENTS_TO_FLUSH = "hdfs.min-elements-to-flush";
  public static final String HDFS_ROLL_INTERVAL = "hdfs.log-roll-interval";
  public static final String HDFS_BATCH_PROCESS_SIZE_MIN = "hdfs.process-size.min";

  private static final int HDFS_MIN_ELEMENTS_TO_FLUSH_DEFAULT = 500;
  private static final long HDFS_ROLL_INTERVAL_DEFAULT = TimeUnit.HOURS.toMillis(1);
  private static final long HDFS_BATCH_PROCES_SIZE_MIN_DEFAULT = 1024 * 1024 * 100; /* 100 MiB */

  private static final Pattern PART_FILE_PARSER = Pattern.compile("part-([0-9]+)_([0-9]+)-.+");

  private final Configuration conf;
  private final FileSystem fs;
  private final int minElementsToFlush;
  private final long rollInterval;
  private final long batchProcessSize;

  private final DateFormat DIR_FORMAT = new SimpleDateFormat("/YYYY/MM");

  private SequenceFile.Writer writer = null;
  private Path writerTmpPath = null;
  private long lastRoll = 0;
  private long elementsSinceFlush = 0;
  private long minElementStamp = Long.MAX_VALUE;
  private long maxElementStamp = Long.MIN_VALUE;
  private long monothonicTime = 0L;

  public HdfsDataAccessor(EntityDescriptor entityDesc,
      URI uri, Map<String, Object> cfg) throws IOException {

    super(entityDesc, uri);
    this.conf = toHadoopConf(cfg);
    this.fs = FileSystem.get(uri, conf);
    this.minElementsToFlush = getCfg(
        HDFS_MIN_ELEMENTS_TO_FLUSH, cfg,
        o -> Integer.valueOf(o.toString()),
        HDFS_MIN_ELEMENTS_TO_FLUSH_DEFAULT);
    this.rollInterval = getCfg(
        HDFS_ROLL_INTERVAL, cfg,
        o -> Long.valueOf(o.toString()),
        HDFS_ROLL_INTERVAL_DEFAULT);
    this.batchProcessSize = getCfg(
        HDFS_BATCH_PROCESS_SIZE_MIN, cfg,
        o -> Long.valueOf(o.toString()),
        HDFS_BATCH_PROCES_SIZE_MIN_DEFAULT);
  }

  private static Configuration toHadoopConf(Map<String, Object> cfg) {
    Configuration conf = new Configuration();
    cfg.entrySet().forEach(entry -> {
      conf.set(entry.getKey(), entry.getValue().toString());
    });
    return conf;
  }

  @Override
  public void rollback() {
    if (writer != null) {
      try {
        writer.close();
        writer = null;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    clearTmpDir();
  }

  @Override
  public void write(StreamElement data, CommitCallback commitCallback) {
    try {
      if (data.getStamp() > monothonicTime) {
        monothonicTime = data.getStamp();
      }
      if (writer == null) {
        clearTmpDir();
        openWriter(monothonicTime);
      }
      if (minElementStamp > data.getStamp()) {
        minElementStamp = data.getStamp();
      }
      if (maxElementStamp < data.getStamp()) {
        maxElementStamp = data.getStamp();
      }
      writer.append(
          new BytesWritable(toKey(data)),
          new TimestampedNullableBytesWritable(data.getStamp(), data.getValue()));
      if (++elementsSinceFlush > minElementsToFlush) {
        writer.hflush();
        writer.hsync();
        LOG.debug("Hflushed chunk {}", writerTmpPath);
        elementsSinceFlush = 0;
      }
      if (monothonicTime - lastRoll >= rollInterval) {
        // close the current writer
        writer.close();
        Path tmpLocation = toTmpLocation(lastRoll);
        Path target = toFinalLocation(lastRoll, minElementStamp, maxElementStamp);
        // move the .tmp file to final location
        if (!fs.exists(target.getParent())) {
          try {
            fs.mkdirs(target.getParent());
          } catch (IOException ex) {
            LOG.warn("Failed to mkdir {}, proceeding for now", target.getParent(), ex);
          }
        }
        fs.rename(tmpLocation, target);
        LOG.info("Completed chunk {}", target);
        writer = null;
        commitCallback.commit(true, null);
      }
    } catch (IOException | URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  private byte[] toKey(StreamElement data) {
    return (data.getKey() + "#" + data.getAttribute()).getBytes();
  }

  private void openWriter(long eventTime) {
    long part = eventTime / rollInterval * rollInterval;
    try {
      Path tmp = toTmpLocation(part);
      LOG.debug("Opening writer at {}", tmp);
      lastRoll = part;
      elementsSinceFlush = 0;
      writerTmpPath = tmp;
      minElementStamp = Long.MAX_VALUE;
      maxElementStamp = Long.MIN_VALUE;
      writer = SequenceFile.createWriter(conf,
          SequenceFile.Writer.file(tmp),
          SequenceFile.Writer.appendIfExists(false),
          SequenceFile.Writer.keyClass(BytesWritable.class),
          SequenceFile.Writer.valueClass(TimestampedNullableBytesWritable.class),
          SequenceFile.Writer.compression(
              SequenceFile.CompressionType.BLOCK,
              new GzipCodec()));
    } catch (IOException | URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  String toPartName(long part) throws UnknownHostException {
    return String.format("part-%d-%s", part, getLocalhost());
  }

  String toFinalName(long minStamp, long maxStamp) throws UnknownHostException {
    return String.format("part-%d_%d-%s", minStamp, maxStamp, getLocalhost());
  }

  @VisibleForTesting
  Path toFinalLocation(long part, long minStamp, long maxStamp)
      throws URISyntaxException, UnknownHostException {

    Date d = new Date(part);
    // place the final file in directory of (YYYY/MM)
    return new Path(
        new URI(getURI().toString() + DIR_FORMAT.format(d)
            + "/" + toFinalName(minStamp, maxStamp)));
  }

  @VisibleForTesting
  Path toTmpLocation(long part)
      throws UnknownHostException, URISyntaxException {

    // place the final file in directory /.tmp/
    return new Path(
        new URI(getURI().toString() + "/.tmp/"
            + toPartName(part)));
  }

  private <T> T getCfg(
      String name, Map<String, Object> cfg, Function<Object, T> convert, T defVal) {

    return Optional.ofNullable(cfg.get(name)).map(convert).orElse(defVal);
  }

  private void clearTmpDir() {
    try {
      Path tmpDir = new Path(getURI().toString() + "/.tmp/");
      if (fs.exists(tmpDir)) {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(tmpDir, false);
        String localhost = getLocalhost();
        while (files.hasNext()) {
          LocatedFileStatus file = files.next();
          if (matchesHostname(localhost, file)) {
            fs.delete(file.getPath(), false);
          }
        }
      }
    } catch (IOException ex) {
      LOG.warn("Failed to clean tmp dir", ex);
    }
  }

  private boolean matchesHostname(String hostname, LocatedFileStatus file) {
    return file.getPath().getName().endsWith("-" + hostname);
  }

  private String getLocalhost() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> partitions = new ArrayList<>();
    try {
      RemoteIterator<LocatedFileStatus> it;
      it = this.fs.listFiles(new Path(getURI().toString()), true);
      HdfsPartition current = null;
      while (it.hasNext()) {
        LocatedFileStatus file = it.next();
        if (file.isFile()) {
          if (current == null) {
            current = new HdfsPartition(partitions.size());
          }
          Map.Entry<Long, Long> minMaxStamp = getMinMaxStamp(file.getPath().getName());
          long min = minMaxStamp.getKey();
          long max = minMaxStamp.getValue();
          if (max >= startStamp && min <= endStamp) {
            current.add(file);
            if (current.size() > batchProcessSize) {
              partitions.add(current);
              current = null;
            }
          }
        }
      }
      if (current != null && current.size() > 0) {
        partitions.add(current);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return partitions;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    Thread thread = new Thread(() -> {
      boolean run = true;
      try {
        for (Iterator<Partition> it = partitions.iterator(); run && it.hasNext();) {
          HdfsPartition p = (HdfsPartition) it.next();
          for (Path f : p.getFiles()) {
            try {
              if (!f.getParent().getName().equals(".tmp")) {
                long element = 0L;
                BytesWritable key = new BytesWritable();
                TimestampedNullableBytesWritable value = new TimestampedNullableBytesWritable();
                try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(f))) {
                  while (reader.next(key, value)) {
                    observer.onNext(toStreamElement(f, element++, key, value), p);
                  }
                }
              }
            } catch (IOException ex) {
              throw new RuntimeException("Failed to read file " + f, ex);
            }
          }
        }
        observer.onCompleted();
      } catch (Throwable err) {
        observer.onError(err);
      }
    });
    thread.setDaemon(true);
    thread.setName(
        "hdfs-observer-"
            + getEntityDescriptor().getName()
            + "-" + attributes + "-" + partitions);
    thread.start();
  }

  private StreamElement toStreamElement(
      Path file,
      long number,
      BytesWritable key,
      TimestampedNullableBytesWritable value) {

    String strKey = new String(key.copyBytes());
    String[] split = strKey.split("#", 2);
    if (split.length != 2) {
      throw new IllegalArgumentException("Invalid input in key bytes " + strKey);
    }
    String rawKey = split[0];
    String attribute = split[1];

    AttributeDescriptor attributeDesc;
    attributeDesc = getEntityDescriptor().findAttribute(attribute).orElseThrow(
        () -> new IllegalArgumentException(
            "Attribute " + attribute + " does not exist in entity "
                + getEntityDescriptor().getName()));
    String uuid = file + ":" + number;
    if (value.hasValue()) {
      return StreamElement.update(getEntityDescriptor(), attributeDesc,
          uuid, rawKey, attribute, value.getStamp(), value.getValue());
    }
    return StreamElement.delete(getEntityDescriptor(), attributeDesc,
        uuid, rawKey, attribute, value.getStamp());
  }

  @VisibleForTesting
  static Map.Entry<Long, Long> getMinMaxStamp(String name) {
    Matcher matched = PART_FILE_PARSER.matcher(name);
    if (matched.find()) {
      return Maps.immutableEntry(Long.valueOf(matched.group(1)), Long.valueOf(matched.group(2)));
    }
    return Maps.immutableEntry(-1L, -1L);
  }

  @Override
  public Optional<AttributeWriterBase> getWriter() {
    return Optional.of(this);
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable() {
    return Optional.of(this);
  }

}