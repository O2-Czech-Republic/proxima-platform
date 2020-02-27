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
package cz.o2.proxima.direct.hadoop;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Utils;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.IOException;
import java.net.URI;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/** {@code DataAccessor} for Hadoop Distributed FileSystem. */
@Slf4j
@ToString
public class HadoopDataAccessor implements DataAccessor {

  public static final String HDFS_ROLL_INTERVAL = "hdfs.log-roll-interval";
  public static final String HDFS_BATCH_PROCESS_SIZE_MIN = "hdfs.process-size.min";
  public static final String HDFS_ALLOWED_LATENESS = "hdfs.allowed-lateness";

  static final long HDFS_ROLL_INTERVAL_DEFAULT = TimeUnit.HOURS.toMillis(1);
  static final long HDFS_BATCH_PROCESS_SIZE_MIN_DEFAULT = 1024 * 1024 * 100L; /* 100 MiB */
  static final String HDFS_DEFAULT_SEQUENCE_FILE_COMPRESSION_CODEC = "gzip";

  static final Pattern PART_FILE_PARSER = Pattern.compile("part-([0-9]+)_([0-9]+)-.+");
  static final DateTimeFormatter DIR_FORMAT = DateTimeFormatter.ofPattern("/yyyy/MM/");

  @Getter private final EntityDescriptor entityDesc;
  @Getter private final URI uri;

  private final Map<String, Object> cfg;

  @Getter private final long rollInterval;
  @Getter private final long batchProcessSize;
  @Getter private final FileFormat format;
  @Getter private final NamingConvention namingConvention;
  @Getter private final NamingConvention temporaryNamingConvention;
  @Getter private final HadoopFileSystem hadoopFs;
  @Getter private final HadoopFileSystem temporaryHadoopFs;
  @Getter private final long allowedLateness;

  public HadoopDataAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    this.entityDesc = entityDesc;
    this.uri = uri;
    this.cfg = cfg;
    this.rollInterval =
        getCfg(
            HDFS_ROLL_INTERVAL, cfg, o -> Long.valueOf(o.toString()), HDFS_ROLL_INTERVAL_DEFAULT);
    this.batchProcessSize =
        getCfg(
            HDFS_BATCH_PROCESS_SIZE_MIN,
            cfg,
            o -> Long.valueOf(o.toString()),
            HDFS_BATCH_PROCESS_SIZE_MIN_DEFAULT);
    this.format = Utils.getFileFormat("hdfs.", cfg);
    this.namingConvention = Utils.getNamingConvention("hdfs.", cfg, rollInterval, format);
    this.temporaryNamingConvention = NamingConvention.prefixed("/_tmp", namingConvention);
    this.hadoopFs = new HadoopFileSystem(this);
    this.temporaryHadoopFs = new HadoopFileSystem(this, temporaryNamingConvention);
    this.allowedLateness = getCfg(HDFS_ALLOWED_LATENESS, cfg, o -> Long.valueOf(o.toString()), 0L);
  }

  public Map<String, Object> getCfg() {
    return Collections.unmodifiableMap(cfg);
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return newWriter(context);
  }

  @VisibleForTesting
  Optional<AttributeWriterBase> newWriter(Context context) {
    return Optional.of(new HadoopBulkAttributeWriter(this, context));
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
    return Optional.of(new HadoopBatchLogObservable(this, context));
  }

  private <T> T getCfg(
      String name, Map<String, Object> cfg, UnaryFunction<Object, T> convert, T defVal) {

    return Optional.ofNullable(cfg.get(name)).map(convert::apply).orElse(defVal);
  }

  FileSystem getFs() {
    try {
      return FileSystem.get(uri, getHadoopConf());
    } catch (IOException ex) {
      throw new RuntimeException("Failed to get filesystem for URI: " + uri, ex);
    }
  }

  Configuration getHadoopConf() {
    Configuration conf = new Configuration();
    cfg.forEach((key, value) -> conf.set(key, value.toString()));
    return conf;
  }

  @Override
  public int hashCode() {
    return getUri().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HadoopDataAccessor) {
      HadoopDataAccessor other = (HadoopDataAccessor) obj;
      return other.getUri().equals(getUri());
    }
    return false;
  }
}
