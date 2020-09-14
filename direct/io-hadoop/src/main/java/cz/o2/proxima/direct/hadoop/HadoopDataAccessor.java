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
import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileFormatUtils;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/** {@code DataAccessor} for Hadoop Distributed FileSystem. */
@Slf4j
@ToString
public class HadoopDataAccessor implements DataAccessor {

  private static final long serialVersionUID = 1L;

  private static final String CFG_PREFIX = "hadoop";
  public static final String HADOOP_ROLL_INTERVAL = CFG_PREFIX + ".log-roll-interval";
  public static final String HADOOP_BATCH_PROCESS_SIZE_MIN = CFG_PREFIX + ".process-size.min";
  public static final String HADOOP_ALLOWED_LATENESS = CFG_PREFIX + ".allowed-lateness";
  public static final String TMP_FS = CFG_PREFIX + ".tmp.fs";

  static final long HADOOP_ROLL_INTERVAL_DEFAULT = TimeUnit.HOURS.toMillis(1);
  static final long HADOOP_BATCH_PROCESS_SIZE_MIN_DEFAULT = 1024 * 1024 * 100L; /* 100 MiB */

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
            HADOOP_ROLL_INTERVAL,
            cfg,
            o -> Long.valueOf(o.toString()),
            HADOOP_ROLL_INTERVAL_DEFAULT);
    this.batchProcessSize =
        getCfg(
            HADOOP_BATCH_PROCESS_SIZE_MIN,
            cfg,
            o -> Long.valueOf(o.toString()),
            HADOOP_BATCH_PROCESS_SIZE_MIN_DEFAULT);
    this.format = FileFormatUtils.getFileFormat(CFG_PREFIX + ".", cfg);
    this.namingConvention =
        FileFormatUtils.getNamingConvention(CFG_PREFIX + ".", cfg, rollInterval, format);
    this.temporaryNamingConvention = NamingConvention.prefixed("/_tmp", namingConvention);
    this.hadoopFs = new HadoopFileSystem(this);
    URI tmpFs =
        Optional.ofNullable(cfg.get(TMP_FS))
            .map(Object::toString)
            .map(URI::create)
            .orElse(hadoopFs.getUri());
    this.temporaryHadoopFs = new HadoopFileSystem(tmpFs, this, temporaryNamingConvention);
    this.allowedLateness =
        getCfg(HADOOP_ALLOWED_LATENESS, cfg, o -> Long.valueOf(o.toString()), 0L);

    Preconditions.checkArgument(
        rollInterval != 0, "Use non-negative %s got %s", HADOOP_ROLL_INTERVAL, rollInterval);
    Preconditions.checkArgument(
        allowedLateness >= 0,
        "Use non-negative %s got %s",
        HADOOP_ALLOWED_LATENESS,
        allowedLateness);
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
  public Optional<BatchLogReader> getBatchLogReader(Context context) {
    return Optional.of(new HadoopBatchLogReader(this, context));
  }

  private <T> T getCfg(
      String name, Map<String, Object> cfg, UnaryFunction<Object, T> convert, T defVal) {

    return Optional.ofNullable(cfg.get(name)).map(convert::apply).orElse(defVal);
  }

  FileSystem getFsFor(URI uri) {
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

  public URI getUriRemapped() {
    return HadoopStorage.remap(getUri());
  }
}
