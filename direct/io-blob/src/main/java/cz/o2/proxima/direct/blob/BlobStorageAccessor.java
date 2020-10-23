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

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileFormatUtils;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** A {@link DataAccessor} for blob storages. */
@Internal
public abstract class BlobStorageAccessor extends AbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  /** How often to roll the blob in milliseconds. */
  public static final String LOG_ROLL_INTERVAL = "log-roll-interval";

  /**
   * Maximal allowed lateness to wait before segment is rolled. Late data are put to separate blob
   * and flushed periodically with on time data.
   */
  public static final String ALLOWED_LATENESS_MS = "allowed-lateness-ms";

  /** Minimal size of partition in bytes. */
  public static final String PARTITION_SIZE = "partition.size";

  /** Maximal number of blobs to be put in single partition. */
  public static final String PARTITION_MAX_BLOBS = "partition.max-blobs";

  /** Maximal amount of time (in milliseconds) a partition containing multiple blobs can span. */
  public static final String PARTITION_MAX_TIME_SPAN_MS = "partition.max-time-span-ms";

  final Map<String, Object> cfg;

  protected BlobStorageAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.cfg = new HashMap<>(cfg);
  }

  public Map<String, Object> getCfg() {
    return Collections.unmodifiableMap(cfg);
  }

  @VisibleForTesting
  void setCfg(String key, Object value) {
    cfg.put(key, value);
  }

  public abstract FileSystem getTargetFileSystem();

  FileFormat getFileFormat() {
    return FileFormatUtils.getFileFormat("", getCfg());
  }

  public NamingConvention getNamingConvention() {
    return FileFormatUtils.getNamingConvention("", getCfg(), getRollPeriod(), getFileFormat());
  }

  public File getTmpDir() {
    File parent =
        Optional.ofNullable(cfg.get("tmp.dir"))
            .map(Object::toString)
            .map(File::new)
            .orElse(new File(System.getProperty("java.io.tmpdir")));
    return new File(parent, "blob-local-storage-" + UUID.randomUUID());
  }

  public long getRollPeriod() {
    return Optional.ofNullable(cfg.get(LOG_ROLL_INTERVAL))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(3600000L);
  }

  public long getAllowedLateness() {
    return Optional.ofNullable(cfg.get(ALLOWED_LATENESS_MS))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(5 * 60000L);
  }

  public long getPartitionMinSize() {
    return Optional.ofNullable(cfg.get(PARTITION_SIZE))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(100 * 1024 * 1024L);
  }

  public int getPartitionMaxNumBlobs() {
    return Optional.ofNullable(cfg.get(PARTITION_MAX_BLOBS))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(1000);
  }

  public long getPartitionMaxTimeSpanMs() {
    return Optional.ofNullable(cfg.get(PARTITION_MAX_TIME_SPAN_MS))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(-1L);
  }
}
