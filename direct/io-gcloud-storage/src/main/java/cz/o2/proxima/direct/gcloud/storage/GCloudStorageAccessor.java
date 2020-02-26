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

import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.util.Classpath;
import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.codec.binary.Hex;

/** A {@link DataAccessor} for gcloud storage. */
class GCloudStorageAccessor extends AbstractStorage implements DataAccessor {

  @Getter(AccessLevel.PACKAGE)
  final Map<String, Object> cfg;

  public GCloudStorageAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.cfg = cfg;
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(new BulkGCloudStorageWriter(getEntityDescriptor(), this, context));
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
    return Optional.of(new GCloudLogObservable(getEntityDescriptor(), this, context));
  }

  FileFormat getFileFormat() {
    String format = Optional.ofNullable(cfg.get("format")).map(Object::toString).orElse("binary");
    boolean gzip =
        Optional.ofNullable(cfg.get("gzip"))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(false);
    if ("binary".equals(format)) {
      return FileFormat.blob(gzip);
    }
    if ("json".equals(format)) {
      return FileFormat.json(gzip);
    }
    throw new IllegalArgumentException("Unknown format " + format);
  }

  NamingConvention getNamingConvention() {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(InetAddress.getLocalHost().getHostName().getBytes(Charset.defaultCharset()));
      String prefix = new String(Hex.encodeHex(digest.digest())).substring(0, 6);

      return Optional.ofNullable(cfg.get("naming-convention"))
          .map(Object::toString)
          .map(cls -> Classpath.newInstance(cls, NamingConvention.class))
          .orElse(
              NamingConvention.defaultConvention(
                  Duration.ofMillis(getRollPeriod()), getUri().getPath(), prefix));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public File getTmpDir() {
    return Optional.ofNullable(cfg.get("tmp.dir"))
        .map(Object::toString)
        .map(File::new)
        .orElse(
            new File(
                System.getProperty("java.io.tmpdir")
                    + File.separator
                    + "bulk-cloud-storage-"
                    + UUID.randomUUID()));
  }

  public long getRollPeriod() {
    return Optional.ofNullable(cfg.get("log-roll-interval"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(3600000L);
  }

  public int getBufferSize() {
    return Optional.ofNullable(cfg.get("buffer-size"))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(1024 * 1024);
  }

  public long getAllowedLateness() {
    return Optional.ofNullable(cfg.get("allowed-lateness-ms"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(5 * 60000L);
  }

  public long getFlushAttemptDelay() {
    return Optional.ofNullable(cfg.get("flush-delay-ms"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(5000L);
  }

  public long getPartitionMinSize() {
    return Optional.ofNullable(cfg.get("partition.size"))
        .map(Object::toString)
        .map(Long::valueOf)
        .orElse(100 * 1024 * 1024L);
  }

  public int getPartitionMaxNumBlobs() {
    return Optional.ofNullable(cfg.get("partition.max-blobs"))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(1000);
  }
}
