/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.bulk;

import cz.o2.proxima.util.Classpath;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** Utility class to be shared between various bulk storages. */
@Slf4j
public class FileFormatUtils {

  /**
   * Retrieve {@link cz.o2.proxima.direct.bulk.NamingConvention} from configuration.
   *
   * @param cfgPrefix config prefix
   * @param cfg config
   * @param rollPeriodMs roll period
   * @param format {@link FileFormat} file format
   * @return {@link NamingConvention} file naming convention
   */
  public static NamingConvention getNamingConvention(
      String cfgPrefix, Map<String, Object> cfg, long rollPeriodMs, FileFormat format) {

    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(InetAddress.getLocalHost().getHostName().getBytes(Charset.defaultCharset()));
      String prefix =
          new String(Base64.getEncoder().withoutPadding().encode(digest.digest()))
              .substring(0, 10)
              .replace('/', '-');

      final String namingConventionKey = cfgPrefix + "naming-convention";
      final String namingConventionFactoryKey = cfgPrefix + "naming-convention-factory";

      if (cfg.containsKey(namingConventionKey)) {
        log.warn(
            String.format(
                "Legacy configuration being used '%s' prefer to use configuration '%s'",
                namingConventionKey, namingConventionFactoryKey));
        return Classpath.newInstance(
            cfg.get(namingConventionKey).toString(), NamingConvention.class);
      } else {
        return Optional.ofNullable(cfg.get(namingConventionFactoryKey))
            .map(Object::toString)
            .map(cls -> Classpath.newInstance(cls, NamingConventionFactory.class))
            .orElse(new DefaultNamingConventionFactory())
            .create(cfgPrefix, cfg, Duration.ofMillis(rollPeriodMs), prefix, format.fileSuffix());
      }

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Get {@link cz.o2.proxima.direct.bulk.FileFormat} from configuration.
   *
   * @param cfgPrefix prefix to add to default config settings
   * @param cfg the configuration
   * @return {@link cz.o2.proxima.direct.bulk.FileFormat}
   */
  public static FileFormat getFileFormat(String cfgPrefix, Map<String, Object> cfg) {
    String format =
        Optional.ofNullable(cfg.get(cfgPrefix + "format")).map(Object::toString).orElse("binary");
    boolean gzip =
        Optional.ofNullable(cfg.get(cfgPrefix + "gzip"))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(false);
    return getFileFormatFromName(format, gzip);
  }

  static FileFormat getFileFormatFromName(String format, boolean gzip) {
    if ("binary".equals(format)) {
      return FileFormat.blob(gzip);
    }
    if ("json".equals(format)) {
      return FileFormat.json(gzip);
    }
    if ("parquet".equals(format)) {
      // just an shortcut for parquet file format
      format = "cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat";
    }
    try {
      return Classpath.newInstance(format, FileFormat.class);
    } catch (Exception ex) {
      throw new IllegalArgumentException("Unknown format " + format, ex);
    }
  }

  private FileFormatUtils() {
    // nop
  }
}
