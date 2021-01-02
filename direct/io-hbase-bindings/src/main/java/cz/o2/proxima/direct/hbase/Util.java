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
package cz.o2.proxima.direct.hbase;

import com.google.common.base.Preconditions;
import cz.o2.proxima.storage.UriUtil;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

/** Various utils. */
@Slf4j
class Util {

  private static final String FAMILY_QUERY = "family";

  static Configuration getConf(URI uri) {
    Configuration conf = HBaseConfiguration.create();
    List<String> paths = UriUtil.parsePath(uri);
    if (paths.size() > 1) {
      conf.set(
          HConstants.ZOOKEEPER_ZNODE_PARENT, String.join("/", paths.subList(0, paths.size() - 1)));
    }
    conf.set(HConstants.ZOOKEEPER_QUORUM, uri.getAuthority());
    return conf;
  }

  static String getTable(URI uri) {
    List<String> paths = UriUtil.parsePath(uri);
    Preconditions.checkArgument(!paths.isEmpty(), "Table cannot be empty in uri: {}!", uri);
    return paths.get(paths.size() - 1);
  }

  static byte[] getFamily(URI uri) {
    return Optional.ofNullable(UriUtil.parseQuery(uri).get(FAMILY_QUERY))
        .map(String::getBytes)
        .orElseThrow(() -> new IllegalArgumentException("Query " + FAMILY_QUERY + " is missing!"));
  }

  static void closeQuietly(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception ex) {
      log.warn("Failed to close {}. Ignored.", closeable, ex);
    }
  }

  static byte[] cloneArray(byte[] array, int offset, int length) {
    byte[] ret = new byte[length];
    System.arraycopy(array, offset, ret, 0, length);
    return ret;
  }

  private Util() {
    // nop
  }
}
