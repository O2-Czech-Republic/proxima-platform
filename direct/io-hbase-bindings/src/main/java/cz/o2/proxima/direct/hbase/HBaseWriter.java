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
package cz.o2.proxima.direct.hbase;

import static cz.o2.proxima.direct.hbase.Util.cloneArray;

import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

/** Writer to HBase. */
@Slf4j
@EqualsAndHashCode
class HBaseWriter extends HBaseClientWrapper implements OnlineAttributeWriter {

  private static final String DEL_BATCH_SIZE_CONF = "del-batch-size";
  private static final String FLUSH_COMMITS_CFG = "flush-commits";

  private final int batchSize;
  private final Map<String, Object> cfg;

  private boolean flushCommits;

  HBaseWriter(URI uri, Configuration conf, Map<String, Object> cfg) {
    super(uri, conf);
    batchSize =
        Optional.ofNullable(cfg.get(DEL_BATCH_SIZE_CONF))
            .map(o -> Integer.valueOf(o.toString()))
            .orElse(1000);
    flushCommits =
        Optional.ofNullable(cfg.get(FLUSH_COMMITS_CFG))
            .map(o -> Boolean.valueOf(o.toString()))
            .orElse(true);
    this.cfg = cfg;
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {

    ensureClient();
    byte[] key = data.getKey().getBytes(StandardCharsets.UTF_8);
    long stamp = data.getStamp();

    try {
      if (data.isDelete()) {
        if (data.isDeleteWildcard()) {
          // due to HBASE-5268 we have to first scan for all columns by prefix
          // and then delete them one by one
          deletePrefix(key, family, data.getAttributeDescriptor().toAttributePrefix(), stamp);
        } else {
          Delete del = new Delete(key, stamp);
          del.addColumns(family, data.getAttribute().getBytes(StandardCharsets.UTF_8), stamp);
          this.client.delete(del);
        }
      } else {
        String column = data.getAttribute();
        Put put = new Put(key, stamp);
        put.addColumn(family, column.getBytes(StandardCharsets.UTF_8), stamp, data.getValue());
        this.client.put(put);
      }
      if (flushCommits) {
        ((HTable) this.client).flushCommits();
      }
      statusCallback.commit(true, null);
    } catch (Exception ex) {
      log.error("Failed to write {}", data, ex);
      statusCallback.commit(false, ex);
    }
  }

  @Override
  public Factory<?> asFactory() {
    final URI uri = getUri();
    final Map<String, Object> cfg = this.cfg;
    final byte[] serializedConf = this.serializedConf;
    return repo -> new HBaseWriter(uri, deserialize(serializedConf, new Configuration()), cfg);
  }

  private void deletePrefix(byte[] key, byte[] family, String prefix, long stamp)
      throws IOException {

    Delete del = new Delete(key);
    Get get = new Get(key);
    get.addFamily(family);
    get.setFilter(new ColumnPrefixFilter(prefix.getBytes(StandardCharsets.UTF_8)));
    Scan scan = new Scan(get);
    scan.setAllowPartialResults(true);

    try (ResultScanner scanner = client.getScanner(scan)) {
      Result res;
      while ((res = scanner.next()) != null) {
        CellScanner cellScanner = res.cellScanner();
        while (cellScanner.advance()) {
          Cell c = cellScanner.current();
          if (c.getTimestamp() <= stamp) {
            del.addColumns(
                family,
                cloneArray(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength()),
                stamp);
          }
          if (del.size() >= batchSize) {
            client.delete(del);
            del = new Delete(key);
          }
        }
      }
    }
    if (!del.isEmpty()) {
      client.delete(del);
    }
    if (log.isDebugEnabled()) {
      log.debug(
          "Deleted prefix {} of key {} in family {} at {}",
          prefix,
          new String(key, StandardCharsets.UTF_8),
          new String(family, StandardCharsets.UTF_8),
          stamp);
    }
  }

  @Override
  void ensureClient() {
    super.ensureClient();
    if (!(client instanceof HTable)) {
      flushCommits = false;
    }
  }
}
