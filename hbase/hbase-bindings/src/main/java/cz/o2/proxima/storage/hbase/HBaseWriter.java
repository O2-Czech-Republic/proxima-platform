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
package cz.o2.proxima.storage.hbase;

import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer to HBase.
 */
class HBaseWriter extends HBaseClientWrapper implements OnlineAttributeWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseWriter.class);
  private static final String DEL_BATCH_SIZE_CONF = "del-batch-size";
  private static final String FLUSH_COMMITS_CFG = "flush-commits";

  private final int batchSize;
  private final boolean flushCommits;

  HBaseWriter(URI uri, Configuration conf, Map<String, Object> cfg) {
    super(uri, conf, cfg);
    batchSize = Optional.ofNullable(cfg.get(DEL_BATCH_SIZE_CONF))
        .map(o -> Integer.valueOf(o.toString()))
        .orElse(1000);
    flushCommits = Optional.ofNullable(cfg.get(FLUSH_COMMITS_CFG))
        .map(o -> Boolean.valueOf(o.toString()))
        .orElse(true);
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {

    ensureClient();
    byte[] key = data.getKey().getBytes();
    long stamp = data.getStamp();

    try {
      if (data.isDelete()) {
        if (data.isDeleteWildcard()) {
          // due to HBASE-5268 we have to first scan for all columns by prefix
          // and then delete them one by one
          deletePrefix(
              key, family,
              data.getAttributeDescriptor().toAttributePrefix(),
              stamp);
        } else {
          Delete del = new Delete(key, stamp);
          del.addColumns(family, data.getAttribute().getBytes(), stamp);
          this.client.delete(del);
        }
      } else {
        String column = data.getAttribute();
        Put put = new Put(key, stamp);
        put.addColumn(family, column.getBytes(), data.getValue());
        this.client.put(put);
      }
      if (flushCommits) {
        ((HTable) this.client).flushCommits();
      }
      statusCallback.commit(true, null);
    } catch (Exception ex) {
      LOG.error("Failed to write {}", data, ex);
      statusCallback.commit(false, ex);
    }
  }

  private void deletePrefix(
      byte[] key, byte[] family, String prefix, long stamp)
      throws IOException {

    Delete del = new Delete(key);
    Get get = new Get(key);
    get.addFamily(family);
    get.setFilter(new ColumnPrefixFilter(prefix.getBytes()));
    Scan scan = new Scan(get);
    scan.setAllowPartialResults(true);

    try (ResultScanner scanner = client.getScanner(scan)) {
      Result res;
      while ((res = scanner.next()) != null) {
        CellScanner cellScanner = res.cellScanner();
        while (cellScanner.advance()) {
          Cell c = cellScanner.current();
          del.addColumns(family, c.getQualifier(), stamp);
          if (del.size() >= batchSize) {
            client.delete(del);
            del = new Delete(key);
          }
        }
      }
    }
    if (del.size() > 0) {
      client.delete(del);
    }
  }

}
