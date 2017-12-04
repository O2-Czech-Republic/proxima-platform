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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * Class containing embedded HBase client.
 */
class HBaseClientWrapper {

  final URI uri;
  final TableName table;
  final Configuration conf;
  final byte[] family;
  Connection conn;
  Table client;

  HBaseClientWrapper(URI uri, Configuration conf, Map<String, Object> cfg) {
    this.uri = uri;
    table = TableName.valueOf(Util.getTable(uri));
    family = Util.getFamily(uri);
    this.conf = HBaseConfiguration.create(conf);
  }

  public URI getURI() {
    return uri;
  }

  void ensureClient() {
    if (this.conn == null || this.conn.isClosed()) {
      try {
        this.conn = ConnectionFactory.createConnection(conf);
        this.client = conn.getTable(table);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }


}
