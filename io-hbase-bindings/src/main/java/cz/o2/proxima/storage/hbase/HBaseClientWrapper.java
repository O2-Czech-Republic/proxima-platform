/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.io.Writable;

/**
 * Class containing embedded HBase client.
 */
@Slf4j
class HBaseClientWrapper implements AutoCloseable, Serializable {

  final URI uri;
  final byte[] serializedConf;
  final byte[] family;
  transient Connection conn;
  transient Table client;

  HBaseClientWrapper(URI uri, Configuration conf, Map<String, Object> cfg) {
    this.uri = uri;
    this.family = Util.getFamily(uri);
    this.serializedConf = serialize(HBaseConfiguration.create(conf));
  }

  public URI getUri() {
    return uri;
  }

  void ensureClient() {
    if (this.conn == null || this.conn.isClosed()) {
      try {
        if (this.client != null) {
          this.client.close();
        }
        if (this.conn != null) {
          this.conn.close();
        }
        this.conn = ConnectionFactory.createConnection(
            deserialize(serializedConf, new Configuration()));
        this.client = conn.getTable(tableName());
      } catch (IOException ex) {
        log.error("Error connecting to cluster", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  TableName tableName() {
    return TableName.valueOf(Util.getTable(uri));
  }

  @Override
  public void close() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  private static byte[] serialize(Writable obj) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos)) {

      obj.write(dos);
      dos.flush();
      return baos.toByteArray();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static <W extends Writable> W deserialize(byte[] bytes, W obj) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream dis = new DataInputStream(bais)) {
      obj.readFields(dis);
      return obj;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
