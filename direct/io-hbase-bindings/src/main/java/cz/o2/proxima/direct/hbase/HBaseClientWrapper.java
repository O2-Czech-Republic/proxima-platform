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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.Writable;

/** Class containing embedded HBase client. */
@Slf4j
class HBaseClientWrapper implements AutoCloseable {

  final URI uri;
  final byte[] serializedConf;
  final byte[] family;
  @Nullable Connection conn;
  @Nullable Table client;

  HBaseClientWrapper(URI uri, Configuration conf) {
    this.uri = uri;
    this.family = Util.getFamily(uri);
    this.serializedConf = serialize(HBaseConfiguration.create(conf));
  }

  public URI getUri() {
    return uri;
  }

  void ensureClient() {
    if (conn == null || conn.isClosed()) {
      try {
        if (client != null) {
          client.close();
        }
        if (conn != null && !conn.isClosed()) {
          conn.close();
        }
        conn = ConnectionFactory.createConnection(deserialize(serializedConf, new Configuration()));
        client = conn.getTable(tableName());
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
  public void close() {
    if (client != null) {
      Util.closeQuietly(client);
      client = null;
    }
    if (conn != null) {
      Util.closeQuietly(conn);
      conn = null;
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

  static <W extends Writable> W deserialize(byte[] bytes, W obj) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais)) {
      obj.readFields(dis);
      return obj;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
