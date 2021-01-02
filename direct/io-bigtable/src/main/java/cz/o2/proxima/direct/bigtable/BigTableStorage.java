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
package cz.o2.proxima.direct.bigtable;

import com.google.cloud.bigtable.hbase1_x.BigtableConnection;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.hbase.HBaseDataAccessor;
import cz.o2.proxima.repository.EntityDescriptor;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/** Storage descriptor for bigtable:// URIs. */
public class BigTableStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator direct, EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    return new HBaseDataAccessor(
        entityDesc,
        uri,
        cfg,
        (m, u) -> {
          Configuration ret = new Configuration();
          ret.setClass("hbase.client.connection.impl", BigtableConnection.class, Connection.class);
          String authority = u.getAuthority();
          String[] parts = authority.split(":");
          if (parts.length != 2) {
            throw new IllegalArgumentException(
                "Invalid authority " + u.getAuthority() + ", expected <projectId>:<instanceId>");
          }
          ret.set("google.bigtable.project.id", parts[0]);
          ret.set("google.bigtable.instance.id", parts[1]);
          return ret;
        });
  }

  public Accept accepts(URI uri) {
    return uri.getScheme().equals("bigtable") ? Accept.ACCEPT : Accept.REJECT;
  }
}
