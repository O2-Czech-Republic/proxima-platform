/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.bigtable;

import com.google.auto.service.AutoService;
import com.google.cloud.bigtable.hbase2_x.BigtableConnection;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.io.hbase.HBaseDataAccessor;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Storage descriptor for bigtable:// URIs.
 *
 * <p>BigTable storage uses URIs in the form of {@code
 * bigtable://<project>:<instance>/<table>?family=<family>} and stores data using HBase client in
 * BigTable instance {@code instance} of project {@code project} in table named {@code table} in
 * family {@code family}.
 *
 * <p>An optional parameter in URI called {@code v} can be used to distinguish two serialization
 * versions of data in BigTable cell:
 *
 * <ol>
 *   <li>{@code v=1} (default) stores the serialized bytes of a value in a cell directly
 *   <li>{@code v=2} uses protobuffer to store more metadata into the value
 * </ol>
 *
 * The {@code v=2} serialization format is required to support transactions on top of BigTable,
 * because the metadata preserves sequentialId (stored in {@code StreamElement#getSequentialId}).
 */
@AutoService(DataAccessorFactory.class)
public class BigTableStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator operator, AttributeFamilyDescriptor familyDescriptor) {
    return new HBaseDataAccessor(
        familyDescriptor.getEntity(),
        familyDescriptor.getStorageUri(),
        familyDescriptor.getCfg(),
        (m, u) -> {
          Configuration ret = new Configuration();
          ret.setClass("hbase.client.connection.impl", BigtableConnection.class, Connection.class);
          String authority = u.getAuthority();
          if (authority == null) {
            throw new IllegalArgumentException("Missing authority in URI " + u);
          }
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
