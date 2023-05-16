/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.cassandra;

import com.google.auto.service.AutoService;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import java.net.URI;

/** Storage descriptor for Apache Cassandra. */
@AutoService(DataAccessorFactory.class)
public class CassandraStorageDescriptor implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator operator, AttributeFamilyDescriptor family) {
    return new CassandraDBAccessor(family.getEntity(), family.getStorageUri(), family.getCfg());
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("cassandra") ? Accept.ACCEPT : Accept.REJECT;
  }
}
