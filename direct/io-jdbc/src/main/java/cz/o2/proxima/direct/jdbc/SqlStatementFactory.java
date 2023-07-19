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
package cz.o2.proxima.direct.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import java.io.Serializable;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface SqlStatementFactory extends Serializable {

  void setup(EntityDescriptor entity, URI uri, HikariDataSource dataSource) throws SQLException;

  default PreparedStatement get(
      HikariDataSource dataSource, AttributeDescriptor<?> desc, Object value) throws SQLException {

    throw new UnsupportedOperationException("Not implemented");
  }

  default PreparedStatement list(HikariDataSource dataSource, RandomOffset offset, int limit)
      throws SQLException {

    throw new UnsupportedOperationException("Not implemented");
  }

  default PreparedStatement update(HikariDataSource dataSource, StreamElement element)
      throws SQLException {

    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Scan the complete content of table.
   * @param dataSource the source
   * @return prepared statement
   */
  default PreparedStatement scanAll(HikariDataSource dataSource) {
    throw new UnsupportedOperationException("Not implemented");
  }

  default void close() {
    // NO OP
  }
}
