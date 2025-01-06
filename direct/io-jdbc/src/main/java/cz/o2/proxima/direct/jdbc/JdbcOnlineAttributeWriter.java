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
package cz.o2.proxima.direct.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcOnlineAttributeWriter extends AbstractOnlineAttributeWriter {
  private final JdbcDataAccessor accessor;
  private final SqlStatementFactory sqlStatementFactory;
  private final HikariDataSource source;

  protected JdbcOnlineAttributeWriter(
      JdbcDataAccessor accessor,
      SqlStatementFactory sqlStatementFactory,
      EntityDescriptor entityDesc,
      URI uri) {
    super(entityDesc, uri);
    this.accessor = accessor;
    this.sqlStatementFactory = sqlStatementFactory;
    this.source = accessor.borrowDataSource();
  }

  @Override
  public void close() {
    accessor.releaseDataSource();
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    Preconditions.checkArgument(!data.getKey().isEmpty(), "Key should not be empty.");
    try (PreparedStatement statement = sqlStatementFactory.update(source, data)) {
      log.debug("Executing statement {}", statement);
      int result = statement.executeUpdate();
      statusCallback.commit(result != 0, null);
    } catch (SQLException e) {
      log.error("Error in writing data {}", e.getMessage(), e);
      statusCallback.commit(false, e);
    }
  }

  @Override
  public OnlineAttributeWriter.Factory<? extends OnlineAttributeWriter> asFactory() {
    final JdbcDataAccessor accessor = this.accessor;
    final SqlStatementFactory sqlStatementFactory = this.sqlStatementFactory;
    final EntityDescriptor entityDesc = this.getEntityDescriptor();
    final URI uri = this.getUri();
    return repo -> new JdbcOnlineAttributeWriter(accessor, sqlStatementFactory, entityDesc, uri);
  }
}
