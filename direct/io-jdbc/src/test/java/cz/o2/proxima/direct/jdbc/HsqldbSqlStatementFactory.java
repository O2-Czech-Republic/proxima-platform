/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbSqlStatementFactory implements SqlStatementFactory {
  private final String tableName = "DUMMYTABLE";
  private final String primaryKey = "id";
  private final String timestampCol = "updatedAt";

  @Override
  public void setup(
      EntityDescriptor entity, URI uri, Map<String, Object> cfg, HikariDataSource dataSource) {
    // currently no-op
  }

  public PreparedStatement get(
      HikariDataSource dataSource, AttributeDescriptor<?> desc, Object value) throws SQLException {

    PreparedStatement statement =
        dataSource
            .getConnection()
            .prepareStatement(
                String.format(
                    "SELECT %s,%s,%s FROM %s WHERE id = ? LIMIT 1",
                    desc.getName(), primaryKey, timestampCol, tableName));
    statement.setString(1, value.toString());
    return statement;
  }

  public PreparedStatement list(HikariDataSource dataSource, RandomOffset offset, int limit)
      throws SQLException {
    return dataSource
        .getConnection()
        .prepareStatement(
            String.format(
                "SELECT %s FROM %s ORDER BY %s LIMIT %d",
                primaryKey, tableName, primaryKey, limit));
  }

  @Override
  public PreparedStatement update(HikariDataSource dataSource, StreamElement element)
      throws SQLException {
    Connection connection = dataSource.getConnection();
    PreparedStatement statement;
    if (element.isDelete() || element.isDeleteWildcard()) {
      statement =
          connection.prepareStatement(
              String.format("DELETE FROM %s WHERE %s = ?", tableName, primaryKey));
      statement.setString(1, element.getKey());
      return statement;
    } else if (element.getValue() != null) {
      statement =
          connection.prepareStatement(
              String.format(
                  "MERGE INTO %s AS T USING (VALUES(?,?,?)) as vals(%s, %s, %s) ON T.%s = vals.%s "
                      + "WHEN MATCHED THEN UPDATE SET T.%s = vals.%s, T.%s = vals.%s "
                      + "WHEN NOT MATCHED THEN INSERT VALUES vals.%s, vals.%s, vals.%s",
                  tableName,
                  primaryKey,
                  element.getAttribute(),
                  timestampCol,
                  primaryKey,
                  primaryKey,
                  element.getAttribute(),
                  element.getAttribute(),
                  timestampCol,
                  timestampCol,
                  primaryKey,
                  element.getAttribute(),
                  timestampCol));

      statement.setString(1, element.getKey());
      statement.setString(2, new String(element.getValue()));
      statement.setTimestamp(3, Timestamp.from(Instant.ofEpochMilli(element.getStamp())));
      return statement;
    }
    return null;
  }

  @Override
  public PreparedStatement scanAll(HikariDataSource dataSource) {
    return ExceptionUtils.uncheckedFactory(
        () ->
            dataSource
                .getConnection()
                .prepareStatement(String.format("SELECT * FROM %s", tableName)));
  }
}
