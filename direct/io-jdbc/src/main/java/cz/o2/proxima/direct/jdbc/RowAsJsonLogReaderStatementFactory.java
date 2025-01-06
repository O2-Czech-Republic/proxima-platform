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
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.net.URI;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class RowAsJsonLogReaderStatementFactory implements SqlStatementFactory {

  private static final Pattern TABLE_PATTERN = Pattern.compile("[a-zA-Z0-9-_]+");

  private String tableName;

  @Override
  public void setup(
      EntityDescriptor entity, URI uri, Map<String, Object> cfg, HikariDataSource dataSource) {

    tableName = Objects.requireNonNull(cfg.get("sql.table")).toString();
    Preconditions.checkArgument(
        TABLE_PATTERN.matcher(tableName).matches(), "Invalid table name in %s", uri);
  }

  @Override
  public PreparedStatement scanAll(HikariDataSource dataSource) {
    return ExceptionUtils.uncheckedFactory(
        () -> dataSource.getConnection().prepareStatement("SELECT * FROM " + tableName));
  }
}
