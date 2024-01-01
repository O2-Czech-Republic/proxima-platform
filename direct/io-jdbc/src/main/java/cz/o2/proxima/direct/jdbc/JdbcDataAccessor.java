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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage.SerializableAbstractStorage;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcDataAccessor extends SerializableAbstractStorage implements DataAccessor {
  static final String JDBC_URI_STORAGE_PREFIX = "jdbc://";
  static final String JDBC_DRIVER_CFG = "sql.driver-class-name";
  static final String JDBC_USERNAME_CFG = "sql.username";

  @SuppressWarnings("squid:S2068")
  static final String JDBC_PASSWORD_CFG = "sql.password";

  static final String JDBC_SQL_QUERY_FACTORY_CFG = "sql.query-factory";
  static final String JDBC_RESULT_CONVERTER_CFG = "sql.converter";

  private final String jdbcUri;
  private final String jdbcUsername;
  private final String jdbcPassword;
  private final String jdbcDriver;
  private final SqlStatementFactory sqlStatementFactory;

  @Getter private final Converter<?> resultConverter;

  private transient HikariDataSource dataSource;

  private int numSourceBorrows = 0;

  protected JdbcDataAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.jdbcUri = uri.toString().substring(JDBC_URI_STORAGE_PREFIX.length());

    jdbcDriver = cfg.getOrDefault(JDBC_DRIVER_CFG, "").toString();
    jdbcUsername = cfg.getOrDefault(JDBC_USERNAME_CFG, "").toString();
    jdbcPassword = cfg.getOrDefault(JDBC_PASSWORD_CFG, "").toString();

    if (!cfg.containsKey(JDBC_SQL_QUERY_FACTORY_CFG)) {
      log.error("Missing configuration param {}.", JDBC_SQL_QUERY_FACTORY_CFG);
      throw new IllegalStateException(
          String.format("Missing configuration param %s", JDBC_SQL_QUERY_FACTORY_CFG));
    }
    log.info("Using '{}' as {}.", cfg.get(JDBC_SQL_QUERY_FACTORY_CFG), JDBC_SQL_QUERY_FACTORY_CFG);
    sqlStatementFactory =
        Classpath.newInstance(
            cfg.get(JDBC_SQL_QUERY_FACTORY_CFG).toString(), SqlStatementFactory.class);
    try {
      getOrCreateDataSource();
      sqlStatementFactory.setup(entityDesc, uri, cfg, dataSource);
    } catch (SQLException e) {
      log.error(
          "Unable to setup {} from class {}.",
          JDBC_SQL_QUERY_FACTORY_CFG,
          cfg.get(JDBC_SQL_QUERY_FACTORY_CFG),
          e);
      throw new IllegalStateException(e.getMessage(), e);
    }

    if (!cfg.containsKey(JDBC_RESULT_CONVERTER_CFG)) {
      log.error("Missing configuration param {}.", JDBC_RESULT_CONVERTER_CFG);
      throw new IllegalStateException(
          String.format("Missing configuration param %s", JDBC_RESULT_CONVERTER_CFG));
    }
    log.info("Using '{}' as SqlStatementFactory.", cfg.get(JDBC_RESULT_CONVERTER_CFG));
    resultConverter =
        Classpath.newInstance(cfg.get(JDBC_RESULT_CONVERTER_CFG).toString(), Converter.class);
    resultConverter.setup();
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(newWriter());
  }

  @Override
  public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
    return Optional.of(newRandomAccessReader());
  }

  @Override
  public Optional<BatchLogReader> getBatchLogReader(Context context) {
    return Optional.of(newBatchLogReader(context));
  }

  @VisibleForTesting
  AttributeWriterBase newWriter() {
    return new JdbcOnlineAttributeWriter(
        this, this.sqlStatementFactory, getEntityDescriptor(), getUri());
  }

  @VisibleForTesting
  RandomAccessReader newRandomAccessReader() {
    return new JdbcOnlineAttributeReader(
        this, this.sqlStatementFactory, getEntityDescriptor(), getUri());
  }

  private BatchLogReader newBatchLogReader(Context context) {
    return new JdbcBatchLogReader(
        this,
        this.sqlStatementFactory,
        getEntityDescriptor(),
        getUri(),
        context::getExecutorService);
  }

  synchronized HikariDataSource borrowDataSource() {
    getOrCreateDataSource();
    numSourceBorrows += 1;
    return dataSource;
  }

  private void getOrCreateDataSource() {
    if (dataSource == null) {
      HikariConfig dataSourceConfig = new HikariConfig();
      dataSourceConfig.setPoolName(
          String.format("jdbc-pool-%s", this.getEntityDescriptor().getName()));
      if (!jdbcDriver.isEmpty()) {
        dataSourceConfig.setDataSourceClassName(jdbcDriver);
      }
      log.info("Creating JDBC storage from url: {}", jdbcUri);
      dataSourceConfig.setJdbcUrl(jdbcUri);
      if (!jdbcUsername.isEmpty()) {
        dataSourceConfig.setUsername(jdbcUsername);
      }
      if (!jdbcPassword.isEmpty()) {
        dataSourceConfig.setPassword(jdbcPassword);
      }
      dataSource = new HikariDataSource(dataSourceConfig);
    }
  }

  public synchronized void releaseDataSource() {
    if (--numSourceBorrows == 0) {
      dataSource.close();
      dataSource = null;
    }
  }
}
