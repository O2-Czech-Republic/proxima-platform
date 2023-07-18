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
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;

@Slf4j
public abstract class JdbcBaseTest {
  final Repository repository =
      ConfigRepository.Builder.ofTest(ConfigFactory.defaultApplication()).build();

  final AttributeDescriptor<Byte[]> attr;
  final EntityDescriptor entity;
  final JdbcDataAccessor accessor;

  public JdbcBaseTest() throws URISyntaxException {
    attr =
        AttributeDescriptor.newBuilder(repository)
            .setEntity("dummy")
            .setName("attribute")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    entity = EntityDescriptor.newBuilder().setName("dummy").addAttribute(attr).build();
    Map<String, Object> config = new HashMap<>();
    config.put(
        JdbcDataAccessor.JDBC_SQL_QUERY_FACTORY_CFG, HsqldbSqlStatementFactory.class.getName());
    config.put(JdbcDataAccessor.JDBC_RESULT_CONVERTER_CFG, TestConverter.class.getName());
    // config.put(JdbcDataAccessor.JDBC_DRIVER_CFG, "org.hsqldb.jdbc.JDBCDataSource");
    config.put(JdbcDataAccessor.JDBC_USERNAME_CFG, "SA");
    config.put(JdbcDataAccessor.JDBC_PASSWORD_CFG, "");
    accessor =
        new JdbcDataAccessor(
            entity,
            URI.create(JdbcDataAccessor.JDBC_URI_STORAGE_PREFIX + "jdbc:hsqldb:mem:testdb"),
            config);
  }

  @Before
  public void setup() throws SQLException {
    HikariDataSource dataSource = accessor.borrowDataSource();
    try (Statement statement = dataSource.getConnection().createStatement()) {
      final String createTableSql =
          "CREATE TABLE DUMMYTABLE (id VARCHAR(255) NOT NULL, attribute VARCHAR(255) NOT NULL, updatedAt TIMESTAMP NOT NULL, PRIMARY KEY (id) )";
      statement.execute(createTableSql);
      log.info("Test table created.");
    }
  }

  @After
  public void cleanup() throws SQLException {
    HikariDataSource dataSource = accessor.borrowDataSource();
    try (Statement statement = dataSource.getConnection().createStatement()) {
      final String sql = "DROP TABLE DUMMYTABLE";
      statement.execute(sql);
      log.info("Test table dropped.");
    }
  }

  AtomicBoolean writeElement(JdbcDataAccessor accessor, StreamElement element) {
    try (AttributeWriterBase writer = accessor.newWriter()) {
      AtomicBoolean success = new AtomicBoolean(false);
      writer
          .online()
          .write(
              element,
              (status, exc) -> {
                success.set(status);
              });
      return success;
    }
  }
}
