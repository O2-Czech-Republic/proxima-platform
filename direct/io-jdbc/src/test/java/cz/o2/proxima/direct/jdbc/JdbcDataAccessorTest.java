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

import cz.o2.proxima.core.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class JdbcDataAccessorTest extends JdbcBaseTest {

  @Test(expected = IllegalStateException.class)
  public void initializeWithoutSqlFactoryTest() {
    Map<String, Object> cfg =
        Collections.singletonMap(
            JdbcDataAccessor.JDBC_RESULT_CONVERTER_CFG, TestConverter.class.getName());
    new JdbcDataAccessor(
        entity,
        URI.create(JdbcDataAccessor.JDBC_URI_STORAGE_PREFIX + "jdbc:hsqldb:mem:testdb"),
        cfg);
  }

  @Test(expected = IllegalStateException.class)
  public void initializeWithoutConverterTest() {
    Map<String, Object> cfg =
        Collections.singletonMap(
            JdbcDataAccessor.JDBC_SQL_QUERY_FACTORY_CFG, HsqldbSqlStatementFactory.class.getName());
    new JdbcDataAccessor(
        entity,
        URI.create(JdbcDataAccessor.JDBC_URI_STORAGE_PREFIX + "jdbc:hsqldb:mem:testdb"),
        cfg);
  }

  @Test
  public void serializableTest() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(accessor);
  }
}
