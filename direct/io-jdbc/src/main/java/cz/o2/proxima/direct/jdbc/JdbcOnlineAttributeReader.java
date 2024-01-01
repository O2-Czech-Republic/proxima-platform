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
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcOnlineAttributeReader extends AbstractStorage implements RandomAccessReader {

  private final JdbcDataAccessor accessor;
  private final SqlStatementFactory sqlStatementFactory;
  private final HikariDataSource source;

  public JdbcOnlineAttributeReader(
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
  public RandomOffset fetchOffset(Listing type, String key) {
    return null;
  }

  @Override
  public <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

    @SuppressWarnings("unchecked")
    Converter<T> converter = (Converter<T>) accessor.getResultConverter();
    try (PreparedStatement statement = sqlStatementFactory.get(source, desc, key);
        ResultSet result = statement.executeQuery()) {

      log.debug("Executed statement {}", statement);
      if (!result.next()) {
        return Optional.empty();
      } else {
        return Optional.of(
            KeyValue.of(
                getEntityDescriptor(),
                desc,
                key,
                desc.getName(),
                new Offsets.Raw(converter.getKeyFromResult(result)),
                converter.getValueBytes(result, desc),
                converter.getTimestampFromResult(result)));
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          String.format("Failed to get key %s attribute %s", key, attribute), e);
    }
  }

  @Override
  public void scanWildcardAll(
      String key,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<?>> consumer) {

    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void listEntities(
      @Nullable RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

    try (PreparedStatement statement = sqlStatementFactory.list(source, offset, limit);
        ResultSet resultSet = statement.executeQuery()) {
      log.debug("Executed statement {}", statement);
      while (resultSet.next()) {
        String key = accessor.getResultConverter().getKeyFromResult(resultSet);
        consumer.accept(Pair.of(new Offsets.Raw(key), key));
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to list offset " + offset, e);
    }
  }

  @Override
  public Factory<?> asFactory() {
    final JdbcDataAccessor accessor = this.accessor;
    final SqlStatementFactory sqlStatementFactory = this.sqlStatementFactory;
    final EntityDescriptor entityDesc = this.getEntityDescriptor();
    final URI uri = this.getUri();
    return repo -> new JdbcOnlineAttributeReader(accessor, sqlStatementFactory, entityDesc, uri);
  }

  @Override
  public void close() {
    accessor.releaseDataSource();
  }
}
