/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * A CQL factory that stores data in other fields than what would suggest the model itself. The
 * factory extracts column names and values from ingest by provided extractors. The ingest is first
 * modified by provided parser.
 */
@Slf4j
public class TransformingCqlFactory<T extends Serializable> extends CacheableCqlFactory {

  private static final long serialVersionUID = 1L;

  /** Parser of ingest to any intermediate type. */
  private final UnaryFunction<StreamElement, T> parser;
  /** Name of the the primary key column . */
  private final List<String> columns;
  /** Extract name value of key from ingest. */
  private final List<UnaryFunction<Pair<String, T>, Object>> extractors;
  /** Filter for bad messages. */
  private final UnaryFunction<T, Boolean> filter;

  protected TransformingCqlFactory(
      UnaryFunction<StreamElement, T> parser,
      List<String> columns,
      List<UnaryFunction<Pair<String, T>, Object>> extractors) {

    this(parser, columns, extractors, e -> true);
  }

  protected TransformingCqlFactory(
      UnaryFunction<StreamElement, T> parser,
      List<String> columns,
      List<UnaryFunction<Pair<String, T>, Object>> extractors,
      UnaryFunction<T, Boolean> filter) {

    this.parser = Objects.requireNonNull(parser);
    this.columns = Objects.requireNonNull(columns);
    this.extractors = Objects.requireNonNull(extractors);
    if (this.columns.size() != this.extractors.size() || this.columns.isEmpty()) {
      throw new IllegalArgumentException("Pass two non-empty same length lists");
    }
    this.filter = Objects.requireNonNull(filter);
  }

  @Override
  protected String createDeleteStatement(StreamElement ingest) {
    throw new UnsupportedOperationException("Cannot delete by instance of " + getClass());
  }

  @Override
  protected String createDeleteWildcardStatement(StreamElement what) {
    throw new UnsupportedOperationException("Cannot delete by instance of " + getClass());
  }

  @Override
  protected String createInsertStatement(StreamElement element) {
    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format("INSERT INTO %s (%s) VALUES (", getTableName(), String.join(", ", columns)));
    String comma = "";
    for (int i = 0; i < extractors.size(); i++) {
      sb.append(comma);
      sb.append("?");
      comma = ", ";
    }
    sb.append(") USING TIMESTAMP ?");
    if (ttl > 0) {
      sb.append(" AND TTL ");
      sb.append(ttl);
    }
    return sb.toString();
  }

  @Override
  public Optional<BoundStatement> getWriteStatement(StreamElement element, Session session) {

    ensureSession(session);
    if (element.isDelete()) {
      log.warn("Throwing away delete ingest specified for {}", getClass());
      return Optional.empty();
    }
    PreparedStatement statement = getPreparedStatement(session, element);
    T parsed = parser.apply(element);
    if (Boolean.TRUE.equals(filter.apply(parsed))) {
      List<Object> values =
          extractors
              .stream()
              .map(f -> f.apply(Pair.of(element.getKey(), parsed)))
              .collect(Collectors.toList());
      if (values.stream().anyMatch(Objects::isNull)) {
        log.warn("Received null value while writing {}. Discarding.", element);
        return Optional.empty();
      }
      BoundStatement bound = statement.bind(values.toArray(new Object[values.size()]));
      bound.setLong(values.size(), element.getStamp() * 1000L);
      return Optional.of(bound);
    } else {
      log.debug("Ingest {} was filtered out.", element);
    }
    return Optional.empty();
  }

  @Override
  protected String createGetStatement(String attribute, AttributeDescriptor desc) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected String createListStatement(AttributeDescriptor wildcard) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BoundStatement getReadStatement(
      String key, String attribute, AttributeDescriptor desc, Session session) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BoundStatement getListStatement(
      String key, AttributeDescriptor wildcard, Offsets.Raw offset, int limit, Session session) {

    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  protected String createListEntitiesStatement() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  protected String createFetchTokenStatement() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Statement scanPartition(
      List<AttributeDescriptor<?>> attributes, CassandraPartition partition, Session session) {

    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  protected String createListAllStatement(Session session) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public <T> KvIterable<T> getListAllStatement(
      String key, Offsets.Raw offset, int limit, Session session) {

    throw new UnsupportedOperationException("Not supported.");
  }
}
