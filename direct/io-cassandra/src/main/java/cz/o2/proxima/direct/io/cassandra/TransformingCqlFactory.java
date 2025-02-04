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
package cz.o2.proxima.direct.io.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
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
  public Optional<BoundStatement> getWriteStatement(StreamElement element, CqlSession session) {

    ensureSession(session);
    if (element.isDelete()) {
      log.warn("Throwing away delete ingest specified for {}", getClass());
      return Optional.empty();
    }
    PreparedStatement statement = getPreparedStatement(session, element);
    T parsed = parser.apply(element);
    if (Boolean.TRUE.equals(filter.apply(parsed))) {
      List<Object> values =
          extractors.stream()
              .map(f -> f.apply(Pair.of(element.getKey(), parsed)))
              .collect(Collectors.toList());
      if (values.stream().anyMatch(Objects::isNull)) {
        log.warn("Received null value while writing {}. Discarding.", element);
        return Optional.empty();
      }
      BoundStatement bound = statement.bind(values.toArray(new Object[values.size()]));
      bound = bound.setLong(values.size(), element.getStamp() * 1000L);
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
      String key, String attribute, AttributeDescriptor desc, CqlSession session) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BoundStatement getListStatement(
      String key, AttributeDescriptor wildcard, Offsets.Raw offset, int limit, CqlSession session) {

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
      List<AttributeDescriptor<?>> attributes, CassandraPartition partition, CqlSession session) {

    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public <V> KeyValue<V> toKeyValue(
      EntityDescriptor entityDescriptor,
      AttributeDescriptor<V> attributeDescriptor,
      String key,
      String attribute,
      long stamp,
      RandomOffset offset,
      byte[] serializedValue) {

    return KeyValue.of(
        entityDescriptor, attributeDescriptor, key, attribute, offset, serializedValue, stamp);
  }

  @Override
  protected String createListAllStatement(CqlSession session) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public <T> KvIterable<T> getListAllStatement(
      String key, Offsets.Raw offset, int limit, CqlSession session) {

    throw new UnsupportedOperationException("Not supported.");
  }
}
