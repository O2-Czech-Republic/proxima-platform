/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.storage.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CQL factory that stores data in other fields than what would
 * suggest the model itself.
 * The factory extracts column names and values from ingest by
 * provided extractors. The ingest is first modified by provided parser.
 */
public class TransformingCQLFactory<T> extends CacheableCQLFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TransformingCQLFactory.class);

  /** Parser of ingest to any intermediate type. */
  private final Function<StreamElement, T> parser;
  /** Name of the the primary key column .*/
  private final List<String> columns;
  /** Extract name value of key from ingest. */
  private final List<Function<Pair<String, T>, Object>> extractors;
  /** Filter for bad messages. */
  private final Function<T, Boolean> filter;

  protected TransformingCQLFactory(
      Function<StreamElement, T> parser,
      List<String> columns,
      List<Function<Pair<String, T>, Object>> extractors) {

    this(parser, columns, extractors, e -> true);
  }

  protected TransformingCQLFactory(
      Function<StreamElement, T> parser,
      List<String> columns,
      List<Function<Pair<String, T>, Object>> extractors,
      Function<T, Boolean> filter) {

    this.parser = Objects.requireNonNull(parser);
    this.columns = Objects.requireNonNull(columns);
    this.extractors = Objects.requireNonNull(extractors);
    if (this.columns.size() != this.extractors.size()
        || this.columns.isEmpty()) {
      throw new IllegalArgumentException(
          "Pass two non-empty same length lists");
    }
    this.filter = Objects.requireNonNull(filter);
  }

  @Override
  protected String createDeleteStatement(StreamElement ingest) {
    throw new UnsupportedOperationException(
        "Cannot delete by instance of " + getClass());
  }

  @Override
  protected String createDeleteWildcardStatement(StreamElement what) {
    throw new UnsupportedOperationException(
        "Cannot delete by instance of " + getClass());
  }

  @Override
  protected String createInsertStatement(StreamElement element) {
    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format("INSERT INTO %s (%s) VALUES (", tableName,
        Joiner.on(", ").join(columns)));
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
  public Optional<BoundStatement> getWriteStatement(
      StreamElement element, Session session) {

    if (element.getValue() == null) {
      LOG.warn("Throwing away delete ingest specified for {}", getClass());
      return Optional.empty();
    }
    PreparedStatement statement = getPreparedStatement(session, element);
    T parsed = parser.apply(element);
    if (filter.apply(parsed)) {
      List<Object> values = extractors.stream()
          .map(f -> f.apply(Pair.of(element.getKey(), parsed)))
          .collect(Collectors.toList());
      BoundStatement bound = statement.bind(values.toArray(new Object[values.size()]));
      bound.setLong(values.size(), element.getStamp() * 1000L);
      return Optional.of(bound);
    } else {
      LOG.debug("Ingest {} was filtered out.", element);
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
      String key,
      AttributeDescriptor wildcard,
      Offsets.Raw offset,
      int limit,
      Session session) {

    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  protected String createListEntititiesStatement() {
    throw new UnsupportedOperationException("Not supported.");
  }


  @Override
  protected String createFetchTokenStatement() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Statement scanPartition(
      List<AttributeDescriptor<?>> attributes,
      CassandraPartition partition,
      Session session) {

    throw new UnsupportedOperationException("Not supported.");
  }

}
