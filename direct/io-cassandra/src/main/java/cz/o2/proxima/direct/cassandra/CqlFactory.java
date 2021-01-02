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
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/** A factory CQL queries for data access. */
public interface CqlFactory extends Serializable {

  /** Interface for iteration over returned results returning {@link KeyValue}s. */
  interface KvIterable<T> {
    Iterable<KeyValue<T>> iterable(CassandraDBAccessor accessor);
  }

  /**
   * Setup the factory from URI and given string converter.
   *
   * @param entity descriptor of entity
   * @param uri URI of the source
   * @param converter payload to string converter
   */
  void setup(EntityDescriptor entity, URI uri, StringConverter<?> converter);

  /**
   * Retrieve a CQL query to execute in order to ingest the request.
   *
   * @param element input data
   * @param session current session
   * @return the statement to execute. When empty, the ingest is silently discarded.
   */
  Optional<BoundStatement> getWriteStatement(StreamElement element, Session session);

  /**
   * Retrieve a CQL query to execute in order to read data.
   *
   * @param key the primary key whose attribute to return
   * @param attribute the attribute to fetch
   * @param desc descriptor of the attribute
   * @param session the connection session
   * @return the statement to execute
   */
  BoundStatement getReadStatement(
      String key, String attribute, AttributeDescriptor<?> desc, Session session);

  /**
   * Retrieve wrapped statement to execute to list all attributes of given key.
   *
   * @param key key to list attributes of
   * @param offset offset to start from (return next attribute)
   * @param limit maximum number of items to return
   * @param session the connection session
   * @param <T> type of {@link KvIterable}
   * @return iterable over keyvalues
   */
  <T> KvIterable<T> getListAllStatement(
      String key, @Nullable Offsets.Raw offset, int limit, Session session);

  /**
   * Retrieve a CQL query to execute in order to list wildcard attributes. The prefix is name of
   * wildcard attribute shortened by two last characters (.*).
   *
   * @param key the primary key value (first part of the composite key)
   * @param wildcard the wildcard attribute to list
   * @param offset the offset to start from this might be null (start from beginning)
   * @param limit maximal number of elements to list (-1 for all)
   * @param session the connection session
   * @return the statement to execute
   */
  BoundStatement getListStatement(
      String key,
      AttributeDescriptor<?> wildcard,
      @Nullable Offsets.Raw offset,
      int limit,
      Session session);

  /**
   * Get statement for listing entities.
   *
   * @param offset offset of the query
   * @param limit maximal number of elements to list (-1 for all)
   * @param session connection session
   * @return the statement to execute
   */
  BoundStatement getListEntitiesStatement(
      @Nullable Offsets.Token offset, int limit, Session session);

  /**
   * Retrieve a bound statement to fetch a token for given entity.
   *
   * @param key key to fetch token for
   * @param session connection session
   * @return the statement to execute
   */
  BoundStatement getFetchTokenStatement(String key, Session session);

  /**
   * Retrieve a bound statement to scan data for given attribute and partition.
   *
   * @param attributes list of attributes to scan
   * @param partition the partition to scan
   * @param session connection session
   * @return the statement to execute
   */
  Statement scanPartition(
      List<AttributeDescriptor<?>> attributes, CassandraPartition partition, Session session);
}
