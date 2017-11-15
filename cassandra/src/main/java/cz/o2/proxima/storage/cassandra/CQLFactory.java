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
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A factory CQL queries for ingesting.
 */
public interface CQLFactory extends Serializable {

  /** Setup the factory from URI and given string converter. */
  public void setup(
      URI uri,
      StringConverter<?> converter);


  /**
   * Retrieve a CQL query to execute in order to ingest the request.
   * @returns the statement to execute. When empty, the ingest is silently discarded.
   */
  Optional<BoundStatement> getWriteStatement(
      StreamElement element,
      Session session);


  /**
   * Retrieve a CQL query to execute in order to read data.
   * @param key the primary key whose attribute to return
   * @param attribute the attribute to fetch
   * @param desc descriptor of the attribute
   * @param session the connection session
   */
  BoundStatement getReadStatement(
      String key,
      String attribute,
      AttributeDescriptor<?> desc,
      Session session);


  /**
   * Retrieve a CQL query to execute in order to list wildcard attributes.
   * The prefix is name of wildcard attribute shortened by two last characters
   * (.*).
   * @param key the primary key value (first part of the composite key)
   * @param wildcard the wildcard attribute to list
   * @param offset the offset to start from
   *                 this might be null (start from beginning)
   * @param limit maximal number of elements to list (-1 for all)
   * @param session the connection session
   */
  BoundStatement getListStatement(
      String key,
      AttributeDescriptor<?> wildcard,
      @Nullable Offsets.Raw offset,
      int limit,
      Session session);


  /**
   * Get statement for listing entities.
   */
  BoundStatement getListEntitiesStatement(
      @Nullable Offsets.Token offset,
      int limit,
      Session session);


  /**
   * Retrieve a bound statement to fetch a token for given entity.
   */
  BoundStatement getFetchTokenStatement(
      String key,
      Session session);


  /**
   * Retrieve a bound statement to scan data for given attribute
   * and partition.
   */
  Statement scanPartition(
      List<AttributeDescriptor<?>> attributes,
      CassandraPartition partition,
      Session session);

}
