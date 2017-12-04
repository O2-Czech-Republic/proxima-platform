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
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.URIUtil;
import cz.seznam.euphoria.shaded.guava.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Strings;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A cache for prepared CQL statements.
 */
public abstract class CacheableCQLFactory implements CQLFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CacheableCQLFactory.class);

  protected String tableName;
  @Nullable
  private String payloadCol;

  /**
   * A TTL value in seconds associated with each update or insert.
   */
  protected long ttl = 0;

  private final Map<AttributeDescriptor, PreparedStatement> ingestCache;
  private final Map<AttributeDescriptor, PreparedStatement> deleteCache;
  private final Map<AttributeDescriptor, PreparedStatement> deleteWildcardCache;
  private final Map<AttributeDescriptor, PreparedStatement> getCache;
  private final Map<AttributeDescriptor, PreparedStatement> listCache;
  private PreparedStatement listEntities;
  private PreparedStatement fetchToken;

  private static Map<AttributeDescriptor, PreparedStatement> createCache(long maxSize) {

    return new LinkedHashMap<AttributeDescriptor, PreparedStatement>() {

      @Override
      protected boolean removeEldestEntry(
          Map.Entry<AttributeDescriptor, PreparedStatement> eldest) {

        return size() > maxSize;
      }

      @Override
      public PreparedStatement get(Object key) {
        PreparedStatement ret = super.get(key);
        if (ret != null) {
          remove((AttributeDescriptor) key);
          put((AttributeDescriptor) key, ret);
        }
        return ret;
      }
    };

  }

  protected CacheableCQLFactory() {
    this.listCache = createCache(1000);
    this.getCache = createCache(1000);
    this.deleteWildcardCache = createCache(1000);
    this.deleteCache = createCache(1000);
    this.ingestCache = createCache(1000);
  }


  @Override
  public final void setup(URI uri, StringConverter<?> converter) {
    String path = uri.getPath();
    this.tableName = path;
    while (this.tableName.endsWith("/")) {
      this.tableName = this.tableName.substring(0, this.tableName.length() - 1);
    }
    if (tableName.length() <= 1) {
      throw new IllegalArgumentException("Invalid path in cassandra URI "
          + uri + ". The path represents name of table (including keyspace)");
    }
    this.tableName = tableName.substring(1);
    final Map<String, String> parsed;
    if (!Strings.isNullOrEmpty(uri.getQuery())) {
      parsed = URIUtil.parseQuery(uri);
      payloadCol = parsed.get("data");
    } else {
      parsed = Collections.emptyMap();
    }
    String tmp = parsed.get("ttl");
    if (tmp != null) {
      ttl = Long.valueOf(tmp);
    }
    try {
      setup(parsed, converter);
    } catch (RuntimeException ex) {
      throw new IllegalStateException("Cannot setup URI " + uri, ex);
    }
  }


  /**
   * Retrieve cached prepared statement for writing given data.
   * @param session the connection session
   * @param what data to ingest
   * @return the statement to use in order to store the data
   */
  protected PreparedStatement getPreparedStatement(
      Session session, StreamElement what) {

    if (what.isDelete()) {
      PreparedStatement cached;
      if (what.isDeleteWildcard()) {
        cached = deleteWildcardCache.get(what.getAttributeDescriptor());
        if (cached == null) {
          cached = prepare(session, createDeleteWildcardStatement(what));
          deleteWildcardCache.put(what.getAttributeDescriptor(), cached);
        }
      } else {
        cached = deleteCache.get(what.getAttributeDescriptor());
        if (cached == null) {
          cached = prepare(session, createDeleteStatement(what));
          deleteCache.put(what.getAttributeDescriptor(), cached);
        }
      }
      return cached;
    } else {
      PreparedStatement cached = ingestCache.get(what.getAttributeDescriptor());
      if (cached == null) {
        cached = prepare(session, createInsertStatement(what));
        ingestCache.put(what.getAttributeDescriptor(), cached);
      }
      return cached;
    }
  }


  /**
   * Retrieve cached prepared statement for getting data from cassandra
   * for given key and attribute.
   * @param session the connection session
   * @param key key of the entity
   * @param attribute the attribute to fetch
   * @param desc descriptor of the attribute
   * @return the statement to use in order to read the data
   */
  protected PreparedStatement getPreparedGetStatement(
      Session session, String attribute, AttributeDescriptor desc) {

    PreparedStatement cached = getCache.get(desc);
    if (cached == null) {
      cached = prepare(session, createGetStatement(attribute, desc));
      LOG.info("Prepared statement {}", cached);
      getCache.put(desc, cached);
    }
    return cached;
  }


  /**
   * Retrieve cached prepared statement for listing data by attribute prefix
   * (for wildcard attributes).
   * @param session the connection session
   * @param wildcardAttribute the wildcard attribute to list
   * @return the statement to use in order to read the data
   */
  protected PreparedStatement getPreparedListStatement(
      Session session,
      AttributeDescriptor wildcardAttribute) {

    PreparedStatement cached = listCache.get(wildcardAttribute);
    if (cached == null) {
      cached = prepare(session, createListStatement(wildcardAttribute));

      listCache.put(wildcardAttribute, cached);
    }
    return cached;
  }


  /**
   * Create statement to be prepared for given ingest.
   * This will be then stored in cache after call to {@code prepare}.
   */
  protected abstract String createInsertStatement(StreamElement element);


  /**
   * Create statement to be prepared for given ingest when deleting attribute.
   */
  protected abstract String createDeleteStatement(StreamElement element);


  /**
   * Create statement to delete wildcard attribute.
   */
  protected abstract String createDeleteWildcardStatement(StreamElement what);


  /**
   * Create get statement for key-attribute pair.
   * The statement must return only single field with value.
   */
  protected abstract String createGetStatement(
      String attribute,
      AttributeDescriptor desc);


  /**
   * Create list statement for key-wildcardAttribute pair.
   * The statement must return two fields - attribute, value.
   */
  protected abstract String createListStatement(AttributeDescriptor desc);


  /**
   * Create list statement for entity keys.
   * The statement must return single field - the entity key.
   */
  protected abstract String createListEntititiesStatement();


  /**
   * Create statement to fetch token for primary key.
   * The statement must return only the token as single field in single row.
   */
  protected abstract String createFetchTokenStatement();


  /**
   * Clear the cache (e.g. on reconnects).
   */
  protected void clearCache() {
    ingestCache.clear();
    deleteCache.clear();
    getCache.clear();
    listCache.clear();
    listEntities = null;
    fetchToken = null;
  }


  @VisibleForTesting
  String toUnderScore(String what) {
    StringBuilder sb = new StringBuilder();
    for (char c : what.toCharArray()) {
      if (c == '.') {
        sb.append("_");
      } else if (!Character.isUpperCase(c)) {
        sb.append(c);
      } else {
        sb.append("_").append(Character.toLowerCase(c));
      }
    }
    return sb.toString();
  }

  /**
   * Use configured payload column to read value bytes from cassandra table,
   * or use attribute name as column name.
   **/
  String toPayloadCol(AttributeDescriptor<?> attr) {
    if (payloadCol != null) {
      return payloadCol;
    }
    return attr.toAttributePrefix(false);
  }


  /**
   * Setup the factory from URI parameters passed in.
   * @param query the parsed URI query parameters
   * @param cfg the parsed parameters from configuration
   */
  protected void setup(
      Map<String, String> query,
      StringConverter<?> converter)
      throws IllegalArgumentException {

  }


  @Override
  public BoundStatement getListEntitiesStatement(
      Offsets.Token offset, int limit, Session session) {

    if (listEntities == null) {
      listEntities = prepare(session, createListEntititiesStatement());
    }
    limit = limit < 0 ? Integer.MAX_VALUE : limit;
    if (offset == null) {
      return listEntities.bind(Long.MIN_VALUE, limit);
    }
    return listEntities.bind(offset.getToken(), limit);
  }

  @Override
  public BoundStatement getFetchTokenStatement(String key, Session session) {
    if (fetchToken == null) {
      fetchToken = prepare(session, createFetchTokenStatement());
    }
    return fetchToken.bind(key);
  }

  static PreparedStatement prepare(Session session, String statement) {
    PreparedStatement ret = session.prepare(statement);
    LOG.info("Prepared statement {} as {}", statement, ret);
    return ret;
  }

}
