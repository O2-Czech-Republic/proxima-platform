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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.UriUtil;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** A cache for prepared CQL statements. */
@Slf4j
public abstract class CacheableCqlFactory implements CqlFactory {

  private static final long serialVersionUID = 1L;

  @Getter private EntityDescriptor entity;

  @Getter private String tableName;

  @Nullable private String payloadCol;

  /** The connection session in use. */
  @Getter @Nullable transient Session current = null;

  /** A TTL value in seconds associated with each update or insert. */
  protected long ttl = 0;

  private final Map<AttributeDescriptor<?>, PreparedStatement> ingestCache;
  private final Map<AttributeDescriptor<?>, PreparedStatement> deleteCache;
  private final Map<AttributeDescriptor<?>, PreparedStatement> deleteWildcardCache;
  private final Map<AttributeDescriptor<?>, PreparedStatement> getCache;
  private final Map<AttributeDescriptor<?>, PreparedStatement> listCache;

  @Nullable private transient PreparedStatement listEntities;

  @Nullable private transient PreparedStatement fetchToken;

  @Nullable private transient PreparedStatement listAllAttributes;

  private static Map<AttributeDescriptor<?>, PreparedStatement> createCache(long maxSize) {

    return new LinkedHashMap<AttributeDescriptor<?>, PreparedStatement>() {

      @Override
      protected boolean removeEldestEntry(
          Map.Entry<AttributeDescriptor<?>, PreparedStatement> eldest) {

        return size() > maxSize;
      }

      @Override
      public PreparedStatement get(Object key) {
        PreparedStatement ret = super.get(key);
        if (ret != null) {
          remove(key);
          put((AttributeDescriptor) key, ret);
        }
        return ret;
      }
    };
  }

  protected CacheableCqlFactory() {
    this.listCache = createCache(1000);
    this.getCache = createCache(1000);
    this.deleteWildcardCache = createCache(1000);
    this.deleteCache = createCache(1000);
    this.ingestCache = createCache(1000);
  }

  @Override
  public final void setup(EntityDescriptor entity, URI uri, StringConverter<?> converter) {

    this.entity = entity;
    this.tableName = uri.getPath();
    while (this.tableName.endsWith("/")) {
      this.tableName = this.tableName.substring(0, this.tableName.length() - 1);
    }
    if (tableName.length() <= 1) {
      throw new IllegalArgumentException(
          "Invalid path in cassandra URI "
              + uri
              + ". The path represents name of table (including keyspace)");
    }
    this.tableName = tableName.substring(1);
    final Map<String, String> parsed;
    if (!Strings.isNullOrEmpty(uri.getQuery())) {
      parsed = UriUtil.parseQuery(uri);
      payloadCol = parsed.get("data");
    } else {
      parsed = Collections.emptyMap();
    }
    String tmp = parsed.get("ttl");
    if (tmp != null) {
      ttl = Long.parseLong(tmp);
    }
    try {
      setup(parsed, converter);
    } catch (RuntimeException ex) {
      throw new IllegalStateException("Cannot setup URI " + uri, ex);
    }
  }

  /**
   * Setup the factory from URI parameters passed in.
   *
   * @param query the parsed URI query parameters
   * @param converter converter of payload to string
   */
  protected void setup(Map<String, String> query, StringConverter<?> converter) {}

  /**
   * Retrieve cached prepared statement for writing given data.
   *
   * @param session the connection session
   * @param what data to ingest
   * @return the statement to use in order to store the data
   */
  protected PreparedStatement getPreparedStatement(Session session, StreamElement what) {

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
   * Retrieve cached prepared statement for getting data from cassandra for given attribute.
   *
   * @param session the connection session
   * @param attribute the attribute to fetch
   * @param desc descriptor of the attribute
   * @return the statement to use in order to read the data
   */
  protected PreparedStatement getPreparedGetStatement(
      Session session, String attribute, AttributeDescriptor<?> desc) {

    return getCache.computeIfAbsent(
        desc,
        k -> {
          PreparedStatement prepared = prepare(session, createGetStatement(attribute, desc));
          log.info("Prepared statement {}", prepared);
          return prepared;
        });
  }

  /**
   * Retrieve cached prepared statement for listing data by attribute prefix (for wildcard
   * attributes).
   *
   * @param session the connection session
   * @param wildcardAttribute the wildcard attribute to list
   * @return the statement to use in order to read the data
   */
  protected PreparedStatement getPreparedListStatement(
      Session session, AttributeDescriptor<?> wildcardAttribute) {

    return listCache.computeIfAbsent(
        wildcardAttribute, k -> prepare(session, createListStatement(wildcardAttribute)));
  }

  protected PreparedStatement getPreparedListAllStatement(Session session) {
    if (listAllAttributes == null) {
      listAllAttributes = prepare(session, createListAllStatement(session));
    }
    return listAllAttributes;
  }

  /**
   * Create statement to be prepared for given ingest. This will be then stored in cache after call
   * to {@code prepare}.
   *
   * @param element the input element to create statement for
   * @return string representation of the CQL
   */
  protected abstract String createInsertStatement(StreamElement element);

  /**
   * Create statement to be prepared for given ingest when deleting attribute.
   *
   * @param element the input element to create statement for
   * @return string representation of the CQL
   */
  protected abstract String createDeleteStatement(StreamElement element);

  /**
   * Create statement to delete wildcard attribute.
   *
   * @param element the input element to create statement for
   * @return string representation of the CQL
   */
  protected abstract String createDeleteWildcardStatement(StreamElement element);

  /**
   * Create get statement for key-attribute pair. The statement must return only single field with
   * value.
   *
   * @param attribute the input attribute to create get for
   * @param desc the descriptor of the attribute
   * @return string representation of the CQL
   */
  protected abstract String createGetStatement(String attribute, AttributeDescriptor<?> desc);

  /**
   * Create list statement for key-wildcardAttribute pair. The statement must return two fields -
   * attribute, value.
   *
   * @param desc the descriptor of the attribute
   * @return string representation of the CQL
   */
  protected abstract String createListStatement(AttributeDescriptor<?> desc);

  /**
   * Create list statement for entity keys. The statement must return single field - the entity key.
   *
   * @return string representation of the CQL
   */
  protected abstract String createListEntitiesStatement();

  /**
   * Create statement to fetch token for primary key. The statement must return only the token as
   * single field in single row.
   *
   * @return string representation of the CQL
   */
  protected abstract String createFetchTokenStatement();

  /**
   * Create statement to list all attributes of this entity.
   *
   * @param session the connection session
   * @return string representation of the CQL
   */
  protected abstract String createListAllStatement(Session session);

  /** Clear the cache (e.g. on reconnects). */
  protected void clearCache() {
    ingestCache.clear();
    deleteCache.clear();
    getCache.clear();
    listCache.clear();
    listEntities = null;
    fetchToken = null;
    listAllAttributes = null;
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
   * Use configured payload column to read value bytes from cassandra table, or use attribute name
   * as column name.
   *
   * @param attr descriptor of attribute
   * @return string name of the payload column
   */
  String toPayloadCol(AttributeDescriptor<?> attr) {
    if (payloadCol != null) {
      return payloadCol;
    }
    return attr.toAttributePrefix(false);
  }

  @Override
  public BoundStatement getListEntitiesStatement(Offsets.Token offset, int limit, Session session) {

    if (listEntities == null) {
      listEntities = prepare(session, createListEntitiesStatement());
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

  void ensureSession(Session session) {
    if (this.current != session) {
      clearCache();
      current = session;
    }
  }

  static PreparedStatement prepare(Session session, String statement) {
    PreparedStatement ret = session.prepare(statement);
    log.info("Prepared statement {} as {}", statement, ret);
    return ret;
  }
}
