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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.direct.io.cassandra.CqlFactory.KvIterable;
import cz.o2.proxima.direct.io.cassandra.Offsets.Raw;
import java.nio.ByteBuffer;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** A {@link RandomAccessReader} for Cassandra. */
@Slf4j
class CassandraRandomReader extends AbstractStorage implements RandomAccessReader {

  private final CassandraDBAccessor accessor;

  CassandraRandomReader(CassandraDBAccessor accessor) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
  }

  @Override
  public <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

    CqlSession session = accessor.ensureSession();
    BoundStatement statement =
        accessor.getCqlFactory().getReadStatement(key, attribute, desc, session);
    final ResultSet result;
    try {
      result = accessor.execute(statement);
    } catch (AllNodesFailedException ex) {
      log.warn("Got {}, closing session.", AllNodesFailedException.class.getSimpleName(), ex);
      accessor.closeSession();
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException("Unable to execute query.", ex);
    }
    // the row has to have format (value)
    for (Row row : result) {
      ByteBuffer val = row.get(0, ByteBuffer.class);
      if (val != null) {
        byte[] rowValue = val.array();
        try {
          return Optional.ofNullable(
              accessor
                  .getCqlFactory()
                  .toKeyValue(
                      getEntityDescriptor(),
                      desc,
                      key,
                      attribute,
                      System.currentTimeMillis(),
                      new Offsets.Raw(attribute),
                      rowValue));
        } catch (Exception ex) {
          log.warn("Failed to read data from {}.{}", key, attribute, ex);
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public void scanWildcardAll(
      String key, RandomOffset offset, long stamp, int limit, Consumer<KeyValue<?>> consumer) {
    try {
      Offsets.Raw off = (Offsets.Raw) offset;
      CqlSession session = accessor.ensureSession();
      KvIterable<?> iter = accessor.getCqlFactory().getListAllStatement(key, off, limit, session);
      for (KeyValue<?> kv : iter.iterable(accessor)) {
        if (kv.getAttribute().compareTo(off.getRaw()) > 0) {
          if (limit-- == 0) {
            break;
          }
          consumer.accept(kv);
        }
      }
    } catch (Exception ex) {
      log.error("Failed to scan attributes of {}", key, ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer) {

    try {
      CqlSession session = accessor.ensureSession();
      BoundStatement statement =
          accessor
              .getCqlFactory()
              .getListStatement(key, wildcard, (Offsets.Raw) offset, limit, session);

      ResultSet result = accessor.execute(statement);
      // the row has to have the format (attribute, value)
      for (Row row : result) {
        Object attribute = row.getObject(0);
        ByteBuffer val = row.get(1, ByteBuffer.class);
        if (val != null) {
          byte[] rowValue = val.array();
          // by convention
          String name = wildcard.toAttributePrefix() + accessor.asString(attribute);
          @Nullable
          KeyValue<T> keyValue =
              accessor
                  .getCqlFactory()
                  .toKeyValue(
                      getEntityDescriptor(),
                      wildcard,
                      key,
                      name,
                      System.currentTimeMillis(),
                      new Raw(name),
                      rowValue);
          if (keyValue != null) {
            consumer.accept(keyValue);
          } else {
            log.error(
                "Failed to parse value for key {} attribute {} using class {}",
                key,
                name,
                wildcard.getValueSerializer().getClass());
          }
        }
      }
    } catch (Exception ex) {
      log.error("Failed to scan wildcard attribute {}", wildcard, ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void listEntities(
      RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

    CqlSession session = accessor.ensureSession();
    BoundStatement statement =
        accessor
            .getCqlFactory()
            .getListEntitiesStatement((Offsets.TokenOffset) offset, limit, session);

    try {
      ResultSet result = accessor.execute(statement);
      for (Row row : result) {
        String key = row.getString(0);
        long token = row.getLong(1);
        consumer.accept(Pair.of(new Offsets.TokenOffset(token), key));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Factory<CassandraRandomReader> asFactory() {
    final CassandraDBAccessor accessor = this.accessor;
    return repo -> new CassandraRandomReader(accessor);
  }

  @Override
  public void close() {
    // nop
  }

  @Override
  public RandomOffset fetchOffset(Listing type, String key) {
    try {
      switch (type) {
        case ATTRIBUTE:
          return new Offsets.Raw(key);

        case ENTITY:
          CqlSession session = accessor.ensureSession();
          ResultSet res =
              accessor.execute(accessor.getCqlFactory().getFetchTokenStatement(key, session));
          if (res.isFullyFetched()) {
            return new Offsets.TokenOffset(Long.MIN_VALUE);
          }
          return new Offsets.TokenOffset(res.one().getLong(0));
        default:
          throw new IllegalArgumentException("Unknown type of listing: " + type);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
