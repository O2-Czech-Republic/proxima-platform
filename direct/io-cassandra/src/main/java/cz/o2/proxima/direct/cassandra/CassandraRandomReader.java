/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import cz.o2.proxima.direct.cassandra.CqlFactory.KvIterable;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.util.Pair;
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
  public synchronized <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

    Session session = accessor.ensureSession();
    BoundStatement statement =
        accessor.getCqlFactory().getReadStatement(key, attribute, desc, session);
    ResultSet result;
    try {
      result = accessor.execute(statement);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    // the row has to have format (value)
    for (Row row : result) {
      ByteBuffer val = row.getBytes(0);
      if (val != null) {
        byte[] rowValue = val.array();

        try {
          return Optional.of(
              KeyValue.of(
                  getEntityDescriptor(),
                  desc,
                  key,
                  attribute,
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
      Session session = accessor.ensureSession();
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
  @SuppressWarnings("unchecked")
  public synchronized <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer) {

    try {
      Session session = accessor.ensureSession();
      BoundStatement statement =
          accessor
              .getCqlFactory()
              .getListStatement(key, wildcard, (Offsets.Raw) offset, limit, session);

      ResultSet result = accessor.execute(statement);
      // the row has to have format (attribute, value)
      for (Row row : result) {
        Object attribute = row.getObject(0);
        ByteBuffer val = row.getBytes(1);
        if (val != null) {
          byte[] rowValue = val.array();
          // by convention
          String name = wildcard.toAttributePrefix() + accessor.getConverter().asString(attribute);

          Optional<T> parsed = wildcard.getValueSerializer().deserialize(rowValue);

          if (parsed.isPresent()) {
            consumer.accept(
                KeyValue.of(
                    getEntityDescriptor(),
                    wildcard,
                    key,
                    name,
                    new Offsets.Raw(name),
                    parsed.get(),
                    rowValue));
          } else {
            log.error("Failed to parse value for key {} attribute {}.{}", key, wildcard, attribute);
          }
        }
      }
    } catch (Exception ex) {
      log.error("Failed to scan wildcard attribute {}", wildcard, ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public synchronized void listEntities(
      RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

    Session session = accessor.ensureSession();
    BoundStatement statement =
        accessor.getCqlFactory().getListEntitiesStatement((Offsets.Token) offset, limit, session);

    try {
      ResultSet result = accessor.execute(statement);
      for (Row row : result) {
        String key = row.getString(0);
        Token token = row.getToken(1);
        consumer.accept(Pair.of(new Offsets.Token((long) token.getValue()), key));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Factory asFactory() {
    final CassandraDBAccessor accessor = this.accessor;
    return repo -> new CassandraRandomReader(accessor);
  }

  @Override
  public synchronized void close() {
    accessor.close();
  }

  @Override
  public synchronized RandomOffset fetchOffset(Listing type, String key) {
    try {
      switch (type) {
        case ATTRIBUTE:
          return new Offsets.Raw(key);

        case ENTITY:
          Session session = accessor.ensureSession();
          ResultSet res =
              accessor.execute(accessor.getCqlFactory().getFetchTokenStatement(key, session));
          if (res.isExhausted()) {
            return new Offsets.Token(Long.MIN_VALUE);
          }
          return new Offsets.Token(res.one().getLong(0));

        default:
          throw new IllegalArgumentException("Unknown type of listing: " + type);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
