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
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage.SerializableAbstractStorage;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.io.serialization.shaded.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.io.serialization.shaded.com.google.common.base.Preconditions;
import cz.o2.proxima.io.serialization.shaded.com.google.common.base.Strings;
import java.io.ObjectStreamException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** {@code AttributeWriter} for Apache Cassandra. */
@Slf4j
public class CassandraDBAccessor extends SerializableAbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private static final Map<String, CqlSession> SESSION_MAP =
      Collections.synchronizedMap(new HashMap<>());

  static final String CQL_FACTORY_CFG = "cqlFactory";
  static final String CQL_STRING_CONVERTER = "converter";
  static final String CQL_PARALLEL_SCANS = "scanParallelism";
  static final String CONSISTENCY_LEVEL_CFG = "consistency-level";
  static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
  static final String USERNAME_CFG = "username";
  static final String PASSWORD_CFG = "password";
  static final String DATACENTER_CFG = "datacenter";

  /** Converter between string and native cassandra type used for wildcard types. */
  private final StringConverter<?> converter;

  /** Parallel scans. */
  @Getter(AccessLevel.PACKAGE)
  private final int batchParallelism;

  private final String cqlFactoryName;

  private transient ThreadLocal<CqlFactory> cqlFactory;

  /** Quorum for both reads and writes. */
  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private final ConsistencyLevel consistencyLevel;

  @Nullable
  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private final String username;

  @Nullable
  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private final String password;

  @Getter(AccessLevel.PACKAGE)
  private final String datacenter;

  public CassandraDBAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.cqlFactoryName = getCqlFactoryName(cfg);
    this.batchParallelism = getBatchParallelism(cfg);
    this.converter = getStringConverter(cfg);
    this.consistencyLevel = getConsistencyLevel(cfg);
    this.username = getOpt(cfg, USERNAME_CFG, Object::toString, null);
    this.password = getOpt(cfg, PASSWORD_CFG, Object::toString, "");
    this.datacenter = getOpt(cfg, DATACENTER_CFG, Object::toString, "datacenter1");
    initializeCqlFactory();
  }

  private String getCqlFactoryName(Map<String, Object> cfg) {
    Object tmp = cfg.get(CQL_FACTORY_CFG);
    return tmp == null ? DefaultCqlFactory.class.getName() : tmp.toString();
  }

  private int getBatchParallelism(Map<String, Object> cfg) {
    final Object tmp = cfg.get(CQL_PARALLEL_SCANS);
    final int ret;
    if (tmp != null) {
      ret = Integer.parseInt(tmp.toString());
    } else {
      ret = Math.max(2, Runtime.getRuntime().availableProcessors());
    }
    Preconditions.checkArgument(ret >= 2, "Batch parallelism must be at least 2, got %s", ret);
    return ret;
  }

  private StringConverter<?> getStringConverter(Map<String, Object> cfg) {
    Object tmp;
    tmp = cfg.get(CQL_STRING_CONVERTER);
    StringConverter<?> c = StringConverter.getDefault();
    if (tmp != null) {
      try {
        c = Classpath.newInstance(tmp.toString(), StringConverter.class);
      } catch (Exception ex) {
        log.warn("Failed to instantiate type converter {}", tmp, ex);
      }
    }
    return c;
  }

  private ConsistencyLevel getConsistencyLevel(Map<String, Object> cfg) {
    return getOpt(
        cfg, CONSISTENCY_LEVEL_CFG, DefaultConsistencyLevel::valueOf, DEFAULT_CONSISTENCY_LEVEL);
  }

  private static <T> T getOpt(
      Map<String, Object> cfg, String name, UnaryFunction<String, T> map, T defval) {
    return Optional.ofNullable(cfg.get(name)).map(Object::toString).map(map::apply).orElse(defval);
  }

  private void initializeCqlFactory() {
    this.cqlFactory =
        ThreadLocal.withInitial(
            () -> {
              CqlFactory factory = Classpath.newInstance(cqlFactoryName, CqlFactory.class);
              factory.setup(getEntityDescriptor(), getUri(), converter);
              return factory;
            });
  }

  @Nullable
  ResultSet executeOptionally(UnaryFunction<CqlSession, Optional<Statement<?>>> statementFn) {
    int i = 0;
    while (true) {
      try {
        final CqlSession session = ensureSession();
        final Optional<Statement<?>> maybeStatement =
            statementFn.apply(session).map(s -> s.setConsistencyLevel(consistencyLevel));
        if (maybeStatement.isEmpty()) {
          return null;
        }
        Statement<?> statement = maybeStatement.get();
        if (log.isDebugEnabled()) {
          if (statement instanceof BoundStatement) {
            final BoundStatement s = (BoundStatement) statement;
            log.debug("Executing BoundStatement {}", s.getPreparedStatement().getQuery());
          } else {
            log.debug(
                "Executing {} {} with payload {}",
                statement.getClass().getSimpleName(),
                statement,
                statement.getCustomPayload());
          }
        }
        return session.execute(statement);
      } catch (AllNodesFailedException ex) {
        int tryN = i;
        closeSession();
        if (tryN < 2) {
          log.warn(
              "Got {}, retry {}, closing session and retrying.",
              ex.getClass().getSimpleName(),
              tryN + 1,
              ex);
          ExceptionUtils.ignoringInterrupted(
              () -> TimeUnit.MILLISECONDS.sleep((long) (100 * Math.pow(2, tryN))));
        } else {
          log.warn("Got {}. Closing session and rethrowing.", ex.getClass().getSimpleName());
          throw ex;
        }
      }
      i++;
    }
  }

  ResultSet execute(UnaryFunction<CqlSession, Statement<?>> statementFn) {
    return Objects.requireNonNull(
        executeOptionally(session -> Optional.of(statementFn.apply(session))));
  }

  private CqlSession getSession(URI uri) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(uri.getAuthority()), "Invalid authority in %s", uri);
    return getSession(uri.getAuthority(), this.username);
  }

  private CqlSession getSession(String authority, @Nullable String username) {
    final String clusterCachedKey = computeClusterKey(authority, username);

    int retry = 0;
    while (true) {
      try {
        synchronized (SESSION_MAP) {
          CqlSession session =
              SESSION_MAP.computeIfAbsent(clusterCachedKey, k -> createSession(authority));
          if (session.isClosed()) {
            SESSION_MAP.remove(clusterCachedKey);
            continue;
          }
          return Objects.requireNonNull(session);
        }
      } catch (Exception ex) {
        if (retry++ < 3) {
          int attempt = retry;
          ExceptionUtils.ignoringInterrupted(
              () -> TimeUnit.MILLISECONDS.sleep((int) (Math.pow(2, attempt) * 100)));
        } else {
          throw ex;
        }
      }
    }
  }

  private static String computeClusterKey(String authority, @Nullable String username) {
    return (username != null ? username + "@" : "") + authority;
  }

  @VisibleForTesting
  CqlSession createSession(String authority) {
    log.info("Creating session for authority {} in accessor {}", authority, this);
    return configureSessionBuilder(CqlSession.builder(), authority).build();
  }

  @VisibleForTesting
  CqlSessionBuilder configureSessionBuilder(CqlSessionBuilder builder, String authority) {
    builder =
        builder
            .addContactPoints(
                Arrays.stream(authority.split(","))
                    .map(CassandraDBAccessor::getAddress)
                    .collect(Collectors.toList()))
            .withLocalDatacenter(this.datacenter)
            .withClassLoader(builder.getClass().getClassLoader());

    if (username != null && password != null) {
      return builder.withAuthCredentials(username, password);
    }
    return builder;
  }

  @VisibleForTesting
  static InetSocketAddress getAddress(String p) {
    String[] parts = p.split(":", 2);
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid hostport " + p);
    }
    return InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(parts[1]));
  }

  CqlSession ensureSession() {
    return getSession(getUri());
  }

  void closeSession() {
    final String clusterCachedKey = computeClusterKey(getUri().getAuthority(), username);
    Optional.ofNullable(SESSION_MAP.remove(clusterCachedKey)).ifPresent(CqlSession::close);
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(newWriter());
  }

  @Override
  public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
    return Optional.of(newRandomReader());
  }

  @Override
  public Optional<BatchLogReader> getBatchLogReader(Context context) {
    return Optional.of(newBatchReader(context));
  }

  @VisibleForTesting
  CassandraRandomReader newRandomReader() {
    return new CassandraRandomReader(this) {
      @Override
      public void close() {
        super.close();
        cqlFactory.remove();
      }
    };
  }

  @VisibleForTesting
  CassandraLogReader newBatchReader(Context context) {
    return new CassandraLogReader(this, context::getExecutorService) {

      @Override
      public ObserveHandle observe(
          List<Partition> partitions,
          List<AttributeDescriptor<?>> attributes,
          BatchLogObserver observer) {
        final ObserveHandle handle = super.observe(partitions, attributes, observer);
        return () -> {
          handle.close();
          cqlFactory.remove();
        };
      }
    };
  }

  @VisibleForTesting
  CassandraWriter newWriter() {
    return new CassandraWriter(this) {

      @Override
      public void close() {
        super.close();
        cqlFactory.remove();
      }
    };
  }

  @VisibleForTesting
  static void clear() {
    SESSION_MAP.clear();
  }

  @SuppressWarnings("unchecked")
  String asString(Object value) {
    return ((StringConverter<Object>) converter).asString(value);
  }

  CqlFactory getCqlFactory() {
    return cqlFactory.get();
  }

  Object readResolve() throws ObjectStreamException {
    initializeCqlFactory();
    return this;
  }
}
