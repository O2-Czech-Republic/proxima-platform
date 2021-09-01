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
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.util.Classpath;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** {@code AttributeWriter} for Apache Cassandra. */
@Slf4j
public class CassandraDBAccessor extends AbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  private static final Map<String, Cluster> CLUSTER_MAP =
      Collections.synchronizedMap(new HashMap<>());
  private static final Map<Cluster, AtomicInteger> CLUSTER_REFERENCES = new ConcurrentHashMap<>();
  private static final Map<Cluster, Session> CLUSTER_SESSIONS = new ConcurrentHashMap<>();

  static final String CQL_FACTORY_CFG = "cqlFactory";
  static final String CQL_STRING_CONVERTER = "converter";
  static final String CQL_PARALLEL_SCANS = "scanParallelism";

  /** Converter between string and native cassandra type used for wildcard types. */
  @Getter(AccessLevel.PACKAGE)
  private final StringConverter<Object> converter;

  /** Parallel scans. */
  @Getter(AccessLevel.PACKAGE)
  private final int batchParallelism;

  private final String cqlFactoryName;

  private transient ThreadLocal<CqlFactory> cqlFactory;

  /** Our cassandra cluster. */
  @Nullable private transient Cluster cluster;
  /** Session we are connected to. */
  @Nullable private transient Session session;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public CassandraDBAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    super(entityDesc, uri);

    Object factoryName = cfg.get(CQL_FACTORY_CFG);
    this.cqlFactoryName =
        factoryName == null ? DefaultCqlFactory.class.getName() : factoryName.toString();

    Object tmp = cfg.get(CQL_PARALLEL_SCANS);
    if (tmp != null) {
      batchParallelism = Integer.parseInt(tmp.toString());
    } else {
      batchParallelism = Runtime.getRuntime().availableProcessors();
    }

    if (batchParallelism < 2) {
      throw new IllegalArgumentException(
          "Batch parallelism must be at least 2, got " + batchParallelism);
    }

    tmp = cfg.get(CQL_STRING_CONVERTER);
    StringConverter<String> c = StringConverter.getDefault();
    if (tmp != null) {
      try {
        c = Classpath.newInstance(tmp.toString(), StringConverter.class);
      } catch (Exception ex) {
        log.warn("Failed to instantiate type converter {}", tmp, ex);
      }
    }
    this.converter = (StringConverter) c;
    initializeCqlFactory();
  }

  private void initializeCqlFactory() {
    this.cqlFactory =
        ThreadLocal.withInitial(
            () -> {
              try {
                final CqlFactory cqlFactory =
                    Classpath.findClass(cqlFactoryName, CqlFactory.class).newInstance();
                cqlFactory.setup(getEntityDescriptor(), getUri(), converter);
                return cqlFactory;
              } catch (InstantiationException | IllegalAccessException ex) {
                throw new IllegalArgumentException(
                    "Cannot instantiate class " + cqlFactoryName, ex);
              }
            });
  }

  ResultSet execute(Statement statement) {
    if (log.isDebugEnabled()) {
      if (statement instanceof BoundStatement) {
        final BoundStatement s = (BoundStatement) statement;
        log.debug("Executing BoundStatement {}", s.preparedStatement().getQueryString());
      } else {
        log.debug(
            "Executing {} {} with payload {}",
            statement.getClass().getSimpleName(),
            statement,
            statement.getOutgoingPayload());
      }
    }
    return ensureSession().execute(statement);
  }

  private Cluster getCluster(URI uri) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(uri.getAuthority()), "Invalid authority in %s", uri);
    return getCluster(uri.getAuthority());
  }

  private Cluster getCluster(String authority) {
    if (this.cluster != null) {
      return this.cluster;
    }
    synchronized (CLUSTER_MAP) {
      Cluster cluster = CLUSTER_MAP.get(authority);
      if (cluster == null) {
        cluster = createCluster(authority);
        CLUSTER_MAP.put(authority, cluster);
        this.cluster = cluster;
      }
      return Objects.requireNonNull(cluster);
    }
  }

  void incrementClusterReference() {
    Cluster cluster = getCluster(getUri());
    CLUSTER_REFERENCES.computeIfAbsent(cluster, tmp -> new AtomicInteger(0)).incrementAndGet();
  }

  void decrementClusterReference() {
    if (cluster != null) {
      AtomicInteger references = CLUSTER_REFERENCES.get(cluster);
      if (references != null && references.decrementAndGet() == 0) {
        synchronized (CLUSTER_MAP) {
          Optional.ofNullable(CLUSTER_SESSIONS.remove(cluster)).ifPresent(Session::close);
          Optional.ofNullable(CLUSTER_MAP.remove(getUri().getAuthority()))
              .ifPresent(Cluster::close);
          CLUSTER_REFERENCES.remove(cluster);
          cluster = null;
        }
      }
    }
  }

  @VisibleForTesting
  Cluster createCluster(String authority) {
    return Cluster.builder()
        .addContactPointsWithPorts(
            Arrays.stream(authority.split(","))
                .map(CassandraDBAccessor::getAddress)
                .collect(Collectors.toList()))
        .build();
  }

  @VisibleForTesting
  static InetSocketAddress getAddress(String p) {
    String[] parts = p.split(":", 2);
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid hostport " + p);
    }
    return InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(parts[1]));
  }

  Session ensureSession() {
    if (cluster == null) {
      cluster = getCluster(getUri());
    }
    Preconditions.checkState(cluster != null);
    session = CLUSTER_SESSIONS.computeIfAbsent(cluster, Cluster::connect);
    if (session.isClosed()) {
      synchronized (this) {
        session = CLUSTER_SESSIONS.get(cluster);
        if (session.isClosed()) {
          session = cluster.connect();
          CLUSTER_SESSIONS.put(cluster, session);
        }
      }
    }
    Preconditions.checkState(!session.isClosed());
    return session;
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
    incrementClusterReference();
    return new CassandraRandomReader(this) {
      @Override
      public void close() {
        cqlFactory.remove();
        decrementClusterReference();
      }
    };
  }

  @VisibleForTesting
  CassandraLogReader newBatchReader(Context context) {
    incrementClusterReference();
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
    incrementClusterReference();
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
    CLUSTER_REFERENCES.clear();
    CLUSTER_MAP.clear();
    CLUSTER_SESSIONS.clear();
  }

  CqlFactory getCqlFactory() {
    return cqlFactory.get();
  }

  Object readResolve() throws ObjectStreamException {
    initializeCqlFactory();
    return this;
  }
}
