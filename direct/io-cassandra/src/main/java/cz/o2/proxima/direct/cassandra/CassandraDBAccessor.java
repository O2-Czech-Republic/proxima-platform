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
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.util.Classpath;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * {@code AttributeWriter} for Apache Cassandra. This class is completely synchronized for now, need
 * to do performance measurements to do better
 */
@Slf4j
public class CassandraDBAccessor extends AbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  static final String CQL_FACTORY_CFG = "cqlFactory";
  static final String CQL_STRING_CONVERTER = "converter";
  static final String CQL_PARALLEL_SCANS = "scanParallelism";

  @Getter(AccessLevel.PACKAGE)
  private final CqlFactory cqlFactory;

  /** Converter between string and native cassandra type used for wildcard types. */
  @Getter(AccessLevel.PACKAGE)
  private final StringConverter<Object> converter;
  /** Parallel scans. */
  @Getter(AccessLevel.PACKAGE)
  private final int batchParallelism;
  /** Our cassandra cluster. */
  @Nullable private transient Cluster cluster;
  /** Session we are connected to. */
  @Nullable private transient Session session;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public CassandraDBAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    super(entityDesc, uri);

    Object factoryName = cfg.get(CQL_FACTORY_CFG);
    String cqlFactoryName =
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
    try {
      cqlFactory = Classpath.findClass(cqlFactoryName, CqlFactory.class).newInstance();
      cqlFactory.setup(entityDesc, uri, converter);
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new IllegalArgumentException("Cannot instantiate class " + cqlFactoryName, ex);
    }
  }

  ResultSet execute(Statement statement) {
    if (log.isDebugEnabled()) {
      if (statement instanceof BoundStatement) {
        BoundStatement s = (BoundStatement) statement;
        log.debug("Executing BoundStatement {}", s.preparedStatement().getQueryString());
      } else {
        log.debug(
            "Executing {} {} with payload {}",
            statement.getClass().getSimpleName(),
            statement,
            statement.getOutgoingPayload());
      }
    }
    return session.execute(statement);
  }

  @VisibleForTesting
  Cluster getCluster(URI uri) {
    String authority = uri.getAuthority();
    if (Strings.isNullOrEmpty(authority)) {
      throw new IllegalArgumentException("Invalid authority in " + uri);
    }
    return Cluster.builder()
        // .withCodecRegistry(CodecRegistry.DEFAULT_INSTANCE.register(TypeCodec.))
        .addContactPointsWithPorts(
            Arrays.stream(authority.split(","))
                .map(
                    p -> {
                      String[] parts = p.split(":", 2);
                      if (parts.length != 2) {
                        throw new IllegalArgumentException("Invalid hostport " + p);
                      }
                      return InetSocketAddress.createUnresolved(
                          parts[0], Integer.parseInt(parts[1]));
                    })
                .collect(Collectors.toList()))
        .build();
  }

  synchronized Session ensureSession() {
    if (session == null || session.isClosed()) {
      if (cluster == null || cluster.isClosed()) {
        if (cluster != null) {
          cluster.close();
        }
        cluster = getCluster(getUri());
      }
      if (session != null) {
        session.close();
      }
      session = cluster.connect();
    }
    return session;
  }

  synchronized void close() {
    if (session != null) {
      session.close();
      session = null;
    }
    if (cluster != null) {
      cluster.close();
      cluster = null;
    }
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
    return new CassandraRandomReader(this);
  }

  @VisibleForTesting
  CassandraLogReader newBatchReader(Context context) {
    return new CassandraLogReader(this, context::getExecutorService);
  }

  @VisibleForTesting
  CassandraWriter newWriter() {
    return new CassandraWriter(this);
  }
}
