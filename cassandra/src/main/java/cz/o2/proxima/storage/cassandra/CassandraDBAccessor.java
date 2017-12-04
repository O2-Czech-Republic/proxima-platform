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
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractOnlineAttributeWriter;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.Pair;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.batch.BatchLogObserver;
import cz.seznam.euphoria.shaded.guava.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Strings;

/**
 * {@code AttributeWriter} for Apache Cassandra.
 * This class is completely synchronized for now, need to do performance
 * measurements to do better
 */
public class CassandraDBAccessor extends AbstractOnlineAttributeWriter
    implements RandomAccessReader, BatchLogObservable, DataAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraDBAccessor.class);

  static final String CQL_FACTORY_CFG = "cqlFactory";
  static final String CQL_STRING_CONVERTER = "converter";
  static final String CQL_PARALLEL_SCANS = "scanParallelism";

  private final CQLFactory cqlFactory;

  /** Config map. */
  private final Map<String, Object> cfg;
  /** Our connection URI. */
  private final URI uri;
  /** Converter between string and native cassandra type used for wildcard types. */
  private final StringConverter<Object> converter;
  /** Parallel scans. */
  private final int batchParallelism;
  /** Our cassandra cluster. */
  private Cluster cluster;
  /** Session we are connected to. */
  private Session session;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public CassandraDBAccessor(
      EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    super(entityDesc, uri);

    this.cfg = cfg;
    this.uri = uri;
    Object factoryName = cfg.get(CQL_FACTORY_CFG);
    String cqlFactoryName = factoryName == null
        ? DefaultCQLFactory.class.getName()
        : factoryName.toString();

    Object tmp = cfg.get(CQL_PARALLEL_SCANS);
    if (tmp != null) {
      batchParallelism = Integer.valueOf(tmp.toString());
    } else {
      batchParallelism = Runtime.getRuntime().availableProcessors();
    }

    if (batchParallelism < 2) {
      throw new IllegalArgumentException(
          "Batch parallelism must be at least 2, got " + batchParallelism);
    }

    tmp = cfg.get(CQL_STRING_CONVERTER);
    StringConverter c = StringConverter.DEFAULT;
    if (tmp != null) {
      try {
        c = (StringConverter) Class.forName(tmp.toString()).newInstance();
      } catch (Exception ex) {
        LOG.warn("Failed to instantiate type converter {}", tmp, ex);
      }
    }
    this.converter = c;


    try {
      cqlFactory = Classpath.findClass(cqlFactoryName, CQLFactory.class).newInstance();
      cqlFactory.setup(uri, converter);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
      throw new IllegalArgumentException("Cannot instantiate class " + cqlFactoryName,
          ex);
    }


  }


  @Override
  public synchronized void write(
      StreamElement data,
      CommitCallback statusCallback) {

    try {
      ensureSession();
      Optional<BoundStatement> cql = cqlFactory.getWriteStatement(data, session);
      if (cql.isPresent()) {
        execute(cql.get());
      }
      statusCallback.commit(true, null);
    } catch (Exception ex) {
      LOG.error("Failed to ingest record {} into cassandra", data, ex);
      // reset the session and cluster connection
      if (session != null) {
        session.close();
        session = null;
      }
      if (cluster != null) {
        cluster.close();
        cluster = null;
      }
      statusCallback.commit(false, ex);
    }
  }

  @VisibleForTesting
  ResultSet execute(Statement statement) {
    if (LOG.isDebugEnabled()) {
      if (statement instanceof BoundStatement) {
        BoundStatement s = (BoundStatement) statement;
        LOG.debug(
            "Executing BoundStatement {} prepared from PreparedStatement {} with payload {}",
            s, s.preparedStatement(), s.getOutgoingPayload());
      } else {
        LOG.debug(
            "Executing {} {} with payload {}",
            statement.getClass().getSimpleName(),
            statement, statement.getOutgoingPayload());
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
        //.withCodecRegistry(CodecRegistry.DEFAULT_INSTANCE.register(TypeCodec.))
        .addContactPointsWithPorts(Arrays.stream(authority.split(","))
            .map(p -> {
              String[] parts = p.split(":", 2);
              if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid hostport " + p);
              }
              return InetSocketAddress.createUnresolved(
                  parts[0], Integer.valueOf(parts[1]));
            })
            .collect(Collectors.toList()))
        .build();
  }

  private void ensureSession() {
    if (session == null) {
      this.cluster = getCluster(uri);
      this.session = this.cluster.connect();
    }
  }

  @Override
  public synchronized Optional<KeyValue<?>> get(
      String key,
      String attribute,
      AttributeDescriptor<?> desc) {

    ensureSession();
    BoundStatement statement = cqlFactory.getReadStatement(key, attribute, desc, session);
    ResultSet result;
    try {
      result = execute(statement);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    // the row has to have format (value)
    for (Row row : result) {
      ByteBuffer val = row.getBytes(0);
      if (val != null) {
        byte[] rowValue = val.array();

        try {
          return Optional.of(KeyValue.of(
              getEntityDescriptor(),
              desc,
              key,
              attribute,
              new Offsets.Raw(attribute),
              rowValue));
        } catch (Exception ex) {
          LOG.warn("Failed to read data from {}.{}", key, attribute, ex);
        }
      }
    }

    return Optional.empty();
  }


  @Override
  @SuppressWarnings("unchecked")
  public synchronized void scanWildcard(
      String key,
      AttributeDescriptor<?> wildcard,
      @Nullable Offset offset,
      int limit,
      Consumer<KeyValue<?>> consumer) {

    try {
      ensureSession();
      BoundStatement statement = cqlFactory.getListStatement(
          key, wildcard,
          (Offsets.Raw) offset, limit, session);

      ResultSet result = execute(statement);
      // the row has to have format (attribute, value)
      for (Row row : result) {
        Object attribute = row.getObject(0);
        ByteBuffer val = row.getBytes(1);
        if (val != null) {
          byte[] rowValue = val.array();
          // by convention
          String name = wildcard.toAttributePrefix() + converter.toString(attribute);


          Optional parsed = wildcard.getValueSerializer().deserialize(rowValue);

          if (parsed.isPresent()) {
            consumer.accept(KeyValue.of(
                getEntityDescriptor(),
                (AttributeDescriptor) wildcard,
                key,
                name,
                new Offsets.Raw(name),
                parsed.get(),
                rowValue));
          } else {
            LOG.error("Failed to parse value for key {} attribute {}.{}",
                key, wildcard, attribute);
          }
        }
      }
    } catch (Exception ex) {
      LOG.error("Failed to scan wildcard attribute {}", wildcard, ex);
      throw new RuntimeException(ex);
    }

  }

  @Override
  public synchronized void listEntities(
      Offset offset,
      int limit,
      Consumer<Pair<Offset, String>> consumer) {

    ensureSession();
    BoundStatement statement = cqlFactory.getListEntitiesStatement(
        (Offsets.Token) offset, limit, session);

    try {
      ResultSet result = execute(statement);
      for (Row row : result) {
        String key = row.getString(0);
        Token token = row.getToken(1);
        consumer.accept(Pair.of(
            new Offsets.Token((long) token.getValue()),
            key));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public synchronized void close() {
    if (cluster != null) {
      cluster.close();
      cluster = null;
    }
  }

  @Override
  public synchronized Offset fetchOffset(Listing type, String key) {
    try {
      switch (type) {
        case ATTRIBUTE:
          return new Offsets.Raw(key);

        case ENTITY:
          ensureSession();
          ResultSet res = execute(cqlFactory.getFetchTokenStatement(key, session));
          if (res.isExhausted()) {
            return new Offsets.Token(Long.MIN_VALUE);
          }
          return new Offsets.Token(res.one().getLong(0));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    throw new IllegalArgumentException("Unknown type of listing: " + type);
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> ret = new ArrayList<>();
    double step = (((double) Long.MAX_VALUE) * 2 + 1) / batchParallelism;
    double tokenStart = Long.MIN_VALUE;
    double tokenEnd = tokenStart + step;
    for (int i = 0; i < batchParallelism; i++) {
      // FIXME: we ignore the start stamp for now
      ret.add(new CassandraPartition(i, startStamp, endStamp,
          (long) tokenStart, (long) tokenEnd, i == batchParallelism - 1));
      tokenStart = tokenEnd;
      tokenEnd += step;
      if (i == batchParallelism - 2) {
        tokenEnd = Long.MAX_VALUE;
      }
    }
    return ret;
  }

  @Override
  public void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    Thread thread = new Thread(() -> {
      boolean cont = true;
      Iterator<Partition> it = partitions.iterator();
      try {
        while (cont && it.hasNext()) {
          CassandraPartition p = (CassandraPartition) it.next();
          ResultSet result;
          ensureSession();
          result = execute(this.cqlFactory.scanPartition(attributes, p, session));
          AtomicLong position = new AtomicLong();
          Iterator<Row> rowIter = result.iterator();
          while (rowIter.hasNext() && cont) {
            Row row = rowIter.next();
            String key = row.getString(0);
            int field = 1;
            for (AttributeDescriptor<?> attribute : attributes) {
              String attributeName = attribute.getName();
              if (attribute.isWildcard()) {
                String suffix = row.getString(field++);
                attributeName = attribute.toAttributePrefix() + suffix;
              }
              ByteBuffer bytes = row.getBytes(field++);
              if (bytes != null) {
                byte[] array = bytes.slice().array();
                if (!observer.onNext(StreamElement.update(
                    getEntityDescriptor(), attribute,
                    "cql-" + getEntityDescriptor().getName() + "-part"
                        + p.getId() + position.incrementAndGet(),
                    key, attributeName,
                    System.currentTimeMillis(), array), p)) {

                  cont = false;
                  break;
                }
              }
            }
          };
        }
        observer.onCompleted();
      } catch (Throwable err) {
        observer.onError(err);
      }
    });
    thread.setName("cassandra-batch-observable-"
        + "-" + getEntityDescriptor().getName()
        + ":" + attributes);
    thread.start();
  }

  @Override
  public Optional<AttributeWriterBase> getWriter() {
    return Optional.of(this);
  }

  @Override
  public Optional<RandomAccessReader> getRandomAccessReader() {
    return Optional.of(this);
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable() {
    return Optional.of(this);
  }



}
