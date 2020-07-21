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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import org.junit.Test;

/** Test suite for {@code CassandraDBAccessor}. */
public class CassandraDBAccessorTest {

  static final class TestDBAccessor extends CassandraDBAccessor {

    @Setter ResultSet res = new EmptyResultSet();

    @Getter final List<Statement> executed = new ArrayList<>();

    public TestDBAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

      super(entityDesc, uri, cfg);
    }

    @Override
    ResultSet execute(Statement statement) {
      executed.add(statement);
      return res;
    }

    @Override
    Cluster getCluster(URI uri) {
      Cluster ret = mock(Cluster.class);
      when(ret.connect()).thenReturn(mock(Session.class));
      return ret;
    }
  }

  static final class TestCqlFactory implements CqlFactory {

    @Override
    public Optional<BoundStatement> getWriteStatement(StreamElement ingest, Session session) {

      return Optional.empty();
    }

    @Override
    public void setup(EntityDescriptor entity, URI uri, StringConverter converter) {
      // nop
    }

    @Override
    public BoundStatement getReadStatement(
        String key, String attribute, AttributeDescriptor desc, Session session) {

      return mock(BoundStatement.class);
    }

    @Override
    public BoundStatement getListStatement(
        String key, AttributeDescriptor wildcard, Offsets.Raw offset, int limit, Session session) {

      return mock(BoundStatement.class);
    }

    @Override
    public BoundStatement getListEntitiesStatement(
        Offsets.Token offset, int limit, Session session) {

      return mock(BoundStatement.class);
    }

    @Override
    public BoundStatement getFetchTokenStatement(String key, Session session) {
      return mock(BoundStatement.class);
    }

    @Override
    public BoundStatement scanPartition(
        List<AttributeDescriptor<?>> attributes, CassandraPartition partition, Session session) {

      return mock(BoundStatement.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> KvIterable<T> getListAllStatement(
        String key, Offsets.Raw offset, int limit, Session session) {

      return mock(KvIterable.class);
    }
  }

  static final class ThrowingTestCqlFactory implements CqlFactory {

    @Override
    public Optional<BoundStatement> getWriteStatement(StreamElement ingest, Session session) {
      throw new RuntimeException("Fail");
    }

    @Override
    public void setup(EntityDescriptor entity, URI uri, StringConverter converter) {
      // nop
    }

    @Override
    public BoundStatement getReadStatement(
        String key, String attribute, AttributeDescriptor desc, Session session) {

      throw new RuntimeException("Fail");
    }

    @Override
    public BoundStatement getListStatement(
        String key, AttributeDescriptor wildcard, Offsets.Raw offset, int limit, Session session) {

      throw new RuntimeException("Fail");
    }

    @Override
    public BoundStatement getListEntitiesStatement(
        Offsets.Token offset, int limit, Session session) {

      throw new RuntimeException("Fail");
    }

    @Override
    public BoundStatement getFetchTokenStatement(String key, Session session) {
      throw new RuntimeException("Fail");
    }

    @Override
    public BoundStatement scanPartition(
        List<AttributeDescriptor<?>> attributes, CassandraPartition partition, Session session) {

      throw new RuntimeException("Fail");
    }

    @Override
    public <T> KvIterable<T> getListAllStatement(
        String key, Offsets.Raw offset, int limit, Session session) {

      throw new RuntimeException("Fail");
    }
  }

  final Repository repo =
      ConfigRepository.Builder.ofTest(ConfigFactory.defaultApplication()).build();
  AttributeDescriptorBase<byte[]> attr;
  AttributeDescriptorBase<byte[]> attrWildcard;
  EntityDescriptor entity;

  public CassandraDBAccessorTest() throws URISyntaxException {
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setName("dummy")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    this.attrWildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dmmy")
            .setName("device.*")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("dummy")
            .addAttribute(attr)
            .addAttribute(attrWildcard)
            .build();
  }

  /** Test successful write. */
  @Test
  public void testWriteSuccess() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    CassandraDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class));
    CassandraWriter writer = accessor.newWriter();

    AtomicBoolean success = new AtomicBoolean(false);
    writer.write(
        StreamElement.upsert(
            entity, attr, "", "key", "attr", System.currentTimeMillis(), new byte[0]),
        (status, exc) -> success.set(status));
    assertTrue(success.get());
  }

  /** Test failed write. */
  @Test
  public void testWriteFailed() {
    CassandraDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(ThrowingTestCqlFactory.class));
    CassandraWriter writer = accessor.newWriter();

    AtomicBoolean success = new AtomicBoolean(true);
    writer.write(
        StreamElement.upsert(
            entity, attr, "", "key", "attr", System.currentTimeMillis(), new byte[0]),
        (status, exc) -> success.set(status));
    assertFalse(success.get());
  }

  /** Test successful delete. */
  @Test
  public void testDeleteSuccess() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    CassandraDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class));
    CassandraWriter writer = accessor.newWriter();

    AtomicBoolean success = new AtomicBoolean(false);
    writer.write(
        StreamElement.upsert(entity, attr, "", "key", "attr", System.currentTimeMillis(), null),
        (status, exc) -> success.set(status));
    assertTrue(success.get());
  }

  /** Test failed delete. */
  @Test
  public void testDeleteFailed() {
    CassandraDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(ThrowingTestCqlFactory.class));
    CassandraWriter writer = accessor.newWriter();

    AtomicBoolean success = new AtomicBoolean(true);
    writer.write(
        StreamElement.upsert(entity, attr, "", "key", "attr", System.currentTimeMillis(), null),
        (status, exc) -> success.set(status));
    assertFalse(success.get());
  }

  /** Test get of attribute. */
  @Test
  public void testGetSuccess() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    byte[] payload = new byte[] {1, 2};
    Row row = mock(Row.class);
    when(row.getBytes(0)).thenReturn(ByteBuffer.wrap(payload));
    List<Row> rows = Collections.singletonList(row);

    ResultSet res = mock(ResultSet.class);
    when(res.iterator()).thenReturn(rows.iterator());

    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class));
    RandomAccessReader db = accessor.newRandomReader();

    accessor.setRes(res);

    Optional<KeyValue<byte[]>> value = db.get("key", attr);
    assertTrue(value.isPresent());
    assertEquals("dummy", value.get().getAttribute());
    assertEquals("key", value.get().getKey());
    assertArrayEquals(payload, (byte[]) value.get().getValue());
  }

  /** Test failed get does throw exceptions. */
  @Test(expected = RuntimeException.class)
  public void testGetFailed() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    byte[] payload = new byte[] {1, 2};
    Row row = mock(Row.class);
    when(row.getBytes(0)).thenReturn(ByteBuffer.wrap(payload));
    List<Row> rows = Collections.singletonList(row);

    ResultSet res = mock(ResultSet.class);
    when(res.iterator()).thenReturn(rows.iterator());

    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(ThrowingTestCqlFactory.class));
    CassandraRandomReader db = accessor.newRandomReader();

    accessor.setRes(res);

    db.get("key", attr);
  }

  /** Test list with success. */
  @Test
  public void testListSuccess() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    byte[] payload = new byte[] {1, 2};
    Row row = mock(Row.class);
    when(row.getObject(0)).thenReturn("1");
    when(row.getBytes(1)).thenReturn(ByteBuffer.wrap(payload));
    List<Row> rows = Collections.singletonList(row);

    ResultSet res = mock(ResultSet.class);
    when(res.iterator()).thenReturn(rows.iterator());

    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class));
    CassandraRandomReader db = accessor.newRandomReader();

    accessor.setRes(res);
    AtomicInteger count = new AtomicInteger();

    db.scanWildcard(
        "key",
        attrWildcard,
        data -> {
          count.incrementAndGet();
          assertEquals("device.1", data.getAttribute());
          assertEquals("key", data.getKey());
          assertArrayEquals(payload, (byte[]) data.getValue());
        });

    assertEquals(1, count.get());
  }

  /** Test list with success. */
  @Test
  public void testListSuccessWithConverter() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    byte[] payload = new byte[] {1, 2};
    Row row = mock(Row.class);
    when(row.getObject(0)).thenReturn(new Date(1234567890000L));
    when(row.getBytes(1)).thenReturn(ByteBuffer.wrap(payload));
    List<Row> rows = Collections.singletonList(row);

    ResultSet res = mock(ResultSet.class);
    when(res.iterator()).thenReturn(rows.iterator());

    TestDBAccessor accessor =
        new TestDBAccessor(
            entity,
            URI.create("cassandra://localhost/"),
            getCfg(TestCqlFactory.class, DateToLongConverter.class));
    CassandraRandomReader db = accessor.newRandomReader();

    accessor.setRes(res);
    AtomicInteger count = new AtomicInteger();

    db.scanWildcard(
        "key",
        attrWildcard,
        data -> {
          count.incrementAndGet();
          assertEquals("device.1234567890000", data.getAttribute());
          assertEquals("key", data.getKey());
          assertArrayEquals(payload, (byte[]) data.getValue());
        });

    assertEquals(1, count.get());
  }

  /** Test list with error. */
  @Test(expected = RuntimeException.class)
  public void testListFailed() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    byte[] payload = new byte[] {1, 2};
    Row row = mock(Row.class);
    when(row.getObject(0)).thenReturn("1");
    when(row.getBytes(1)).thenReturn(ByteBuffer.wrap(payload));
    List<Row> rows = Collections.singletonList(row);

    ResultSet res = mock(ResultSet.class);
    when(res.iterator()).thenReturn(rows.iterator());

    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(ThrowingTestCqlFactory.class));
    CassandraRandomReader db = accessor.newRandomReader();

    accessor.setRes(res);
    AtomicInteger count = new AtomicInteger();

    db.scanWildcard("key", attrWildcard, data -> {});

    assertEquals(1, count.get());
  }

  @Test
  public void testGetPartitions13() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    CassandraDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class, 13));
    CassandraLogObservable observable =
        new CassandraLogObservable(accessor, () -> Executors.newCachedThreadPool());

    List<Partition> partitions = observable.getPartitions();
    assertEquals(13, partitions.size());
    double start = Long.MIN_VALUE;
    double end = 0;
    double step = Long.MAX_VALUE / 13.0 * 2 + 1.0 / 13;
    for (int i = 0; i < 13; i++) {
      CassandraPartition part = (CassandraPartition) partitions.get(i);
      end = start + step;
      assertEquals((long) start, part.getTokenStart());
      assertEquals((long) end, part.getTokenEnd());
      if (i < 12) {
        assertFalse(part.isEndInclusive());
      } else {
        assertTrue(part.isEndInclusive());
      }
      start = end;
    }
    assertEquals(Long.MAX_VALUE, (long) end);
  }

  @Test
  public void testGetPartitions2() {
    entity = EntityDescriptor.newBuilder().setName("dummy").build();

    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class, 2));
    CassandraLogObservable observable =
        new CassandraLogObservable(accessor, () -> Executors.newCachedThreadPool());

    List<Partition> partitions = observable.getPartitions();
    assertEquals(2, partitions.size());
    for (int i = 0; i < 2; i++) {
      CassandraPartition part = (CassandraPartition) partitions.get(i);
      if (i == 0) {
        assertEquals(Long.MIN_VALUE, part.getTokenStart());
        assertEquals(0, part.getTokenEnd());
        assertFalse(part.isEndInclusive());
      } else {
        assertEquals(0, part.getTokenStart());
        assertEquals(Long.MAX_VALUE, part.getTokenEnd());
        assertTrue(part.isEndInclusive());
      }
    }
  }

  @Test(timeout = 10000)
  public void testBatchObserve() throws URISyntaxException, InterruptedException {

    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class, 2));

    CassandraLogObservable observable =
        new CassandraLogObservable(accessor, () -> Executors.newCachedThreadPool());

    CountDownLatch latch = new CountDownLatch(1);
    observable.observe(
        observable.getPartitions(),
        Arrays.asList(attr),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });

    latch.await();

    List<Statement> executed = accessor.getExecuted();
    assertEquals(2, executed.size());
  }

  @Test
  public void testReaderAsFactorySerializable() throws IOException, ClassNotFoundException {
    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class, 2));
    CassandraRandomReader reader = accessor.newRandomReader();
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    RandomAccessReader.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(reader.getUri(), ((CassandraRandomReader) factory.apply(repo)).getUri());
  }

  @Test
  public void testWriterAsFactorySerializable() throws IOException, ClassNotFoundException {
    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class, 2));
    CassandraWriter writer = accessor.newWriter();
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    AttributeWriterBase.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(writer.getUri(), ((CassandraWriter) factory.apply(repo)).getUri());
  }

  @Test
  public void testObservableAsFactorySerializable() throws IOException, ClassNotFoundException {
    TestDBAccessor accessor =
        new TestDBAccessor(
            entity, URI.create("cassandra://localhost/"), getCfg(TestCqlFactory.class, 2));
    CassandraLogObservable reader =
        accessor.newLogObservable(repo.getOrCreateOperator(DirectDataOperator.class).getContext());
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    BatchLogObservable.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(reader.getUri(), ((CassandraLogObservable) factory.apply(repo)).getUri());
  }

  private Map<String, Object> getCfg(Class<?> cls, Class<? extends StringConverter> converter) {

    Map<String, Object> m = new HashMap<>();
    m.put(CassandraDBAccessor.CQL_FACTORY_CFG, cls.getName());
    m.put(CassandraDBAccessor.CQL_STRING_CONVERTER, converter.getName());
    return m;
  }

  private Map<String, Object> getCfg(Class<?> cls) {
    Map<String, Object> m = new HashMap<>();
    m.put(CassandraDBAccessor.CQL_FACTORY_CFG, cls.getName());
    return m;
  }

  private Map<String, Object> getCfg(Class<?> cls, int scans) {
    Map<String, Object> m = new HashMap<>();
    m.put(CassandraDBAccessor.CQL_FACTORY_CFG, cls.getName());
    m.put(CassandraDBAccessor.CQL_PARALLEL_SCANS, scans);
    return m;
  }
}
