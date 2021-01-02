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

import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

/** Test for default CQL factory. */
public class DefaultCqlFactoryTest {

  final Config cfg = ConfigFactory.defaultApplication();
  final Repository repo = ConfigRepository.Builder.ofTest(cfg).build();
  final AttributeDescriptorBase<?> attr;
  final AttributeDescriptorBase<?> attrWildcard;
  final EntityDescriptor entity;
  final PreparedStatement statement = mock(PreparedStatement.class);
  final Session session = mock(Session.class);

  CqlFactory factory;
  List<String> preparedStatement;

  public DefaultCqlFactoryTest() throws URISyntaxException {
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setName("myAttribute")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    this.attrWildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
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

  @Before
  public void setup() throws URISyntaxException {
    preparedStatement = new ArrayList<>();
    factory =
        new DefaultCqlFactory() {

          // store the generated statements for inspection

          @Override
          protected String createInsertStatement(StreamElement what) {
            preparedStatement.add(super.createInsertStatement(what));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createDeleteStatement(StreamElement what) {
            preparedStatement.add(super.createDeleteStatement(what));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createDeleteWildcardStatement(StreamElement what) {
            preparedStatement.add(super.createDeleteWildcardStatement(what));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createListStatement(AttributeDescriptor desc) {
            preparedStatement.add(super.createListStatement(desc));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createGetStatement(String attribute, AttributeDescriptor desc) {
            preparedStatement.add(super.createGetStatement(attribute, desc));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createFetchTokenStatement() {
            preparedStatement.add(super.createFetchTokenStatement());
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createListEntitiesStatement() {
            preparedStatement.add(super.createListEntitiesStatement());
            return preparedStatement.get(preparedStatement.size() - 1);
          }
        };
    factory.setup(
        entity,
        new URI("cassandra://wherever/my_table?data=my_col&primary=hgw"),
        StringConverter.getDefault());
  }

  @Test(expected = IllegalStateException.class)
  public void testSetupWithNoQuery() throws URISyntaxException {
    factory.setup(entity, new URI("cassandra://wherever/my_table"), StringConverter.getDefault());
  }

  @Test
  public void testSetupWithJustPrimary() throws URISyntaxException {
    factory.setup(
        entity, new URI("cassandra://wherever/my_table?primary=hgw"), StringConverter.getDefault());
    assertNotNull(factory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetupWithNoPath() throws URISyntaxException {
    factory.setup(entity, new URI("cassandra://wherever/"), StringConverter.getDefault());
  }

  @Test
  public void testIngest() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(entity, attr, "", "key", "myAttribute", now, "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", ByteBuffer.wrap("value".getBytes()), now * 1000L)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq("key"), eq(ByteBuffer.wrap("value".getBytes())), eq(now * 1000L));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, my_attribute) VALUES (?, ?) USING TIMESTAMP ?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestWithTtl() throws URISyntaxException {
    long now = System.currentTimeMillis();
    factory.setup(
        entity,
        new URI("cassandra://wherever/my_table?data=my_col&primary=hgw&ttl=86400"),
        StringConverter.getDefault());
    StreamElement ingest =
        StreamElement.upsert(entity, attr, "", "key", "myAttribute", now, "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", ByteBuffer.wrap("value".getBytes()), now * 1000L)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);
    when(bound.setBytes(eq(1), any())).thenReturn(bound);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq("key"), eq(ByteBuffer.wrap("value".getBytes())), eq(now * 1000L));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, my_attribute) VALUES (?, ?) USING TIMESTAMP ?"
            + " AND TTL 86400",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestWildcard() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(entity, attrWildcard, "", "key", "device.1", now, "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1", ByteBuffer.wrap("value".getBytes()), now * 1000L))
        .thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement)
        .bind(
            eq("key"), eq("1"),
            eq(ByteBuffer.wrap("value".getBytes())), eq(now * 1000L));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, device, my_col) VALUES (?, ?, ?) USING TIMESTAMP ?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestDeleteSimple() {
    long now = System.currentTimeMillis();
    StreamElement ingest = StreamElement.delete(entity, attr, "", "key", "myAttribute", now);
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(now * 1000L, "key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq(now * 1000L), eq("key"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "DELETE my_attribute FROM my_table USING TIMESTAMP ? WHERE hgw=?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestDeleteWildcard() {
    long now = System.currentTimeMillis();
    StreamElement ingest = StreamElement.delete(entity, attrWildcard, "", "key", "device.1", now);
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(now * 1000L, "1", "key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq(now * 1000L), eq("1"), eq("key"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "DELETE my_col FROM my_table USING TIMESTAMP ? WHERE device=? AND hgw=?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestDeleteWildcardAll() {
    long now = System.currentTimeMillis();
    StreamElement ingest = StreamElement.deleteWildcard(entity, attrWildcard, "", "key", now);
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(now * 1000L, "key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq(now * 1000L), eq("key"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("DELETE FROM my_table USING TIMESTAMP ? WHERE hgw=?", preparedStatement.get(0));
  }

  @Test
  public void testGetAttribute() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement = factory.getReadStatement("key", attr.getName(), attr, session);
    verify(statement).bind(eq("key"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT my_attribute FROM my_table WHERE hgw=?", preparedStatement.get(0));
  }

  @Test
  public void testGetAttributeWildcard() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getReadStatement("key", "device.1", attrWildcard, session);

    verify(statement).bind(eq("key"), eq("1"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT my_col FROM my_table WHERE hgw=? AND device=?", preparedStatement.get(0));
  }

  @Test
  public void testGetAttributeWildcardWithNonCharacterSuffix() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1:2")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getReadStatement("key", "device.1:2", attrWildcard, session);

    verify(statement).bind(eq("key"), eq("1:2"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT my_col FROM my_table WHERE hgw=? AND device=?", preparedStatement.get(0));
  }

  @Test
  public void testListWildcardWithoutStart() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "", Integer.MAX_VALUE)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getListStatement("key", attrWildcard, null, -1, session);
    verify(statement).bind(eq("key"), eq(""), eq(Integer.MAX_VALUE));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "SELECT device, my_col FROM my_table WHERE hgw=? AND device>? LIMIT ?",
        preparedStatement.get(0));
  }

  @Test
  public void testListWildcardWithStart() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1", 10)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getListStatement("key", attrWildcard, new Offsets.Raw("device.1"), 10, session);
    verify(statement).bind(eq("key"), eq("1"), eq(10));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "SELECT device, my_col FROM my_table WHERE hgw=? AND device>? LIMIT ?",
        preparedStatement.get(0));
  }

  @Test
  public void testFetchOffset() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement = factory.getFetchTokenStatement("key", session);
    verify(statement).bind(eq("key"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT token(hgw) FROM my_table WHERE hgw=?", preparedStatement.get(0));
  }

  @Test
  public void testListWildcardWithExplicitSecondaryField() throws URISyntaxException {
    factory.setup(
        entity,
        new URI("cassandra://wherever/my_table?data=my_col" + "&primary=hgw&secondary=stamp"),
        StringConverter.getDefault());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "", Integer.MAX_VALUE)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getListStatement("key", attrWildcard, null, -1, session);
    verify(statement).bind(eq("key"), eq(""), eq(Integer.MAX_VALUE));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "SELECT stamp, my_col FROM my_table WHERE hgw=? AND stamp>? LIMIT ?",
        preparedStatement.get(0));
  }
}
