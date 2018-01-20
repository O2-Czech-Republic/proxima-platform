/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@code TransformingCQLFactory}.
 */
public class TransformingCQLFactoryTest {

  Repository repo = Repository.Builder.ofTest(ConfigFactory.defaultApplication()).build();
  AttributeDescriptorBase<?> attr;
  EntityDescriptor entity;

  final List<String> statements = new ArrayList<>();
  CQLFactory factory;

  public TransformingCQLFactoryTest() throws URISyntaxException {
    attr = AttributeDescriptor.newBuilder(repo)
        .setEntity("dummy")
        .setName("first")
        .setName("second")
        .setSchemeURI(new URI("bytes:///"))
        .build();
    entity = EntityDescriptor.newBuilder()
        .addAttribute(attr)
        .setName("dummy")
        .build();
  }

  @Before
  public void setup() throws URISyntaxException {
    statements.clear();
    factory = new TransformingCQLFactory<String>(
        i -> new String(i.getValue()),
        Arrays.asList("a", "b"),
        Arrays.asList(
            p -> "a_" + p.getFirst() + "_" + p.getSecond(),
            p -> "b_" + p.getFirst() + "_" + p.getSecond())) {

      @Override
      protected String createInsertStatement(StreamElement ingest) {
        String s = super.createInsertStatement(ingest);
        statements.add(s);
        return s;
      }
    };
    factory.setup(
        new URI("cassandra://wherever/my_table/"),
        StringConverter.DEFAULT);
  }

  /**
   * Test of getWriteStatement method, of class TransformingCQLFactory.
   */
  @Test
  public void testApply() {
    long now = System.currentTimeMillis();
    StreamElement ingest = StreamElement.update(
        entity, attr, "", "123", "first",
        now, "value".getBytes());
    Session session = mock(Session.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    BoundStatement bound = mock(BoundStatement.class);
    when(session.prepare((String) any())).thenReturn(statement);
    when(statement.bind(any(), any())).thenReturn(bound);

    factory.getWriteStatement(ingest, session);
    assertEquals(1, statements.size());
    assertEquals(
        "INSERT INTO my_table (a, b) VALUES (?, ?) USING TIMESTAMP ?",
        statements.get(0));
    verify(statement).bind("a_123_value", "b_123_value");
    verify(bound).setLong(2, now * 1000L);
  }

  /**
   * Test of getWriteStatement method, of class TransformingCQLFactory.
   */
  @Test
  public void testApplyWithTTL() throws URISyntaxException {
    long now = System.currentTimeMillis();
    StreamElement ingest = StreamElement.update(
        entity, attr, "", "123", "first",
        now, "value".getBytes());
    Session session = mock(Session.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    when(session.prepare((String) any())).thenReturn(statement);
    when(statement.bind(any(), any())).thenReturn(mock(BoundStatement.class));
    factory.setup(
        new URI("cassandra://wherever/my_table/?ttl=86400"),
        StringConverter.DEFAULT);

    factory.getWriteStatement(ingest, session);
    assertEquals(1, statements.size());
    assertEquals(
        "INSERT INTO my_table (a, b) VALUES (?, ?) USING TIMESTAMP ?"
            + " AND TTL 86400",
        statements.get(0));
    verify(statement).bind("a_123_value", "b_123_value");
  }


  /**
   * Test of getWriteStatement method, of class TransformingCQLFactory.
   */
  @Test
  public void testApplyWithDelete() {
    StreamElement ingest = StreamElement.update(
        entity, attr, "", "123", "first",
        System.currentTimeMillis(),
        null);
    Session session = mock(Session.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    when(session.prepare((String) any())).thenReturn(statement);
    when(statement.bind(any(), any())).thenReturn(mock(BoundStatement.class));

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    assertEquals(0, statements.size());
    assertFalse(boundStatement.isPresent());
  }


}
