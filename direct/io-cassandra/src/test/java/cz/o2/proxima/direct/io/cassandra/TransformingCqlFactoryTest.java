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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeDescriptorBase;
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@code TransformingCqlFactory}. */
public class TransformingCqlFactoryTest {

  final Repository repo =
      ConfigRepository.Builder.ofTest(ConfigFactory.defaultApplication()).build();
  AttributeDescriptorBase<?> attr;
  EntityDescriptor entity;

  final List<String> statements = new ArrayList<>();
  CqlFactory factory;

  public TransformingCqlFactoryTest() throws URISyntaxException {
    attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setName("first")
            .setName("second")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    entity = EntityDescriptor.newBuilder().addAttribute(attr).setName("dummy").build();
  }

  @Before
  public void setup() throws URISyntaxException {
    statements.clear();
    factory =
        new TransformingCqlFactory<String>(
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
    factory.setup(entity, new URI("cassandra://wherever/my_table/"), StringConverter.getDefault());
  }

  /** Test of getWriteStatement method, of class TransformingCqlFactory. */
  @Test
  public void testApply() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "123", "first", now, "value".getBytes());
    CqlSession session = mock(CqlSession.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    BoundStatement bound = mock(BoundStatement.class);
    when(session.prepare((String) any())).thenReturn(statement);
    when(bound.setLong(anyInt(), anyLong())).thenReturn(bound);
    when(statement.bind(any(), any())).thenReturn(bound);

    factory.getWriteStatement(ingest, session);
    assertEquals(1, statements.size());
    assertEquals("INSERT INTO my_table (a, b) VALUES (?, ?) USING TIMESTAMP ?", statements.get(0));
    verify(statement).bind("a_123_value", "b_123_value");
    verify(bound).setLong(2, now * 1000L);
  }

  /** Test of getWriteStatement method, of class TransformingCqlFactory. */
  @Test
  public void testApplyWithTtl() {
    final long now = System.currentTimeMillis();
    final StreamElement ingest =
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "123", "first", now, "value".getBytes());
    final CqlSession session = mock(CqlSession.class);
    final PreparedStatement statement = mock(PreparedStatement.class);

    when(session.prepare((String) any())).thenReturn(statement);
    BoundStatement mockStatement = mock(BoundStatement.class);
    when(statement.bind(any(), any())).thenReturn(mockStatement);
    when(mockStatement.setLong(anyInt(), anyLong())).thenReturn(mockStatement);
    factory.setup(
        entity,
        ExceptionUtils.uncheckedFactory(() -> new URI("cassandra://wherever/my_table/?ttl=86400")),
        StringConverter.getDefault());

    factory.getWriteStatement(ingest, session);
    assertEquals(1, statements.size());
    assertEquals(
        "INSERT INTO my_table (a, b) VALUES (?, ?) USING TIMESTAMP ?" + " AND TTL 86400",
        statements.get(0));
    verify(statement).bind("a_123_value", "b_123_value");
  }

  /** Test of getWriteStatement method, of class TransformingCqlFactory. */
  @Test
  public void testApplyWithDelete() {
    StreamElement ingest =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "123",
            "first",
            System.currentTimeMillis(),
            null);
    CqlSession session = mock(CqlSession.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    when(session.prepare((String) any())).thenReturn(statement);
    when(statement.bind(any(), any())).thenReturn(mock(BoundStatement.class));

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    assertEquals(0, statements.size());
    assertFalse(boundStatement.isPresent());
  }
}
