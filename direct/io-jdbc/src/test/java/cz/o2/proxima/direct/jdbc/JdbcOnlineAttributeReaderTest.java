/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.jdbc;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class JdbcOnlineAttributeReaderTest extends JdbcBaseTest {

  @Test
  public void listEntitiesTest() throws IOException {
    assertTrue(
        writeElement(
                accessor,
                StreamElement.upsert(
                    entity,
                    attr,
                    UUID.randomUUID().toString(),
                    "1",
                    attr.getName(),
                    System.currentTimeMillis(),
                    "value".getBytes()))
            .get());
    assertTrue(
        writeElement(
                accessor,
                StreamElement.upsert(
                    entity,
                    attr,
                    UUID.randomUUID().toString(),
                    "2",
                    attr.getName(),
                    System.currentTimeMillis(),
                    "value".getBytes()))
            .get());
    try (RandomAccessReader reader = accessor.newRandomAccessReader()) {
      List<String> keys = new ArrayList<>();
      reader.listEntities(x -> keys.add(x.getSecond()));
      assertEquals(Arrays.asList("1", "2"), keys);
    }
  }

  @Test
  public void writeAndReadSuccessfullyTest() {
    final long tms = System.currentTimeMillis();
    StreamElement element =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "12345",
            attr.getName(),
            tms,
            "value".getBytes());
    assertTrue(writeElement(accessor, element).get());

    Optional<KeyValue<Byte[]>> keyValue = accessor.newRandomAccessReader().get("12345", attr);
    assertTrue(keyValue.isPresent());
    log.debug("KV: {}", keyValue.get());
    assertEquals(attr, keyValue.get().getAttributeDescriptor());
    assertEquals(tms, keyValue.get().getStamp());
    assertEquals("value", new String(Objects.requireNonNull(keyValue.get().getValue())));
  }

  @Test
  public void getNotExistsTest() {
    Optional<KeyValue<Byte[]>> keyValue = accessor.newRandomAccessReader().get("12345", attr);
    assertFalse(keyValue.isPresent());
  }

  @Test
  public void getInvalidAttributeTest() throws URISyntaxException {
    AttributeDescriptor<byte[]> missing =
        AttributeDescriptor.newBuilder(repository)
            .setEntity(entity.getName())
            .setName("missing")
            .setSchemeUri(new URI("bytes:///"))
            .build();
    RandomAccessReader reader = accessor.newRandomAccessReader();
    try {
      reader.get("key", missing);
      fail("Should have thrown exception");
    } catch (IllegalStateException ex) {
      assertTrue(ex.getCause() instanceof SQLSyntaxErrorException);
    }
  }

  @Test
  public void testAsFactory() {
    RandomAccessReader reader = accessor.newRandomAccessReader();
    RandomAccessReader clonedReader = reader.asFactory().apply(repository);
    assertEquals(reader, clonedReader);
  }
}
