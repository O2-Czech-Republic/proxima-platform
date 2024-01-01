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
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class JdbcOnlineAttributeWriterTest extends JdbcBaseTest {

  @Test
  public void writeSuccessfullyTest() {
    StreamElement element =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "12345",
            attr.getName(),
            System.currentTimeMillis(),
            "value".getBytes());
    assertTrue(writeElement(accessor, element).get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void writeFailTest() {
    StreamElement element =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "",
            attr.getName(),
            System.currentTimeMillis(),
            "value".getBytes());
    writeElement(accessor, element);
  }

  @Test
  public void writeIllegalAttribute() throws URISyntaxException {
    AttributeDescriptor<byte[]> missing =
        AttributeDescriptor.newBuilder(repository)
            .setEntity(entity.getName())
            .setName("missing")
            .setSchemeUri(new URI("bytes:///"))
            .build();

    StreamElement element =
        StreamElement.upsert(
            entity,
            missing,
            UUID.randomUUID().toString(),
            "key",
            missing.getName(),
            System.currentTimeMillis(),
            "value".getBytes());
    assertFalse(writeElement(accessor, element).get());
  }

  @Test
  public void deleteTest() throws IOException {
    try (RandomAccessReader reader = accessor.newRandomAccessReader()) {
      StreamElement element =
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "12345",
              attr.getName(),
              System.currentTimeMillis(),
              "value".getBytes());
      assertTrue(writeElement(accessor, element).get());
      Optional<KeyValue<Byte[]>> keyValue = reader.get("12345", attr);
      assertTrue(keyValue.isPresent());
      StreamElement delete =
          StreamElement.delete(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "12345",
              attr.getName(),
              System.currentTimeMillis());
      assertTrue(writeElement(accessor, delete).get());

      keyValue = reader.get("12345", attr);
      assertFalse(keyValue.isPresent());
    }
  }

  @Test
  public void testAsFactory() {
    AttributeWriterBase writer = accessor.newWriter();
    AttributeWriterBase clonedWriter = writer.asFactory().apply(repository);
    assertEquals(writer, clonedWriter);
  }
}
